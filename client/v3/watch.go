// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientv3

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

const (
	EventTypeDelete = mvccpb.DELETE
	EventTypePut    = mvccpb.PUT

	closeSendErrTimeout = 250 * time.Millisecond

	// AutoWatchID is the watcher ID passed in WatchStream.Watch when no
	// user-provided ID is available. If pass, an ID will automatically be assigned.
	AutoWatchID = 0

	// InvalidWatchID represents an invalid watch ID and prevents duplication with an existing watch.
	InvalidWatchID = -1
)

type Event mvccpb.Event

type WatchChan <-chan WatchResponse

type Watcher interface {
	// Watch watches on a key or prefix. The watched events will be returned
	// through the returned channel. If revisions waiting to be sent over the
	// watch are compacted, then the watch will be canceled by the server, the
	// client will post a compacted error watch response, and the channel will close.
	// If the requested revision is 0 or unspecified, the returned channel will
	// return watch events that happen after the server receives the watch request.
	// If the context "ctx" is canceled or timed out, returned "WatchChan" is closed,
	// and "WatchResponse" from this closed channel has zero events and nil "Err()".
	// The context "ctx" MUST be canceled, as soon as watcher is no longer being used,
	// to release the associated resources.
	//
	// If the context is "context.Background/TODO", returned "WatchChan" will
	// not be closed and block until event is triggered, except when server
	// returns a non-recoverable error (e.g. ErrCompacted).
	// For example, when context passed with "WithRequireLeader" and the
	// connected server has no leader (e.g. due to network partition),
	// error "etcdserver: no leader" (ErrNoLeader) will be returned,
	// and then "WatchChan" is closed with non-nil "Err()".
	// In order to prevent a watch stream being stuck in a partitioned node,
	// make sure to wrap context with "WithRequireLeader".
	//
	// Otherwise, as long as the context has not been canceled or timed out,
	// watch will retry on other recoverable errors forever until reconnected.
	//
	// TODO: explicitly set context error in the last "WatchResponse" message and close channel?
	// Currently, client contexts are overwritten with "valCtx" that never closes.
	// TODO(v3.4): configure watch retry policy, limit maximum retry number
	// (see https://github.com/etcd-io/etcd/issues/8980)
	Watch(ctx context.Context, key string, opts ...OpOption) WatchChan

	// RequestProgress requests a progress notify response be sent in all watch channels.
	RequestProgress(ctx context.Context) error

	// Close closes the watcher and cancels all watch requests.
	Close() error
}

// 这里定义了 RPC接口的返回方式，grpc 本身支持流式连接，可以屏蔽很多连接细节和 retry逻辑
type WatchResponse struct {
	Header pb.ResponseHeader
	Events []*Event

	// CompactRevision is the minimum revision the watcher may receive.
	CompactRevision int64

	// Canceled is used to indicate watch failure.
	// If the watch failed and the stream was about to close, before the channel is closed,
	// the channel sends a final response that has Canceled set to true with a non-nil Err().
	Canceled bool

	// Created is used to indicate the creation of the watcher.
	Created bool

	closeErr error

	// cancelReason is a reason of canceling watch
	cancelReason string
}

// IsCreate returns true if the event tells that the key is newly created.
func (e *Event) IsCreate() bool {
	return e.Type == EventTypePut && e.Kv.CreateRevision == e.Kv.ModRevision
}

// IsModify returns true if the event tells that a new value is put on existing key.
func (e *Event) IsModify() bool {
	return e.Type == EventTypePut && e.Kv.CreateRevision != e.Kv.ModRevision
}

// Err is the error value if this WatchResponse holds an error.
func (wr *WatchResponse) Err() error {
	switch {
	case wr.closeErr != nil:
		return v3rpc.Error(wr.closeErr)
	case wr.CompactRevision != 0:
		return v3rpc.ErrCompacted
	case wr.Canceled: // server 取消就是这里出错
		if len(wr.cancelReason) != 0 {
			return v3rpc.Error(status.Error(codes.FailedPrecondition, wr.cancelReason))
		}
		return v3rpc.ErrFutureRev
	}
	return nil
}

// IsProgressNotify returns true if the WatchResponse is progress notification.
func (wr *WatchResponse) IsProgressNotify() bool {
	return len(wr.Events) == 0 && !wr.Canceled && !wr.Created && wr.CompactRevision == 0 && wr.Header.Revision != 0
}

// watcher implements the Watcher interface
type watcher struct {
	remote   pb.WatchClient
	callOpts []grpc.CallOption

	// mu protects the grpc streams map
	mu sync.Mutex

	// streams holds all the active grpc streams keyed by ctx value.
	streams map[string]*watchGRPCStream
	lg      *zap.Logger
}

// watchGRPCStream tracks all watch resources attached to a single grpc stream.
type watchGRPCStream struct {
	owner    *watcher
	remote   pb.WatchClient // 这个 client共享给了多个 watchGRPCStream，因此他们最终的 rpc调用就共用了一个 tcp连接了。
	callOpts []grpc.CallOption

	// ctx controls internal remote.Watch requests
	ctx context.Context
	// ctxKey is the key used when looking up this stream's context
	ctxKey string
	cancel context.CancelFunc

	// substreams holds all active watchers on this grpc stream
	substreams map[int64]*watcherStream
	// resuming holds all resuming watchers on this grpc stream
	resuming []*watcherStream

	// reqc sends a watch request from Watch() to the main goroutine
	reqc chan watchStreamRequest // 主线程发送watch请求用的

	// respc receives data from the watch client
	respc chan *pb.WatchResponse // 从 rpc server 收到报文

	// donec closes to broadcast shutdown
	donec chan struct{}
	// errc transmits errors from grpc Recv to the watch stream reconnect logic
	errc chan error
	// closingc gets the watcherStream of closing watchers
	closingc chan *watcherStream
	// wg is Done when all substream goroutines have exited
	wg sync.WaitGroup

	// resumec closes to signal that all substreams should begin resuming
	resumec chan struct{}
	// closeErr is the error that closed the watch stream
	closeErr error

	lg *zap.Logger
}

// watchStreamRequest is a union of the supported watch request operation types
type watchStreamRequest interface {
	toPB() *pb.WatchRequest
}

// watchRequest is issued by the subscriber to start a new watcher
type watchRequest struct {
	ctx context.Context
	key string
	end string
	rev int64

	// send created notification event if this field is true
	createdNotify bool
	// progressNotify is for progress updates
	progressNotify bool
	// fragmentation should be disabled by default
	// if true, split watch events when total exceeds
	// "--max-request-bytes" flag value + 512-byte
	fragment bool

	// filters is the list of events to filter out
	filters []pb.WatchCreateRequest_FilterType
	// get the previous key-value pair before the event happens
	prevKV bool
	// retc receives a chan WatchResponse once the watcher is established
	retc chan chan WatchResponse
}

// progressRequest is issued by the subscriber to request watch progress
type progressRequest struct{}

// watcherStream represents a registered watcher
type watcherStream struct {
	// initReq is the request that initiated this request
	initReq watchRequest

	// outc publishes watch responses to subscriber
	outc chan WatchResponse
	// recvc buffers watch responses before publishing
	// dispatchEvent() 会给这里发送数据，通过 unicast 或者broadcast
	recvc chan *WatchResponse
	// donec closes when the watcherStream goroutine stops.
	donec chan struct{}
	// closing is set to true when stream should be scheduled to shutdown.
	closing bool
	// id is the registered watch id on the grpc stream
	id int64

	// buf holds all events received from etcd but not yet consumed by the client
	// 这里就是缓存吧
	buf []*WatchResponse
}

// 一个 etcd client 会初始化一个 watcher，同一个 watcher的多个 Watch 监听 prefix过程复用了同一个 grpc stream
func NewWatcher(c *Client) Watcher {
	return NewWatchFromWatchClient(pb.NewWatchClient(c.conn), c)
}

func NewWatchFromWatchClient(wc pb.WatchClient, c *Client) Watcher {
	w := &watcher{
		remote:  wc,                                // 这个 wc 里面保存外部传过的唯一 grpc 连接
		streams: make(map[string]*watchGRPCStream), // 这里的 streams 是一个 map，key 是 ctx 的值，不是具体我们需要的 domain区别下，就是说一个 domain，可以通过watch多次，因为版本好可以不同，这是区别。
	}
	if c != nil {
		w.callOpts = c.callOpts
		w.lg = c.lg
	}
	return w
}

// never closes
var (
	valCtxCh = make(chan struct{})
	zeroTime = time.Unix(0, 0)
)

// ctx with only the values; never Done
type valCtx struct{ context.Context }

func (vc *valCtx) Deadline() (time.Time, bool) { return zeroTime, false }
func (vc *valCtx) Done() <-chan struct{}       { return valCtxCh }
func (vc *valCtx) Err() error                  { return nil }

// Watch 方法会根据ctx, 初始化一个wgs 结构体
func (w *watcher) newWatcherGRPCStream(inctx context.Context) *watchGRPCStream {
	ctx, cancel := context.WithCancel(&valCtx{inctx})
	wgs := &watchGRPCStream{
		owner:      w,
		remote:     w.remote,
		callOpts:   w.callOpts,
		ctx:        ctx,
		ctxKey:     streamKeyFromCtx(inctx),
		cancel:     cancel, //wgs关闭会调用这个 cancal()
		substreams: make(map[int64]*watcherStream),
		respc:      make(chan *pb.WatchResponse),
		reqc:       make(chan watchStreamRequest),
		donec:      make(chan struct{}),
		errc:       make(chan error, 1),
		closingc:   make(chan *watcherStream),
		resumec:    make(chan struct{}),
		lg:         w.lg,
	}
	// 每个 wgs 对应一个逻辑管理线程，一个ctx对应一个
	go wgs.run()
	return wgs
}

// Watch posts a watch request to run() and waits for a new watcher channel
func (w *watcher) Watch(ctx context.Context, key string, opts ...OpOption) WatchChan {
	ow := opWatch(key, opts...)

	var filters []pb.WatchCreateRequest_FilterType
	if ow.filterPut {
		filters = append(filters, pb.WatchCreateRequest_NOPUT)
	}
	if ow.filterDelete {
		filters = append(filters, pb.WatchCreateRequest_NODELETE)
	}

	wr := &watchRequest{
		ctx:            ctx,
		createdNotify:  ow.createdNotify,
		key:            string(ow.key),
		end:            string(ow.end),
		rev:            ow.rev,
		progressNotify: ow.progressNotify,
		fragment:       ow.fragment,
		filters:        filters,
		prevKV:         ow.prevKV,
		retc:           make(chan chan WatchResponse, 1), // 需要看下这个东西哪里返回，一个接受 chan 的 chan
	}

	ok := false
	ctxKey := streamKeyFromCtx(ctx)

	// closeCh is a channel for receiving watch responses when the watcher is closed.
	// When nil, indicates the watcher is still active.
	var closeCh chan WatchResponse
	for {
		// find or allocate appropriate grpc watch stream
		w.mu.Lock()
		if w.streams == nil {
			// closed
			w.mu.Unlock()
			ch := make(chan WatchResponse)
			close(ch)
			return ch
		}
		wgs := w.streams[ctxKey]
		if wgs == nil {
			wgs = w.newWatcherGRPCStream(ctx) // wgs.run() 启动了收包流程
			w.streams[ctxKey] = wgs
		}

		donec := wgs.donec
		reqc := wgs.reqc // 这是一个请求，在 grpc 中逻辑上的子流
		w.mu.Unlock()

		// couldn't create channel; return closed channel
		if closeCh == nil {
			closeCh = make(chan WatchResponse, 1)
		}

		// submit request
		select {
		case reqc <- wr: // 要发送一个watch请求，对应具体的某个 ctx下的 substream
			ok = true
		case <-wr.ctx.Done(): //父context手动取消，或者超时了。
			ok = false
		case <-donec:
			ok = false
			if wgs.closeErr != nil {
				closeCh <- WatchResponse{Canceled: true, closeErr: wgs.closeErr}
				break
			}
			// retry; may have dropped stream from no ctxs
			continue
		}

		// receive channel
		if ok {
			select {
			case ret := <-wr.retc: // 这地方是 watchRequest,里面返回一个chan用来给调用着收数据，也即为 Watch()函数返回值
				return ret
			case <-ctx.Done():
			case <-donec:
				// 连接断开或者关闭了，如果有错误，错误信息返回到 closeCh
				if wgs.closeErr != nil {
					closeCh <- WatchResponse{Canceled: true, closeErr: wgs.closeErr}
					break
				}
				// retry; may have dropped stream from no ctxs
				continue
			}
		}
		break
	}

	close(closeCh)
	return closeCh
}

// 这个 Watcher是共享的一个 remote connection
func (w *watcher) Close() (err error) {
	w.mu.Lock()
	streams := w.streams
	w.streams = nil
	w.mu.Unlock()
	for _, wgs := range streams {
		if werr := wgs.close(); werr != nil {
			err = werr
		}
	}
	// Consider context.Canceled as a successful close
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

// RequestProgress requests a progress notify response be sent in all watch channels.
func (w *watcher) RequestProgress(ctx context.Context) (err error) {
	ctxKey := streamKeyFromCtx(ctx)

	w.mu.Lock()
	if w.streams == nil {
		w.mu.Unlock()
		return errors.New("no stream found for context")
	}
	wgs := w.streams[ctxKey]
	if wgs == nil {
		wgs = w.newWatcherGRPCStream(ctx)
		w.streams[ctxKey] = wgs
	}
	donec := wgs.donec
	reqc := wgs.reqc
	w.mu.Unlock()

	pr := &progressRequest{}

	select {
	case reqc <- pr:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-donec:
		if wgs.closeErr != nil {
			return wgs.closeErr
		}
		// retry; may have dropped stream from no ctxs
		return w.RequestProgress(ctx)
	}
}

func (w *watchGRPCStream) close() (err error) {
	w.cancel()
	<-w.donec
	select {
	case err = <-w.errc:
	default:
	}
	return ContextError(w.ctx, err)
}

func (w *watcher) closeStream(wgs *watchGRPCStream) {
	w.mu.Lock()
	close(wgs.donec)
	wgs.cancel()
	if w.streams != nil {
		delete(w.streams, wgs.ctxKey)
	}
	w.mu.Unlock()
}

func (w *watchGRPCStream) addSubstream(resp *pb.WatchResponse, ws *watcherStream) {
	// check watch ID for backward compatibility (<= v3.3)
	if resp.WatchId == InvalidWatchID || (resp.Canceled && resp.CancelReason != "") {
		w.closeErr = v3rpc.Error(errors.New(resp.CancelReason))
		// failed; no channel
		close(ws.recvc)
		return
	}
	ws.id = resp.WatchId
	w.substreams[ws.id] = ws
}

func (w *watchGRPCStream) sendCloseSubstream(ws *watcherStream, resp *WatchResponse) {
	select {
	case ws.outc <- *resp:
	case <-ws.initReq.ctx.Done():
	case <-time.After(closeSendErrTimeout):
	}
	close(ws.outc)
}

// 给上层发挥一个错误的 resp
// 释放结构体资源，变更 map
func (w *watchGRPCStream) closeSubstream(ws *watcherStream) {
	// send channel response in case stream was never established
	select {
	case ws.initReq.retc <- ws.outc:
	default:
	}
	// close subscriber's channel， 这是给外部的调用发送一个关闭的 resp
	if closeErr := w.closeErr; closeErr != nil && ws.initReq.ctx.Err() == nil {
		go w.sendCloseSubstream(ws, &WatchResponse{Canceled: true, closeErr: w.closeErr})
	} else if ws.outc != nil {
		close(ws.outc)
	}
	if ws.id != InvalidWatchID {
		delete(w.substreams, ws.id)
		return
	}
	for i := range w.resuming {
		if w.resuming[i] == ws {
			w.resuming[i] = nil
			return
		}
	}
}

// run is the root of the goroutines for managing a watcher client
func (w *watchGRPCStream) run() {
	var wc pb.Watch_WatchClient
	var closeErr error

	// substreams marked to close but goroutine still running; needed for
	// avoiding double-closing recvc on grpc stream teardown
	closing := make(map[*watcherStream]struct{})

	// 这里如果异常退出就走清理资源逻辑，会关闭返回给用户的 chan
	defer func() {
		w.closeErr = closeErr
		// shutdown substreams and resuming substreams
		for _, ws := range w.substreams {
			if _, ok := closing[ws]; !ok { // 这里修改状态，不是在 closing, 去 close(ws.recvc)关闭 serveSubstream线程
				close(ws.recvc)          // 先关闭内部接受，wgs 不能继续发数据到这里
				closing[ws] = struct{}{} // 标记关闭
			}
		}
		for _, ws := range w.resuming {
			if _, ok := closing[ws]; ws != nil && !ok {
				close(ws.recvc)
				closing[ws] = struct{}{}
			}
		}
		w.joinSubstreams() // 等待substream 退出，done
		for range closing {
			w.closeSubstream(<-w.closingc) // 给用户发送一个错误的 resp，清理最后的数组等逻辑数据
		}
		w.wg.Wait()
		w.owner.closeStream(w) // 在 watcher释放
	}()

	// start a stream with the etcd grpc server
	// 核心调用：
	// 1. joinSubstreams，通过close(w.resumec)
	// 2. openWatchClient
	// 3. serveWatchClient
	// 4. serveSubstream
	// 一个 wc个一个wgs一一对应 etcdclient在这上边维护多个逻辑 substream
	if wc, closeErr = w.newWatchClient(); closeErr != nil {
		return
	}

	cancelSet := make(map[int64]struct{})

	var cur *pb.WatchResponse
	backoff := time.Millisecond
	for {
		select {
		// Watch() requested
		case req := <-w.reqc:
			switch wreq := req.(type) {
			case *watchRequest:
				outc := make(chan WatchResponse, 1)
				// TODO: pass custom watch ID?
				ws := &watcherStream{
					initReq: *wreq,
					id:      InvalidWatchID,
					outc:    outc,
					// unbuffered so resumes won't cause repeat events
					recvc: make(chan *WatchResponse),
				}

				ws.donec = make(chan struct{})
				w.wg.Add(1)
				// 服务订阅对象的每个 ws 有一个单独的 goroutine（这个就对应一个 key监听这么理解）
				go w.serveSubstream(ws, w.resumec)

				// queue up for watcher creation/resume
				w.resuming = append(w.resuming, ws)
				// 这里是这个ctx下唯一的一个 watcherStream，也是第一个 watch req
				// 正常情况就一个 resuming，这是初次请求发生, 如果是多个 resuming,那么会在收到第一个的创建结果后
				// 在去发送创建请求
				if len(w.resuming) == 1 {
					// head of resume queue, can register a new watcher
					if err := wc.Send(ws.initReq.toPB()); err != nil {
						w.lg.Debug("error when sending request", zap.Error(err))
					}
				}
			case *progressRequest:
				if err := wc.Send(wreq.toPB()); err != nil {
					w.lg.Debug("error when sending request", zap.Error(err))
				}
			}

		// new events from the watch client
		// 所有 substream 的包都是这里接受的。所以这地方定义了发送和接收双方的协议
		case pbresp := <-w.respc:
			if cur == nil || pbresp.Created || pbresp.Canceled {
				cur = pbresp
			} else if cur != nil && cur.WatchId == pbresp.WatchId {
				// merge new events
				cur.Events = append(cur.Events, pbresp.Events...)
				// update "Fragment" field; last response with "Fragment" == false
				cur.Fragment = pbresp.Fragment
			}

			switch {
			case pbresp.Created:
				// response to head of queue creation
				// resuming 是严格按照顺序的，是个队列先进先出
				if len(w.resuming) != 0 {
					if ws := w.resuming[0]; ws != nil {
						w.addSubstream(pbresp, ws) // 添加到substream 数组中，会有一个唯一的 watchId
						w.dispatchEvent(pbresp)    // 这是第一个 watch response直接也返回给用户了。
						w.resuming[0] = nil
					}
				}
				// resuming 数组的第一个，这个地方发送初始化 watch 请求
				// 这里也是恢复后续其他 watch stream 逻辑，异常的情况那里只发送了一个 req, 这个 ws恢复了，在恢复其他的
				if ws := w.nextResume(); ws != nil {
					if err := wc.Send(ws.initReq.toPB()); err != nil {
						w.lg.Debug("error when sending request", zap.Error(err))
					}
				}

				// reset for next iteration
				cur = nil

			//  server端通知取消的逻辑, 取消对应的 substream， serveSubStream 线程会退出去
			case pbresp.Canceled && pbresp.CompactRevision == 0:
				delete(cancelSet, pbresp.WatchId)
				if ws, ok := w.substreams[pbresp.WatchId]; ok {
					// signal to stream goroutine to update closingc， 注意这里！！！
					close(ws.recvc)
					closing[ws] = struct{}{} // 先给标记为关闭中
				}

				// reset for next iteration
				cur = nil

			// 分片逻辑
			case cur.Fragment:
				// watch response events are still fragmented
				// continue to fetch next fragmented event arrival
				continue

			default:
				// dispatch to appropriate watch stream
				// 这里是收到 etcd server 的 watch response 了
				ok := w.dispatchEvent(cur)

				// reset for next iteration
				cur = nil

				// ok 说明找到了 substream watchid
				if ok {
					break
				}
				// ok=false 说明收到的包不对劲，需要取消

				// watch response on unexpected watch id; cancel id
				if _, ok := cancelSet[pbresp.WatchId]; ok {
					break
				}

				cancelSet[pbresp.WatchId] = struct{}{}
				cr := &pb.WatchRequest_CancelRequest{
					CancelRequest: &pb.WatchCancelRequest{
						WatchId: pbresp.WatchId,
					},
				}
				req := &pb.WatchRequest{RequestUnion: cr}
				w.lg.Debug("sending watch cancel request for failed dispatch", zap.Int64("watch-id", pbresp.WatchId))
				if err := wc.Send(req); err != nil {
					w.lg.Debug("failed to send watch cancel request", zap.Int64("watch-id", pbresp.WatchId), zap.Error(err))
				}
			}

		// watch client failed on Recv; spawn another if possible
		// 某一个 wc.recv (w.remote.Recv() 返回的结构为 wc， 不同的substream 不一样对象) 出错了，处理重连的场景，并且有退让时间
		case err := <-w.errc:
			// 这些场景直接关闭了
			if isHaltErr(w.ctx, err) || errors.Is(ContextError(w.ctx, err), v3rpc.ErrNoLeader) {
				closeErr = err
				return
			}
			// 这里是 grpc stream 断开了，重新连接
			backoff = w.backoffIfUnavailable(backoff, err)
			// 这个 newWatchClient 全文一共就两个地方使用
			// 1. 这里出错重连，这里重连是要连接所有的 substream，因此 newWatchClient 会需要去join这些连接
			// 2. run() 的开头
			// 这地方相当于从新调用了一次 rpc接口，也就是重连了。这么理解吧, 这个 wc是新的
			if wc, closeErr = w.newWatchClient(); closeErr != nil {
				return // 直接 return会走到 defer中，彻底关闭 wgs
			}
			// 注意这里是按照顺序一个一个 resume,避免 server压力过大
			if ws := w.nextResume(); ws != nil {
				if err := wc.Send(ws.initReq.toPB()); err != nil {
					w.lg.Debug("error when sending request", zap.Error(err))
				}
			}
			cancelSet = make(map[int64]struct{})

		// cancel场景，例如我们取消订阅
		case <-w.ctx.Done():
			return

		// 关闭其中一个 substream 场景
		case ws := <-w.closingc: // 收到的是一个 WatcherStream结构体, 收到之前会先标记到 closing数组中
			w.closeSubstream(ws)
			delete(closing, ws) // 去除标记，标识删除完了，那么后面会给 server发送取消，可能不一定成功，但是其他地方有容错
			// no more watchers on this stream, shutdown, skip cancellation
			if len(w.substreams)+len(w.resuming) == 0 {
				return
			}
			// 给 server发送取消信号
			if ws.id != InvalidWatchID {
				// client is closing an established watch; close it on the server proactively instead of waiting
				// to close when the next message arrives
				cancelSet[ws.id] = struct{}{}
				cr := &pb.WatchRequest_CancelRequest{
					CancelRequest: &pb.WatchCancelRequest{
						WatchId: ws.id, // 这里就是关闭其中的一个 substream（不影响 wgs）, 连接还在
					},
				}
				req := &pb.WatchRequest{RequestUnion: cr}
				w.lg.Debug("sending watch cancel request for closed watcher", zap.Int64("watch-id", ws.id))
				if err := wc.Send(req); err != nil {
					w.lg.Debug("failed to send watch cancel request", zap.Int64("watch-id", ws.id), zap.Error(err))
				}
			}
		}
	}
}

// nextResume chooses the next resuming to register with the grpc stream. Abandoned
// streams are marked as nil in the queue since the head must wait for its inflight registration.
func (w *watchGRPCStream) nextResume() *watcherStream {
	for len(w.resuming) != 0 {
		if w.resuming[0] != nil {
			return w.resuming[0]
		}
		w.resuming = w.resuming[1:len(w.resuming)]
	}
	return nil
}

// dispatchEvent sends a WatchResponse to the appropriate watcher stream
func (w *watchGRPCStream) dispatchEvent(pbresp *pb.WatchResponse) bool {
	events := make([]*Event, len(pbresp.Events))
	for i, ev := range pbresp.Events {
		events[i] = (*Event)(ev)
	}
	// TODO: return watch ID?
	wr := &WatchResponse{
		Header:          *pbresp.Header,
		Events:          events,
		CompactRevision: pbresp.CompactRevision,
		Created:         pbresp.Created,
		Canceled:        pbresp.Canceled,
		cancelReason:    pbresp.CancelReason,
	}

	// watch IDs are zero indexed, so request notify watch responses are assigned a watch ID of InvalidWatchID to
	// indicate they should be broadcast.
	if wr.IsProgressNotify() && pbresp.WatchId == InvalidWatchID {
		return w.broadcastResponse(wr)
	}

	return w.unicastResponse(wr, pbresp.WatchId)
}

// broadcastResponse send a watch response to all watch substreams.
func (w *watchGRPCStream) broadcastResponse(wr *WatchResponse) bool {
	for _, ws := range w.substreams {
		select {
		case ws.recvc <- wr:
		case <-ws.donec:
		}
	}
	return true
}

// unicastResponse sends a watch response to a specific watch substream.
func (w *watchGRPCStream) unicastResponse(wr *WatchResponse, watchID int64) bool {
	ws, ok := w.substreams[watchID]
	if !ok {
		return false
	}
	select {
	case ws.recvc <- wr:
	case <-ws.donec:
		return false
	}
	return true
}

// serveWatchClient forwards messages from the grpc stream to run()
func (w *watchGRPCStream) serveWatchClient(wc pb.Watch_WatchClient) {
	for {
		resp, err := wc.Recv() // 每个 wc 是都一个独立的 grpc stream
		if err != nil {
			select {
			case w.errc <- err: // 这里是异常，wgs.run 会处理异常并重连
			case <-w.donec:
			}
			return
		}
		// 正常读取数据从 stream 中
		select {
		case w.respc <- resp: // 这里是从 grpc stream 读取数据，然后放到 w.respc 中, 这个 resp channel 是包含所有的 substream的包
		case <-w.donec:
			return
		}
	}
}

// serveSubstream forwards watch responses from run() to the subscriber
func (w *watchGRPCStream) serveSubstream(ws *watcherStream, resumec chan struct{}) {
	if ws.closing {
		panic("created substream goroutine but substream is closing")
	}

	// nextRev is the minimum expected next revision
	nextRev := ws.initReq.rev
	resuming := false
	defer func() {
		if !resuming {
			ws.closing = true
		}
		close(ws.donec)
		if !resuming {
			w.closingc <- ws // 这个会传递给 wgs.run 处理, 主要是给 server 发送取消
		}
		w.wg.Done()
	}()

	emptyWr := &WatchResponse{}
	for {
		curWr := emptyWr
		outc := ws.outc

		// curWr 指向 buf的第一个元素，如果不为空，那么 outc 就是 ws(watcherStream) 的 outc
		// 这样子在第一个 case中就被发送出去了，然后 buf[0] 就被置为 nil
		if len(ws.buf) > 0 {
			curWr = ws.buf[0]
		} else {
			outc = nil
		}

		select {
		case outc <- *curWr: // 这里 outc=nil 的时候，不会被写入到 channel
			if ws.buf[0].Err() != nil {
				return
			}
			ws.buf[0] = nil
			ws.buf = ws.buf[1:]
		case wr, ok := <-ws.recvc: // 这个 channel 关闭就是 ok = false
			// 从 server 端读到的 watch response，并缓存到buf中去
			if !ok {
				// shutdown from closeSubstream
				return
			}

			if wr.Created {
				if ws.initReq.retc != nil {
					// 返回是 chan 嵌套，这里保证给 app返回唯一的 chan
					ws.initReq.retc <- ws.outc
					// to prevent next write from taking the slot in buffered channel
					// and posting duplicate create events
					ws.initReq.retc = nil

					// send first creation event only if requested
					if ws.initReq.createdNotify {
						ws.outc <- *wr
					}
					// once the watch channel is returned, a current revision
					// watch must resume at the store revision. This is necessary
					// for the following case to work as expected:
					//	wch := m1.Watch("a")
					//	m2.Put("a", "b")
					//	<-wch
					// If the revision is only bound on the first observed event,
					// if wch is disconnected before the Put is issued, then reconnects
					// after it is committed, it'll miss the Put.
					if ws.initReq.rev == 0 {
						nextRev = wr.Header.Revision
					}
				}
			} else {
				// current progress of watch; <= store revision
				nextRev = wr.Header.Revision + 1
			}

			if len(wr.Events) > 0 {
				nextRev = wr.Events[len(wr.Events)-1].Kv.ModRevision + 1
			}

			ws.initReq.rev = nextRev

			// created event is already sent above,
			// watcher should not post duplicate events
			if wr.Created {
				continue
			}

			// TODO pause channel if buffer gets too large
			ws.buf = append(ws.buf, wr)
		case <-w.ctx.Done():
			return
		case <-ws.initReq.ctx.Done():
			return
		case <-resumec: // 收到 resume 信号，例如新的 grpc stream 建立成功，外部会 close resumec
			resuming = true
			return
		}
	}
	// lazily send cancel message if events on missing id
}

func (w *watchGRPCStream) newWatchClient() (pb.Watch_WatchClient, error) {
	// mark all substreams as resuming
	// 这地方是主动让所有 substream 关闭，然后 join
	close(w.resumec)
	w.resumec = make(chan struct{})
	w.joinSubstreams()

	// join后把所有的 stream全部假如到 resuming数组中用于恢复。
	for _, ws := range w.substreams {
		ws.id = InvalidWatchID
		w.resuming = append(w.resuming, ws)
	}
	// strip out nils, if any
	var resuming []*watcherStream
	for _, ws := range w.resuming {
		if ws != nil {
			resuming = append(resuming, ws)
		}
	}
	w.resuming = resuming
	w.substreams = make(map[int64]*watcherStream)

	// connect to grpc stream while accepting watcher cancelation
	stopc := make(chan struct{})
	// 处理取消逻辑
	donec := w.waitCancelSubstreams(stopc)
	wc, err := w.openWatchClient() // pb.Watch_WatchClient 返回，grpc定义的结构
	close(stopc)
	<-donec

	// serve all non-closing streams, even if there's a client error
	// so that the teardown path can shutdown the streams as expected.
	// 前边已经把stream 加入到了 resuming 中，关闭的除外，关闭的在 waitCancelSubstreams 中会设置 closing=true
	for _, ws := range w.resuming {
		if ws.closing {
			continue
		}
		ws.donec = make(chan struct{})
		w.wg.Add(1)
		// 处理从 server 端读取数据，然后缓存，再从缓存中把数据吐给 ws.outc
		// 这个 resumec 是一个 channel，外部会 close 这个 channel, 让所有的 channel 重连接
		go w.serveSubstream(ws, w.resumec)
	}

	if err != nil {
		return nil, v3rpc.Error(err)
	}

	// receive data from new grpc stream
	// 一个 ctx对应一个全局的，读取数据后，写入到了 wgs.respc中
	go w.serveWatchClient(wc)
	return wc, nil
}

// 就只有连接重连过程会被触发，wgs中的一个 substream异常了就会触发整个 wgs所有的stream重连。
// 这里就是处理关闭 watcherStream 的逻辑 或者 取消 ctx等场景，如果没有那么外部会 close(stopc)
func (w *watchGRPCStream) waitCancelSubstreams(stopc <-chan struct{}) <-chan struct{} {
	var wg sync.WaitGroup
	wg.Add(len(w.resuming))
	donec := make(chan struct{})
	for i := range w.resuming {
		go func(ws *watcherStream) {
			defer wg.Done()
			if ws.closing { // watherStream 退出不是由于resume关闭，那么会进入这里， server触发关闭
				// ctx.Err watch ctx 取消了，或者 watch返回了报错
				if ws.initReq.ctx.Err() != nil && ws.outc != nil {
					close(ws.outc)
					ws.outc = nil
				}
				return
			}
			select {
			case <-ws.initReq.ctx.Done(): // 用户调用了 cancel 或者 watcher.Close 操作
				// closed ws will be removed from resuming
				ws.closing = true
				close(ws.outc)
				ws.outc = nil
				w.wg.Add(1)
				go func() {
					defer w.wg.Done()
					w.closingc <- ws // wgs 会一个个接受处理关闭 substream 线程
				}()
			case <-stopc:
			}
		}(w.resuming[i])
	}
	go func() {
		defer close(donec)
		// 外部的 stopc 会触发每个 ws 的线程结束，每个线程结束后 wg.Done 会触发 wait 唤醒
		// wait 新来后 close(Done) 通知外部等待逻辑
		// 所以这里相当于外部有一个 stop 手动关闭，然后外部还有一个在等待 donec
		wg.Wait()
	}()

	// 这个 donec 是异步的，外边会等待
	return donec
}

// joinSubstreams waits for all substream goroutines to complete.
func (w *watchGRPCStream) joinSubstreams() {
	for _, ws := range w.substreams {
		<-ws.donec
	}
	for _, ws := range w.resuming {
		if ws != nil {
			<-ws.donec
		}
	}
}

var maxBackoff = 100 * time.Millisecond

func (w *watchGRPCStream) backoffIfUnavailable(backoff time.Duration, err error) time.Duration {
	if isUnavailableErr(w.ctx, err) {
		// retry, but backoff
		if backoff < maxBackoff {
			// 25% backoff factor
			backoff = backoff + backoff/4
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		time.Sleep(backoff)
	}
	return backoff
}

// openWatchClient retries opening a watch client until success or halt.
// manually retry in case "ws==nil && err==nil"
// TODO: remove FailFast=false
func (w *watchGRPCStream) openWatchClient() (ws pb.Watch_WatchClient, err error) {
	backoff := time.Millisecond
	for {
		select {
		case <-w.ctx.Done():
			if err == nil {
				return nil, w.ctx.Err()
			}
			return nil, err
		default:
		}
		// 共用了 remote，因此复用了 tcp连接
		// A[Single TCP Connection]
		// 		B[gRPC Connection]
		// 			B --> C[Watch Stream 1]
		// 			B --> D[Watch Stream 2]
		// 			B --> E[Watch Stream N]
		if ws, err = w.remote.Watch(w.ctx, w.callOpts...); ws != nil && err == nil {
			break
		}
		// 判断是否是不可恢复的错误
		if isHaltErr(w.ctx, err) {
			return nil, v3rpc.Error(err)
		}
		backoff = w.backoffIfUnavailable(backoff, err)
	}
	return ws, nil
}

// toPB converts an internal watch request structure to its protobuf WatchRequest structure.
func (wr *watchRequest) toPB() *pb.WatchRequest {
	req := &pb.WatchCreateRequest{
		StartRevision:  wr.rev,
		Key:            []byte(wr.key),
		RangeEnd:       []byte(wr.end),
		ProgressNotify: wr.progressNotify,
		Filters:        wr.filters,
		PrevKv:         wr.prevKV,
		Fragment:       wr.fragment,
	}
	cr := &pb.WatchRequest_CreateRequest{CreateRequest: req}
	return &pb.WatchRequest{RequestUnion: cr}
}

// toPB converts an internal progress request structure to its protobuf WatchRequest structure.
func (pr *progressRequest) toPB() *pb.WatchRequest {
	req := &pb.WatchProgressRequest{}
	cr := &pb.WatchRequest_ProgressRequest{ProgressRequest: req}
	return &pb.WatchRequest{RequestUnion: cr}
}

func streamKeyFromCtx(ctx context.Context) string {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		return fmt.Sprintf("%+v", map[string][]string(md))
	}
	return ""
}
