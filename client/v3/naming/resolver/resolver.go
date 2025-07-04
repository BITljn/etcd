// Copyright 2021 The etcd Authors
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

package resolver

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	gresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

type builder struct {
	c *clientv3.Client
}

func (b builder) Build(target gresolver.Target, cc gresolver.ClientConn, opts gresolver.BuildOptions) (gresolver.Resolver, error) {
	// Refer to https://github.com/grpc/grpc-go/blob/16d3df80f029f57cff5458f1d6da6aedbc23545d/clientconn.go#L1587-L1611
	endpoint := target.URL.Path
	if endpoint == "" {
		endpoint = target.URL.Opaque
	}
	endpoint = strings.TrimPrefix(endpoint, "/")
	r := &resolver{
		c:      b.c,
		target: endpoint,
		cc:     cc,
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	em, err := endpoints.NewManager(r.c, r.target)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolver: failed to new endpoint manager: %s", err)
	}
	r.wch, err = em.NewWatchChannel(r.ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolver: failed to new watch channer: %s", err)
	}

	r.wg.Add(1)
	go r.watch() // 启动一个goroutine 来监听 etcd 的变更，然后更新 grpc的endpoint
	return r, nil
}

func (b builder) Scheme() string {
	return "etcd"
}

// NewBuilder creates a resolver builder.
func NewBuilder(client *clientv3.Client) (gresolver.Builder, error) {
	return builder{c: client}, nil
}

type resolver struct {
	c      *clientv3.Client
	target string
	cc     gresolver.ClientConn // 注意这个对象
	wch    endpoints.WatchChannel
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// watch 变更然后更新 grpc的endpoint
func (r *resolver) watch() {
	defer r.wg.Done()

	allUps := make(map[string]*endpoints.Update)
	for {
		select {
		case <-r.ctx.Done():
			return
		case ups, ok := <-r.wch:
			if !ok {
				return
			}

			for _, up := range ups {
				switch up.Op {
				case endpoints.Add:
					allUps[up.Key] = up
				case endpoints.Delete:
					delete(allUps, up.Key)
				}
			}

			eps := convertToGRPCEndpoint(allUps)
			r.cc.UpdateState(gresolver.State{Endpoints: eps})
		}
	}
}

func convertToGRPCEndpoint(ups map[string]*endpoints.Update) []gresolver.Endpoint {
	var eps []gresolver.Endpoint
	for _, up := range ups {
		ep := gresolver.Endpoint{
			Addresses: []gresolver.Address{
				{
					Addr:     up.Endpoint.Addr,
					Metadata: up.Endpoint.Metadata,
				},
			},
		}
		eps = append(eps, ep)
	}
	return eps
}

// ResolveNow is a no-op here.
// It's just a hint, resolver can ignore this if it's not necessary.
func (r *resolver) ResolveNow(gresolver.ResolveNowOptions) {}

func (r *resolver) Close() {
	r.cancel()
	r.wg.Wait()
}
