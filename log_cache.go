package logcache

import (
	"hash/crc64"
	"io/ioutil"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/egress"
	"code.cloudfoundry.org/log-cache/internal/ingress"
	"code.cloudfoundry.org/log-cache/internal/orchestrator"
	"code.cloudfoundry.org/log-cache/internal/store"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	log        *log.Logger
	lis        net.Listener
	serverOpts []grpc.ServerOption
	metrics    Metrics

	maxPerSource int
	min          int
	proxy        *store.ProxyStore

	// Cluster Properties
	addr     string
	dialOpts []grpc.DialOption
}

// NewLogCache creates a new LogCache.
func New(opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		log:          log.New(ioutil.Discard, "", 0),
		metrics:      nopMetrics{},
		maxPerSource: 100000,
		min:          500000,
		addr:         ":8080",
		dialOpts:     []grpc.DialOption{grpc.WithInsecure()},
	}

	for _, o := range opts {
		o(cache)
	}

	return cache
}

// LogCacheOption configures a LogCache.
type LogCacheOption func(*LogCache)

// WithLogger returns a LogCacheOption that configures the logger used for
// the LogCache. Defaults to silent logger.
func WithLogger(l *log.Logger) LogCacheOption {
	return func(c *LogCache) {
		c.log = l
	}
}

// WithMaxPerSource returns a LogCacheOption that configures the store's
// memory size as number of envelopes for a specific sourceID. Defaults to
// 100000 envelopes.
func WithMaxPerSource(size int) LogCacheOption {
	return func(c *LogCache) {
		c.maxPerSource = size
	}
}

// WithAddr configures the address to listen for gRPC requests. It defaults to
// :8080.
func WithAddr(addr string) LogCacheOption {
	return func(c *LogCache) {
		c.addr = addr
	}
}

// WithServerOpts configures the gRPC server options. It defaults to an
// empty list
func WithServerOpts(opts ...grpc.ServerOption) LogCacheOption {
	return func(c *LogCache) {
		c.serverOpts = opts
	}
}

// WithMinimumSize sets the lower bound for pruning. It will not prune once
// this size is reached. Defaults to 500000.
func WithMinimumSize(min int) LogCacheOption {
	return func(c *LogCache) {
		c.min = min
	}
}

// WithDialOpts are the gRPC options used to dial peer Log Cache nodes. It
// defaults to WithInsecure().
func WithDialOpts(opts ...grpc.DialOption) LogCacheOption {
	return func(c *LogCache) {
		c.dialOpts = opts
	}
}

// Metrics registers Counter and Gauge metrics.
type Metrics interface {
	// NewCounter returns a function to increment for the given metric.
	NewCounter(name string) func(delta uint64)

	// NewGauge returns a function to set the value for the given metric.
	NewGauge(name string) func(value float64)
}

// WithMetrics returns a LogCacheOption that configures the metrics for the
// LogCache. It will add metrics to the given map.
func WithMetrics(m Metrics) LogCacheOption {
	return func(c *LogCache) {
		c.metrics = m
	}
}

// nopMetrics are the default metrics.
type nopMetrics struct{}

func (m nopMetrics) NewCounter(name string) func(uint64) {
	return func(uint64) {}
}

func (m nopMetrics) NewGauge(name string) func(float64) {
	return func(float64) {}
}

// Start starts the LogCache. It has an internal go-routine that it creates
// and therefore does not block.
func (c *LogCache) Start() {
	p := store.NewPruneConsultant(2, 70, NewMemoryAnalyzer(c.metrics))
	store := store.NewStore(c.maxPerSource, c.min, p, c.metrics)
	c.setupRouting(store)
}

func (c *LogCache) setupRouting(s *store.Store) {
	tableECMA := crc64.MakeTable(crc64.ECMA)
	hasher := func(s string) uint64 {
		return crc64.Checksum([]byte(s), tableECMA)
	}

	// gRPC
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.lis = lis
	c.log.Printf("listening on %s...", c.Addr())

	orch := orchestrator.New(hasher, s)
	localAddr := c.lis.Addr().String()
	lookup := orchestrator.NewRoutingTable(hasher, localAddr, orch.LastRanges)
	remotes := newRemotes(localAddr, lookup, c.dialOpts)
	ps := ingress.NewPubsub(lookup.Lookup, func(addr string) func(e *loggregator_v2.Envelope) {
		if addr == localAddr {
			return func(e *loggregator_v2.Envelope) {
				s.Put(e, e.GetSourceId())
			}
		}

		return remotes.peerWriter(addr).Write
	}, c.log)

	copier := store.NewCopier(s, func(sourceID string) []logcache.EgressClient {
		var r []logcache.EgressClient
		for _, addr := range lookup.LookupAll(sourceID) {
			if addr == c.addr {
				continue
			}

			r = append(r, remotes.peerWriter(addr))
		}
		return r
	}, c.log)

	c.proxy = store.NewProxyStore(copier, remotes)

	go func() {
		peerReader := ingress.NewPeerReader(ps.Publish, c.proxy)
		srv := grpc.NewServer(c.serverOpts...)
		logcache.RegisterIngressServer(srv, peerReader)
		logcache.RegisterEgressServer(srv, peerReader)
		logcache.RegisterOrchestrationServer(srv, struct {
			*orchestrator.Orchestrator
			*orchestrator.RoutingTable
		}{
			Orchestrator: orch,
			RoutingTable: lookup,
		})
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC ingress server: %s", err)
		}
	}()
}

// Addr returns the address that the LogCache is listening on. This is only
// valid after Start has been invoked.
func (c *LogCache) Addr() string {
	return c.lis.Addr().String()
}

type remotes struct {
	mu        sync.Mutex
	m         map[string]*egress.PeerWriter
	t         *orchestrator.RoutingTable
	localAddr string
	dialOpts  []grpc.DialOption
}

func newRemotes(localAddr string, t *orchestrator.RoutingTable, dialOpts []grpc.DialOption) *remotes {
	return &remotes{
		m:         make(map[string]*egress.PeerWriter),
		t:         t,
		dialOpts:  dialOpts,
		localAddr: localAddr,
	}
}

func (r *remotes) Lookup(sourceID string) (logcache.EgressClient, bool) {
	addr := r.t.Lookup(sourceID)
	if addr == r.localAddr {
		return nil, false
	}

	return r.peerWriter(addr), true
}

func (r *remotes) AllClients() []logcache.EgressClient {
	addrs := r.t.ListAllNodes()

	var c []logcache.EgressClient

	for _, addr := range addrs {
		if addr == r.localAddr {
			continue
		}

		c = append(c, r.peerWriter(addr))
	}

	return c
}

func (r *remotes) peerWriter(addr string) *egress.PeerWriter {
	r.mu.Lock()
	defer r.mu.Unlock()
	pw, ok := r.m[addr]
	if !ok {
		pw = egress.NewPeerWriter(addr, r.dialOpts...)
		r.m[addr] = pw
	}
	return pw
}
