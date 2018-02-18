package logcache

import (
	"io/ioutil"
	"log"
	"sync"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	orchestrator "code.cloudfoundry.org/go-orchestrator"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Scheduler manages the routes of the Log Cache nodes.
type Scheduler struct {
	log      *log.Logger
	metrics  Metrics
	interval time.Duration
	count    int
	orch     *orchestrator.Orchestrator
	dialOpts []grpc.DialOption

	logCaches []logCacheInfo
}

// NewScheduler returns a new Scheduler. Addrs are the addresses of the Log
// Cache nodes.
func NewScheduler(logCaches []string, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		log:      log.New(ioutil.Discard, "", 0),
		metrics:  nopMetrics{},
		interval: time.Minute,
		count:    100,
		orch:     orchestrator.New(&comm{}),
		dialOpts: []grpc.DialOption{grpc.WithInsecure()},
	}

	for _, o := range opts {
		o(s)
	}

	for _, addr := range logCaches {
		conn, err := grpc.Dial(addr, s.dialOpts...)
		if err != nil {
			s.log.Panic(err)
		}
		s.logCaches = append(s.logCaches, logCacheInfo{l: rpc.NewOrchestrationClient(conn), addr: addr})
	}

	return s
}

// SchedulerOption configures a Scheduler.
type SchedulerOption func(*Scheduler)

// WithSchedulerLogger returns a SchedulerOption that configures the logger
// used for the Scheduler. Defaults to silent logger.
func WithSchedulerLogger(l *log.Logger) SchedulerOption {
	return func(s *Scheduler) {
		s.log = l
	}
}

// WithSchedulerMetrics returns a SchedulerOption that configures the metrics
// for the Scheduler. It will add metrics to the given map.
func WithSchedulerMetrics(m Metrics) SchedulerOption {
	return func(s *Scheduler) {
		s.metrics = m
	}
}

// WithSchedulerInterval returns a SchedulerOption that configures the
// interval for terms to take place. It defaults to a minute.
func WithSchedulerInterval(interval time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.interval = interval
	}
}

// WithSchedulerCount returns a SchedulerOption that configures the
// number of ranges to manage. Defaults to 100.
func WithSchedulerCount(count int) SchedulerOption {
	return func(s *Scheduler) {
		s.count = count
	}
}

// WithSchedulerDialOpts are the gRPC options used to dial peer Log Cache
// nodes. It defaults to WithInsecure().
func WithSchedulerDialOpts(opts ...grpc.DialOption) SchedulerOption {
	return func(s *Scheduler) {
		s.dialOpts = opts
	}
}

// Start starts the scheduler. It does not block.
func (s *Scheduler) Start() {
	for _, lc := range s.logCaches {
		s.orch.AddWorker(lc)
	}

	maxHash := uint64(18446744073709551615)
	x := maxHash / uint64(s.count)
	var start uint64

	for i := 0; i < s.count-1; i++ {
		s.orch.AddTask(rpc.Range{
			Start: start,
			End:   start + x,
		})
	}

	s.orch.AddTask(rpc.Range{
		Start: start,
		End:   maxHash,
	})

	go func() {
		for range time.Tick(s.interval) {
			s.orch.NextTerm(context.Background())
			s.setRemoteTables(s.convertWorkerState(s.orch.LastActual()))
		}
	}()
}

func (s *Scheduler) setRemoteTables(m map[string]*rpc.Ranges) {
	req := &rpc.SetRangesRequest{
		Ranges: m,
	}

	for _, lc := range s.logCaches {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := lc.l.SetRanges(ctx, req); err != nil {
			s.log.Printf("failed to set remote table: %s", err)
			continue
		}
	}
}

func (s *Scheduler) convertWorkerState(ws []orchestrator.WorkerState) map[string]*rpc.Ranges {
	m := make(map[string]*rpc.Ranges)
	for _, w := range ws {
		var ranges []*rpc.Range
		for _, t := range w.Tasks {
			rr := t.(*rpc.Range)
			ranges = append(ranges, rr)
		}

		m[w.Name.(logCacheInfo).addr] = &rpc.Ranges{
			Ranges: ranges,
		}
	}

	return m
}

type logCacheInfo struct {
	l    rpc.OrchestrationClient
	addr string
}

type comm struct {
	mu   sync.Mutex
	term uint64
}

// List implements orchestrator.Communicator.
func (c *comm) List(ctx context.Context, worker interface{}) ([]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(logCacheInfo)

	resp, err := lc.l.ListRanges(ctx, &rpc.ListRangesRequest{})
	if err != nil {
		return nil, err
	}

	var results []interface{}
	for _, r := range resp.Ranges {
		if c.term <= r.Term {
			c.term = r.Term + 1
		}

		results = append(results, r)
	}

	return results, nil
}

// Add implements orchestrator.Communicator.
func (c *comm) Add(ctx context.Context, worker interface{}, task interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(logCacheInfo)
	r := task.(rpc.Range)
	r.Term = c.term

	_, err := lc.l.AddRange(ctx, &rpc.AddRangeRequest{
		Range: &r,
	})

	return err
}

// Remvoe implements orchestrator.Communicator.
func (c *comm) Remove(ctx context.Context, worker interface{}, task interface{}) error {
	// NOP
	return nil
}
