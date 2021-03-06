package logcache

import (
	"io/ioutil"
	"log"
	"sync"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	orchestrator "code.cloudfoundry.org/go-orchestrator"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Scheduler manages the routes of the Log Cache nodes.
type Scheduler struct {
	log               *log.Logger
	metrics           Metrics
	interval          time.Duration
	count             int
	replicationFactor int
	logCacheOrch      *orchestrator.Orchestrator
	groupOrch         *orchestrator.Orchestrator
	dialOpts          []grpc.DialOption
	isLeader          func() bool

	logCacheClients []clientInfo
	groupClients    []clientInfo
}

// NewScheduler returns a new Scheduler. Addrs are the addresses of the Cache
// nodes.
func NewScheduler(logCacheAddrs, groupAddrs []string, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		log:               log.New(ioutil.Discard, "", 0),
		metrics:           nopMetrics{},
		interval:          time.Minute,
		count:             100,
		replicationFactor: 1,
		dialOpts:          []grpc.DialOption{grpc.WithInsecure()},
		isLeader:          func() bool { return true },
	}

	for _, o := range opts {
		o(s)
	}

	s.logCacheOrch = orchestrator.New(&comm{
		log:      s.log,
		isLeader: s.isLeader,
	})

	s.groupOrch = orchestrator.New(&comm{
		log:      s.log,
		isLeader: s.isLeader,
	})

	for _, addr := range logCacheAddrs {
		conn, err := grpc.Dial(addr, s.dialOpts...)
		if err != nil {
			s.log.Panic(err)
		}
		s.logCacheClients = append(s.logCacheClients, clientInfo{l: rpc.NewOrchestrationClient(conn), addr: addr})
	}

	for _, addr := range groupAddrs {
		conn, err := grpc.Dial(addr, s.dialOpts...)
		if err != nil {
			s.log.Panic(err)
		}
		s.groupClients = append(s.groupClients, clientInfo{l: rpc.NewOrchestrationClient(conn), addr: addr})
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

// WithSchedulerReplicationFactor returns a SchedulerOption that configures
// the replication factor for the Log Cache cluster. Replication factor is the
// total number of nodes to replicate data across. It defaults to 1 (meaning
// no replication).
func WithSchedulerReplicationFactor(replicationFactor int) SchedulerOption {
	return func(s *Scheduler) {
		s.replicationFactor = replicationFactor
	}
}

// WithSchedulerDialOpts are the gRPC options used to dial peer Log Cache
// nodes. It defaults to WithInsecure().
func WithSchedulerDialOpts(opts ...grpc.DialOption) SchedulerOption {
	return func(s *Scheduler) {
		s.dialOpts = opts
	}
}

// WithSchedulerLeadership sets the leadership decsision function that returns
// true if the scheduler node is the leader. Default to a fuction that returns
// true.
func WithSchedulerLeadership(isLeader func() bool) SchedulerOption {
	return func(s *Scheduler) {
		s.isLeader = isLeader
	}
}

// Start starts the scheduler. It does not block.
func (s *Scheduler) Start() {
	for _, lc := range s.logCacheClients {
		s.logCacheOrch.AddWorker(lc)
	}

	for _, lc := range s.groupClients {
		s.groupOrch.AddWorker(lc)
	}

	maxHash := uint64(18446744073709551615)
	x := maxHash / uint64(s.count)
	var start uint64

	for i := 0; i < s.count-1; i++ {
		s.logCacheOrch.AddTask(rpc.Range{
			Start: start,
			End:   start + x,
		},
			orchestrator.WithTaskInstances(s.replicationFactor),
		)

		s.groupOrch.AddTask(rpc.Range{
			Start: start,
			End:   start + x,
		},
			orchestrator.WithTaskInstances(5),
		)
		start += x + 1
	}

	s.logCacheOrch.AddTask(rpc.Range{
		Start: start,
		End:   maxHash,
	},
		orchestrator.WithTaskInstances(s.replicationFactor),
	)

	s.groupOrch.AddTask(rpc.Range{
		Start: start,
		End:   maxHash,
	},
		orchestrator.WithTaskInstances(5),
	)

	go func() {
		for range time.Tick(s.interval) {

			// Apply changes
			s.logCacheOrch.NextTerm(context.Background())
			s.groupOrch.NextTerm(context.Background())

			// Run again before setting remote tables to allow the
			// orchestrator to go and query for updates.
			s.logCacheOrch.NextTerm(context.Background())
			s.groupOrch.NextTerm(context.Background())

			if s.isLeader() {
				s.setRemoteTables(s.logCacheClients, s.convertWorkerState(s.logCacheOrch.LastActual()))
				s.setRemoteTables(s.groupClients, s.convertWorkerState(s.groupOrch.LastActual()))
			}
		}
	}()
}

func (s *Scheduler) setRemoteTables(clients []clientInfo, m map[string]*rpc.Ranges) {
	req := &rpc.SetRangesRequest{
		Ranges: m,
	}

	for _, lc := range clients {
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
			rr := t.(rpc.Range)
			ranges = append(ranges, &rr)
		}

		m[w.Name.(clientInfo).addr] = &rpc.Ranges{
			Ranges: ranges,
		}
	}

	return m
}

type clientInfo struct {
	l    rpc.OrchestrationClient
	addr string
}

type comm struct {
	isLeader func() bool
	mu       sync.Mutex
	log      *log.Logger
}

// List implements orchestrator.Communicator.
func (c *comm) List(ctx context.Context, worker interface{}) ([]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(clientInfo)
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	resp, err := lc.l.ListRanges(ctx, &rpc.ListRangesRequest{})
	if err != nil {
		c.log.Printf("failed to list ranges from %s: %s", lc.addr, err)
		return nil, err
	}

	var results []interface{}
	for _, r := range resp.Ranges {
		results = append(results, *r)
	}

	return results, nil
}

// Add implements orchestrator.Communicator.
func (c *comm) Add(ctx context.Context, worker interface{}, task interface{}) error {
	if !c.isLeader() {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(clientInfo)
	r := task.(rpc.Range)
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	_, err := lc.l.AddRange(ctx, &rpc.AddRangeRequest{
		Range: &r,
	})

	if err != nil {
		c.log.Printf("failed to add range to %s: %s", lc.addr, err)
		return err
	}

	return nil
}

// Remvoe implements orchestrator.Communicator.
func (c *comm) Remove(ctx context.Context, worker interface{}, task interface{}) error {
	if !c.isLeader() {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	lc := worker.(clientInfo)
	r := task.(rpc.Range)
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	_, err := lc.l.RemoveRange(ctx, &rpc.RemoveRangeRequest{
		Range: &r,
	})

	if err != nil {
		c.log.Printf("failed to add range to %s: %s", lc.addr, err)
		return err
	}

	return nil
}
