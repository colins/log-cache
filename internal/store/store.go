package store

import (
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
)

// Metrics is the client used for initializing counter and gauge metrics.
type Metrics interface {
	//NewCounter initializes a new counter metric.
	NewCounter(name string) func(delta uint64)

	//NewGauge initializes a new gauge metric.
	NewGauge(name string) func(value float64)
}

// Pruner is used to determine if the store should prune.
type Pruner interface {
	// Prune returns true if the store should prune entries. The returned
	// value is the length of entries to prune.
	Prune() int
}

// Store is an in memory data store for envelopes. It will keep a bounded
// number and drop older data once that threshold is exceeded. All functions
// are thread safe. The Pruner is used to know when entries should be
// pruned.
type Store struct {
	mu           sync.RWMutex
	maxPerSource int

	indexes map[string]*avltree.Tree

	// oldestValueTree stores each tree's oldest value for pruning. As data is
	// added and needs to be pruned, it is done so from here.
	oldestValueTree *treeStorage

	meta map[string]logcache_v1.MetaInfo

	// count is incremented each Put. It is used to determine when to prune. When
	// an envelope is pruned, it is decremented.
	count int
	min   int

	// metrics
	incExpired     func(delta uint64)
	setCachePeriod func(value float64)
	incIngress     func(delta uint64)
	incEgress      func(delta uint64)
	setStoreSize   func(value float64)

	p Pruner
}

// NewStore creates a new store.
func NewStore(maxPerSource, min int, p Pruner, m Metrics) *Store {
	return &Store{
		maxPerSource:    maxPerSource,
		p:               p,
		indexes:         make(map[string]*avltree.Tree),
		meta:            make(map[string]logcache_v1.MetaInfo),
		oldestValueTree: newTreeStorage(),
		min:             min,

		incExpired:     m.NewCounter("Expired"),
		setCachePeriod: m.NewGauge("CachePeriod"),
		incIngress:     m.NewCounter("Ingress"),
		incEgress:      m.NewCounter("Egress"),
		setStoreSize:   m.NewGauge("StoreSize"),
	}
}

// Put adds a batch of envelopes into the store.
func (s *Store) Put(e *loggregator_v2.Envelope, index string) {
	s.incIngress(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.indexes[index]
	if !ok {
		t = avltree.NewWith(utils.Int64Comparator)
		s.indexes[index] = t

		// Store the tree for pruning purposes.
		s.oldestValueTree.Put(e.Timestamp, t)
	}

	var (
		oldest    int64
		hasOldest bool
	)
	if t.Size() > 0 {
		oldest = t.Left().Key.(int64)
		hasOldest = true
	}

	preSize := t.Size()

	if preSize >= s.maxPerSource {
		// This index has reached/exceeded its allowed quota. Truncate the
		// oldest before putting a new envelope in.
		t.Remove(oldest)
		s.incExpired(1)

		// Update the meta
		m := s.meta[index]
		m.Expired++
		s.meta[index] = m
	}

	t.Put(e.Timestamp, envelopeWrapper{e: e, index: index})

	// Only increment if we didn't overwrite.
	s.count += t.Size() - preSize

	newOldest := t.Left().Key.(int64)
	if oldest != newOldest && hasOldest {
		s.oldestValueTree.Remove(oldest, t)
		s.oldestValueTree.Put(newOldest, t)
	}

	s.truncate()
	s.setStoreSize(float64(s.count))

	// Set Cache Period
	oldestValue, _ := s.oldestValueTree.Left()
	cachePeriod := (time.Now().UnixNano() - oldestValue) / int64(time.Millisecond)
	s.setCachePeriod(float64(cachePeriod))

	// Update the meta
	m := s.meta[index]
	if e.GetTimestamp() > m.NewestTimestamp {
		m.NewestTimestamp = e.GetTimestamp()
	}

	if t.Size() > 0 {
		m.OldestTimestamp = t.Left().Key.(int64)
		s.meta[index] = m
	}
}

// truncate removes the oldest envelope from the entire cache. It considers
// each source-id.
func (s *Store) truncate() {
	prune := s.p.Prune()
	for i := 0; i < prune; i++ {
		// Prevent the whole cache from being pruned
		if s.count <= s.min {
			return
		}

		s.count--
		s.incExpired(1)

		// dereference the node so that after we remove it, the pointer does not
		// get updated underneath us.
		key, oldTree := s.oldestValueTree.Left()
		s.oldestValueTree.Remove(key, oldTree)

		// Truncate the oldest envelope.
		left := oldTree.Left()

		index := left.Value.(envelopeWrapper).index

		oldTree.Remove(key)

		if oldTree.Size() == 0 {
			// Remove the index completely.
			delete(s.indexes, index)
			delete(s.meta, index)
			continue
		}

		newOldest := oldTree.Left().Key.(int64)

		// Update the meta
		m := s.meta[index]
		m.Expired++
		m.OldestTimestamp = newOldest
		s.meta[index] = m

		// Add tree back to oldestValueTree for future pruning.
		s.oldestValueTree.Put(newOldest, oldTree)
	}
}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (s *Store) Get(
	index string,
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.indexes[index]
	if !ok {
		return nil
	}

	traverser := s.treeAscTraverse
	if descending {
		traverser = s.treeDescTraverse
	}

	var res []*loggregator_v2.Envelope
	traverser(t.Root, start.UnixNano(), end.UnixNano(), func(e *loggregator_v2.Envelope, idx string) bool {
		if idx == index && s.validEnvelopeType(e, envelopeTypes) {
			res = append(res, e)
		}

		// Return true to stop traversing
		return len(res) >= limit
	})

	s.incEgress(uint64(len(res)))
	return res
}

func (s *Store) validEnvelopeType(e *loggregator_v2.Envelope, types []logcache_v1.EnvelopeType) bool {
	if types == nil {
		return true
	}
	for _, t := range types {
		if s.checkEnvelopeType(e, t) {
			return true
		}
	}
	return false
}

func (s *Store) treeAscTraverse(
	n *avltree.Node,
	start int64,
	end int64,
	f func(e *loggregator_v2.Envelope, index string) bool,
) bool {
	if n == nil {
		return false
	}

	t := n.Key.(int64)
	if t >= start {
		if s.treeAscTraverse(n.Children[0], start, end, f) {
			return true
		}

		w := n.Value.(envelopeWrapper)

		if t >= end || f(w.e, w.index) {
			return true
		}
	}

	return s.treeAscTraverse(n.Children[1], start, end, f)
}

func (s *Store) treeDescTraverse(
	n *avltree.Node,
	start int64,
	end int64,
	f func(e *loggregator_v2.Envelope, index string) bool,
) bool {
	if n == nil {
		return false
	}

	t := n.Key.(int64)
	if t < end {
		if s.treeDescTraverse(n.Children[1], start, end, f) {
			return true
		}

		w := n.Value.(envelopeWrapper)

		if t < start || f(w.e, w.index) {
			return true
		}
	}

	return s.treeDescTraverse(n.Children[0], start, end, f)
}

func (s *Store) checkEnvelopeType(e *loggregator_v2.Envelope, t logcache_v1.EnvelopeType) bool {
	if t == logcache_v1.EnvelopeType_ANY {
		return true
	}

	switch t {
	case logcache_v1.EnvelopeType_LOG:
		return e.GetLog() != nil
	case logcache_v1.EnvelopeType_COUNTER:
		return e.GetCounter() != nil
	case logcache_v1.EnvelopeType_GAUGE:
		return e.GetGauge() != nil
	case logcache_v1.EnvelopeType_TIMER:
		return e.GetTimer() != nil
	case logcache_v1.EnvelopeType_EVENT:
		return e.GetEvent() != nil
	default:
		// This should never happen. This implies the store is being used
		// poorly.
		panic("unknown type")
	}
}

// Meta returns each source ID tracked in the store.
func (s *Store) Meta() map[string]logcache_v1.MetaInfo {
	meta := make(map[string]logcache_v1.MetaInfo)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Copy the map so that we don't leak the lock protected map beyond the
	// locks.
	for k, v := range s.meta {
		v.Count = int64(s.indexes[k].Size())
		meta[k] = v
	}
	return meta
}

// treeStorage stores the trees and sorts them with respect to time. It
// prevents overwrites for the same key.
type treeStorage struct {
	t *avltree.Tree
}

func newTreeStorage() *treeStorage {
	return &treeStorage{
		t: avltree.NewWith(utils.Int64Comparator),
	}
}

func (s *treeStorage) Put(key int64, t *avltree.Tree) {
	var values []*avltree.Tree
	if existing, found := s.t.Get(key); found {
		values = existing.([]*avltree.Tree)
	}

	s.t.Put(key, append(values, t))
}

func (s *treeStorage) Remove(key int64, t *avltree.Tree) {
	var values []*avltree.Tree
	if existing, found := s.t.Get(key); found {
		values = existing.([]*avltree.Tree)
	}

	for i, v := range values {
		if v == t {
			values = append(values[:i], values[i+1:]...)
			break
		}
	}

	if len(values) == 0 {
		s.t.Remove(key)
		return
	}

	s.t.Put(key, values)
}

func (s *treeStorage) Left() (int64, *avltree.Tree) {
	l := s.t.Left()
	return l.Key.(int64), l.Value.([]*avltree.Tree)[0]
}

type envelopeWrapper struct {
	e     *loggregator_v2.Envelope
	index string
}
