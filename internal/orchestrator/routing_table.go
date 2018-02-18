package orchestrator

import (
	"context"
	"sort"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
)

// RoutingTable makes decisions for where a sourceID should be routed.
type RoutingTable struct {
	h func(string) uint64

	mu    sync.RWMutex
	addrs []string
	table []rangeInfo

	localAddr string
	local     func() []rpc.Range

	avlTable *avltree.Tree
}

// NewRoutingTable returns a new RoutingTable.
func NewRoutingTable(hasher func(string) uint64, localAddr string, local func() []rpc.Range) *RoutingTable {
	return &RoutingTable{
		h:         hasher,
		localAddr: localAddr,
		local:     local,
		avlTable:  avltree.NewWith(utils.UInt64Comparator),
	}
}

// Lookup takes a source ID, hash it and determine what node it should be
// routed to.
func (t *RoutingTable) Lookup(sourceID string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.lookup(t.h(sourceID), t.avlTable.Root)
}

func (t *RoutingTable) lookup(hash uint64, n *avltree.Node) string {
	if n == nil {
		return ""
	}

	r := n.Value.(rangeInfo)
	if hash < r.r.Start {
		return t.lookup(hash, n.Children[0])
	}

	if hash > r.r.End {
		return t.lookup(hash, n.Children[1])
	}

	if hash > r.r.Start && hash <= r.r.End {
		return r.addr
	}

	return ""
}

// LookupAll returns every address that has a range where the sourceID would
// fall under.
func (t *RoutingTable) LookupAll(sourceID string) []string {
	h := t.h(sourceID)

	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []string
	ranges := t.table

	for {
		i := t.findRange(h, ranges)
		if i < 0 {
			break
		}
		result = append(result, ranges[i].addr)
		ranges = ranges[i+1:]
	}

	return result
}

// ListAllNodes returns each node address.
func (t *RoutingTable) ListAllNodes() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.addrs
}

// SetRanges sets the routing table.
func (t *RoutingTable) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	localRanges := t.local()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.table = nil
	t.addrs = nil

	// TODO: Triple for loop? Do better!
	var latestTerm uint64
	for addr, ranges := range in.Ranges {
		t.addrs = append(t.addrs, addr)
		for _, r := range ranges.Ranges {
			for _, lr := range localRanges {
				if lr.Start == r.Start && lr.End == r.End && lr.Term == r.Term {
					addr = t.localAddr
					break
				}
			}

			t.table = append(t.table, rangeInfo{
				addr: addr,
				r:    *r,
			})

			if latestTerm < r.Term {
				latestTerm = r.Term
			}
		}
	}

	sort.Sort(rangeInfos(t.table))

	t.avlTable = avltree.NewWith(utils.UInt64Comparator)
	for _, r := range t.table {
		if r.r.Term == latestTerm {
			t.avlTable.Put(r.r.Start, r)
		}
	}

	return &rpc.SetRangesResponse{}, nil
}

func (t *RoutingTable) findRange(h uint64, rs []rangeInfo) int {
	for i, r := range rs {
		if h < r.r.Start || h > r.r.End {
			// Outside of range
			continue
		}
		return i
	}

	return -1
}

type rangeInfo struct {
	r    rpc.Range
	addr string
}

type rangeInfos []rangeInfo

func (r rangeInfos) Len() int {
	return len(r)
}

func (r rangeInfos) Less(i, j int) bool {
	if r[i].r.Start == r[j].r.Start {
		return r[i].r.Term > r[j].r.Term
	}

	return r[i].r.Start < r[j].r.Start
}

func (r rangeInfos) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
