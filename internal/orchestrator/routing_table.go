package orchestrator

import (
	"context"
	"sort"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
)

// RoutingTable makes decisions for where a sourceID should be routed.
type RoutingTable struct {
	addrs map[string]int
	h     func(string) uint64

	table []rangeInfo
}

// NewRoutingTable returns a new RoutingTable.
func NewRoutingTable(addrs []string, hasher func(string) uint64) *RoutingTable {
	a := make(map[string]int)
	for i, addr := range addrs {
		a[addr] = i
	}

	return &RoutingTable{
		addrs: a,
		h:     hasher,
	}
}

// Lookup takes a source ID, hash it and determine what node it should be
// routed to.
func (t *RoutingTable) Lookup(sourceID string) int {
	idx := t.findRange(t.h(sourceID), t.table)
	if idx < 0 {
		return -1
	}

	idx, ok := t.addrs[t.table[idx].addr]
	if !ok {
		return -1
	}

	return idx
}

// LookupAll returns every index that has a range where the sourceID would
// fall under.
func (t *RoutingTable) LookupAll(sourceID string) []int {
	h := t.h(sourceID)

	var result []int
	ranges := t.table

	for {
		i := t.findRange(h, ranges)
		if i < 0 {
			break
		}
		result = append(result, t.addrs[ranges[i].addr])
		ranges = ranges[i+1:]
	}

	return result
}

// SetRanges sets the routing table.
func (t *RoutingTable) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	t.table = nil
	for addr, ranges := range in.Ranges {
		for _, r := range ranges.Ranges {
			t.table = append(t.table, rangeInfo{
				addr: addr,
				r:    *r,
			})
		}
	}

	sort.Sort(rangeInfos(t.table))

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
