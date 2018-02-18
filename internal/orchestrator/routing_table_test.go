package orchestrator_test

import (
	"context"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/log-cache/internal/orchestrator"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RoutingTable", func() {
	var (
		spyHasher *spyHasher
		spyLocal  *spyLocal
		r         *orchestrator.RoutingTable
	)

	BeforeEach(func() {
		spyHasher = newSpyHasher()
		spyLocal = newSpyLocal()
		r = orchestrator.NewRoutingTable(spyHasher.Hash, "local", spyLocal.Local)
	})

	It("returns the correct address for the node", func() {
		r.SetRanges(context.Background(), &rpc.SetRangesRequest{
			map[string]*rpc.Ranges{
				"a": {
					Ranges: []*rpc.Range{
						{Start: 0, End: 100, Term: 1},

						// Older term should and be ignored
						{Start: 101, End: 200, Term: 0},
					},
				},
				"b": {
					Ranges: []*rpc.Range{
						{Start: 101, End: 200, Term: 1},
					},
				},
				"c": {
					Ranges: []*rpc.Range{
						{Start: 201, End: 300, Term: 1},
					},
				},
			},
		})

		spyHasher.results = []uint64{200}

		i := r.Lookup("some-id")
		Expect(spyHasher.ids).To(ConsistOf("some-id"))
		Expect(i).To(Equal("b"))
	})

	It("replaces address for local", func() {
		spyLocal.results = []rpc.Range{
			{Start: 0, End: 100, Term: 1},
		}

		r.SetRanges(context.Background(), &rpc.SetRangesRequest{
			map[string]*rpc.Ranges{
				"a": {
					Ranges: []*rpc.Range{
						{Start: 0, End: 100, Term: 1},

						// Older term should and be ignored
						{Start: 101, End: 200, Term: 0},
					},
				},
				"b": {
					Ranges: []*rpc.Range{
						{Start: 101, End: 200, Term: 1},
					},
				},
				"c": {
					Ranges: []*rpc.Range{
						{Start: 201, End: 300, Term: 1},
					},
				},
			},
		})

		spyHasher.results = []uint64{100}

		i := r.Lookup("some-id")
		Expect(i).To(Equal("local"))
	})

	It("returns the correct address for the node", func() {
		r.SetRanges(context.Background(), &rpc.SetRangesRequest{
			map[string]*rpc.Ranges{
				"a": {
					Ranges: []*rpc.Range{
						{Start: 0, End: 100, Term: 1},
						{Start: 101, End: 200, Term: 0},
					},
				},
				"b": {
					Ranges: []*rpc.Range{
						{Start: 101, End: 200, Term: 1},
					},
				},
				"c": {
					Ranges: []*rpc.Range{
						{Start: 201, End: 300, Term: 1},
					},
				},
			},
		})

		spyHasher.results = []uint64{200}

		i := r.LookupAll("some-id")
		Expect(spyHasher.ids).To(ConsistOf("some-id"))
		Expect(i).To(ConsistOf("a", "b"))
	})

	It("returns an empty string for a non-routable hash", func() {
		i := r.Lookup("some-id")
		Expect(i).To(Equal(""))
	})

	It("returns each node address", func() {
		r.SetRanges(context.Background(), &rpc.SetRangesRequest{
			map[string]*rpc.Ranges{
				"a": {
					Ranges: []*rpc.Range{
						{Start: 0, End: 100, Term: 1},
						{Start: 101, End: 200, Term: 0},
					},
				},
				"b": {
					Ranges: []*rpc.Range{
						{Start: 101, End: 200, Term: 1},
					},
				},
				"c": {
					Ranges: []*rpc.Range{
						{Start: 201, End: 300, Term: 1},
					},
				},
			},
		})

		Expect(r.ListAllNodes()).To(ConsistOf("a", "b", "c"))
	})

	It("survives the race detector", func() {
		go func() {
			for i := 0; i < 100; i++ {
				r.Lookup("a")
			}
		}()

		go func() {
			for i := 0; i < 100; i++ {
				r.LookupAll("a")
			}
		}()

		go func() {
			for i := 0; i < 100; i++ {
				r.ListAllNodes()
			}
		}()

		for i := 0; i < 100; i++ {
			r.SetRanges(context.Background(), &rpc.SetRangesRequest{})
		}
	})
})

type spyLocal struct {
	results []rpc.Range
}

func newSpyLocal() *spyLocal {
	return &spyLocal{}
}

func (s *spyLocal) Local() []rpc.Range {
	return s.results
}
