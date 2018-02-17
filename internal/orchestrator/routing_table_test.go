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
		r         *orchestrator.RoutingTable
	)

	BeforeEach(func() {
		spyHasher = newSpyHasher()
		r = orchestrator.NewRoutingTable([]string{"a", "b", "c"}, spyHasher.Hash)
	})

	It("returns the correct index for the node", func() {
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
		Expect(i).To(Equal(1))
	})

	It("returns the correct index for the node", func() {
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
		Expect(i).To(ConsistOf(0, 1))
	})

	It("returns -1 for a non-routable hash", func() {
		i := r.Lookup("some-id")
		Expect(i).To(Equal(-1))
	})
})
