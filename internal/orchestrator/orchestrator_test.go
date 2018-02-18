package orchestrator_test

import (
	"context"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/log-cache/internal/orchestrator"
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		spyHasher      *spyHasher
		spyMetaFetcher *spyMetaFetcher
		o              *orchestrator.Orchestrator
	)

	BeforeEach(func() {
		spyHasher = newSpyHasher()
		spyMetaFetcher = newSpyMetaFetcher()
		o = orchestrator.New(spyHasher.Hash, spyMetaFetcher)
	})

	It("always keep the latest term", func() {
		// SpyHasher returns 0 by default
		spyMetaFetcher.results["a"] = store.MetaInfo{
			Count: 1,
		}
		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   1,
				Term:  1,
			},
		})

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   1,
				Term:  2,
			},
		})

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 1,
				End:   2,
				Term:  2,
			},
		})

		resp, err := o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf([]*rpc.Range{
			{
				Start: 0,
				End:   1,
				Term:  2,
			},
			{
				Start: 1,
				End:   2,
				Term:  2,
			},
		}))
	})

	It("keeps older terms with meta available", func() {
		// SpyHasher returns 0 by default
		spyMetaFetcher.results["a"] = store.MetaInfo{
			Count: 1,
		}

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   1,
				Term:  1,
			},
		})

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 1,
				End:   2,
				Term:  2,
			},
		})

		resp, err := o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf([]*rpc.Range{
			{
				Start: 0,
				End:   1,
				Term:  1,
			},
			{
				Start: 1,
				End:   2,
				Term:  2,
			},
		}))
	})

	It("survives race the detector", func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			wg.Done()
			for i := 0; i < 100; i++ {
				o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
			}
		}()
		wg.Wait()

		for i := 0; i < 100; i++ {
			o.AddRange(context.Background(), &rpc.AddRangeRequest{
				Range: &rpc.Range{
					Start: 1,
					End:   2,
					Term:  2,
				},
			})
		}
	})
})

type spyMetaFetcher struct {
	results map[string]store.MetaInfo
}

func newSpyMetaFetcher() *spyMetaFetcher {
	return &spyMetaFetcher{
		results: make(map[string]store.MetaInfo),
	}
}

func (f *spyMetaFetcher) Meta() map[string]store.MetaInfo {
	return f.results
}

type spyHasher struct {
	mu      sync.Mutex
	ids     []string
	results []uint64
}

func newSpyHasher() *spyHasher {
	return &spyHasher{}
}

func (s *spyHasher) Hash(id string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, id)

	if len(s.results) == 0 {
		return 0
	}

	r := s.results[0]
	s.results = s.results[1:]
	return r
}
