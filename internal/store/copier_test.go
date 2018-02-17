package store_test

import (
	"io/ioutil"
	"log"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Copier", func() {
	var (
		spyEgressClient1 *spyEgressClient
		spyEgressClient2 *spyEgressClient
		spyLocalStore    *spyLocalStore
		c                *store.Copier
	)

	BeforeEach(func() {
		spyEgressClient1 = newSpyEgressClient()
		spyEgressClient2 = newSpyEgressClient()
		spyLocalStore = newSpyLocalStore()
		c = store.NewCopier(spyLocalStore, func(sourceID string) []rpc.EgressClient {
			switch sourceID {
			case "a":
				return []rpc.EgressClient{spyEgressClient1}
			case "b":
				return []rpc.EgressClient{spyEgressClient2}
			case "c":
				return []rpc.EgressClient{spyEgressClient1, spyEgressClient2}
			default:
				panic("unknown source ID")
			}
		}, log.New(ioutil.Discard, "", 0))
	})

	It("first reads from the remote store and writes to the local store", func() {
		spyEgressClient1.results = []*loggregator_v2.Envelope{
			{Timestamp: 7, SourceId: "a"},
			{Timestamp: 8, SourceId: "a"},
		}

		c.Get("a", time.Unix(0, 5), time.Unix(0, 10), &loggregator_v2.Counter{}, 10, true)
		Eventually(spyEgressClient1.Requests).Should(HaveLen(1))
		Expect(spyEgressClient1.Requests()[0].SourceId).To(Equal("a"))
		Expect(spyEgressClient1.Requests()[0].StartTime).To(Equal(int64(5)))
		Expect(spyEgressClient1.Requests()[0].EndTime).To(Equal(int64(10)))
		Expect(spyEgressClient1.Requests()[0].EnvelopeType).To(Equal(rpc.EnvelopeTypes_COUNTER))
		Expect(spyEgressClient1.Requests()[0].Limit).To(Equal(int64(10)))
		Expect(spyEgressClient1.Requests()[0].Descending).To(BeTrue())

		Expect(spyLocalStore.Envelopes()).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 7, SourceId: "a"},
			&loggregator_v2.Envelope{Timestamp: 8, SourceId: "a"},
		))

		Expect(spyLocalStore.Indexes()).To(ConsistOf("a", "a"))
	})

	It("returns results from the local store", func() {
		spyLocalStore.result = []*loggregator_v2.Envelope{
			{Timestamp: 7, SourceId: "a"},
			{Timestamp: 8, SourceId: "a"},
		}
		es := c.Get("a", time.Unix(0, 5), time.Unix(0, 10), &loggregator_v2.Counter{}, 10, false)

		Expect(spyLocalStore.sourceID).To(Equal("a"))
		Expect(spyLocalStore.start).To(Equal(time.Unix(0, 5)))
		Expect(spyLocalStore.end).To(Equal(time.Unix(0, 10)))
		Expect(spyLocalStore.envelopeType).To(Equal(&loggregator_v2.Counter{}))
		Expect(spyLocalStore.limit).To(Equal(10))
		Expect(spyLocalStore.descending).To(BeFalse())

		Expect(es).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 7, SourceId: "a"},
			&loggregator_v2.Envelope{Timestamp: 8, SourceId: "a"},
		))
	})

	It("each get is async", func() {
		spyEgressClient1.readBlock = true
		spyEgressClient2.results = []*loggregator_v2.Envelope{
			{Timestamp: 7, SourceId: "c"},
			{Timestamp: 8, SourceId: "c"},
		}
		c.Get("c", time.Unix(0, 5), time.Unix(0, 10), &loggregator_v2.Counter{}, 10, false)

		Eventually(spyLocalStore.Envelopes).Should(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 7, SourceId: "c"},
			&loggregator_v2.Envelope{Timestamp: 8, SourceId: "c"},
		))
	})

	It("returns meta from the store", func() {
		spyLocalStore.metaResult = map[string]store.MetaInfo{
			"a": {},
		}

		Expect(c.Meta()).To(HaveKey("a"))
	})
})
