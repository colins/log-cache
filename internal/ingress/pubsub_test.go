package ingress_test

import (
	"io/ioutil"
	"log"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pubsub", func() {
	var (
		spyWriterFetcher *spyWriterFetcher
		spyWriter1       *spyWriter
		spyWriter2       *spyWriter

		r *ingress.Pubsub

		lookupSourceID string
		lookupResult   string
	)

	BeforeEach(func() {
		lookup := func(sourceID string) string {
			lookupSourceID = sourceID
			return lookupResult
		}

		spyWriter1 = newSpyWriter()
		spyWriter2 = newSpyWriter()
		spyWriterFetcher = newSpyWriterFetcher()
		spyWriterFetcher.m["a"] = spyWriter1
		spyWriterFetcher.m["b"] = spyWriter2

		r = ingress.NewPubsub(lookup, spyWriterFetcher.Fetch, log.New(ioutil.Discard, "", 0))
	})

	It("routes data via lookup function", func() {
		lookupResult = "a"
		expected := &loggregator_v2.Envelope{SourceId: "some-id"}
		r.Publish(expected)

		Expect(spyWriterFetcher.addrs).To(ConsistOf("a"))
		Expect(lookupSourceID).To(Equal("some-id"))
		Expect(spyWriter1.es).To(ConsistOf(expected))
	})

	It("does not write data that does not resolve to a route", func() {
		lookupResult = ""
		expected := &loggregator_v2.Envelope{SourceId: "some-id"}
		r.Publish(expected)

		Expect(spyWriterFetcher.addrs).To(HaveLen(0))
	})
})

type spyWriterFetcher struct {
	addrs []string
	m     map[string]*spyWriter
}

func newSpyWriterFetcher() *spyWriterFetcher {
	return &spyWriterFetcher{
		m: make(map[string]*spyWriter),
	}
}

func (s *spyWriterFetcher) Fetch(addr string) func(*loggregator_v2.Envelope) {
	s.addrs = append(s.addrs, addr)
	return s.m[addr].Write
}

type spyWriter struct {
	rpc.IngressClient

	es []*loggregator_v2.Envelope
}

func newSpyWriter() *spyWriter {
	return &spyWriter{}
}

func (s *spyWriter) Write(e *loggregator_v2.Envelope) {
	s.es = append(s.es, e)
}
