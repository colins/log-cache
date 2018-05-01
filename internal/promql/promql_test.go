package promql_test

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/promql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Engine", func() {
	var (
		spyDataReader *spyDataReader
		q             *promql.PromQL
	)

	BeforeEach(func() {
		spyDataReader = newSpyDataReader()

		q = promql.New(
			3,
			spyDataReader,
			log.New(ioutil.Discard, "", 0),
		)
	})

	It("returns a scalar", func() {
		r, err := q.InstantQuery(`7*9`)
		Expect(err).ToNot(HaveOccurred())

		Expect(time.Unix(0, r.GetScalar().GetTime())).To(
			BeTemporally("~", time.Now(), time.Second),
		)

		Expect(r.GetScalar().GetValue()).To(Equal(63.0))
	})

	It("returns a vector", func() {
		now := time.Now()
		spyDataReader.readErrs = []error{nil, nil}
		spyDataReader.readResults = [][]*loggregator_v2.Envelope{
			{{
				SourceId:  "some-id-1",
				Timestamp: now.UnixNano(),
				Message: &loggregator_v2.Envelope_Counter{
					Counter: &loggregator_v2.Counter{
						Name:  "metric",
						Total: 99,
					},
				},
				Tags: map[string]string{
					"a": "tag-a",
					"b": "tag-b",
				},
			}},
			{{
				SourceId:  "some-id-2",
				Timestamp: now.UnixNano(),
				Message: &loggregator_v2.Envelope_Counter{
					Counter: &loggregator_v2.Counter{
						Name:  "metric",
						Total: 101,
					},
				},
				Tags: map[string]string{
					"a": "tag-a",
					"b": "tag-b",
				},
			}},
		}

		r, err := q.InstantQuery(`metric{source_id="some-id-1"} + metric{source_id="some-id-2"}`)
		Expect(err).ToNot(HaveOccurred())

		Expect(r.GetVector().GetSamples()).To(HaveLen(1))

		actualTime := r.GetVector().GetSamples()[0].Point.Time
		Expect(time.Unix(0, actualTime)).To(BeTemporally("~", now, time.Second))

		Expect(r.GetVector().GetSamples()).To(Equal([]*logcache_v1.PromQL_Sample{
			{
				Metric: map[string]string{
					"a": "tag-a",
					"b": "tag-b",
				},
				Point: &logcache_v1.PromQL_Point{
					Time:  actualTime,
					Value: 200,
				},
			},
		}))

		Eventually(spyDataReader.ReadSourceIDs).Should(
			ConsistOf("some-id-1", "some-id-2"),
		)
	})

	It("returns a matrix", func() {
		now := time.Now()
		spyDataReader.readErrs = []error{nil}
		spyDataReader.readResults = [][]*loggregator_v2.Envelope{
			{{
				SourceId:  "some-id-1",
				Timestamp: now.UnixNano(),
				Message: &loggregator_v2.Envelope_Counter{
					Counter: &loggregator_v2.Counter{
						Name:  "metric",
						Total: 99,
					},
				},
				Tags: map[string]string{
					"a": "tag-a",
					"b": "tag-b",
				},
			}},
		}

		r, err := q.InstantQuery(`metric{source_id="some-id-1"}[5m]`)
		Expect(err).ToNot(HaveOccurred())

		Expect(r.GetMatrix().GetSeries()).To(Equal([]*logcache_v1.PromQL_Series{
			{
				Metric: map[string]string{
					"a": "tag-a",
					"b": "tag-b",
				},
				Points: []*logcache_v1.PromQL_Point{{
					Time:  now.Truncate(time.Second).UnixNano(),
					Value: 99,
				}},
			},
		}))

		Eventually(spyDataReader.ReadSourceIDs).Should(
			ConsistOf("some-id-1"),
		)
	})

	It("returns an error for an invalid query", func() {
		_, err := q.InstantQuery(`invalid.query`)
		Expect(err).To(HaveOccurred())
	})

	// It("it recovers if it does not fail too often", func() {
	// 	ticker <- time.Now()
	// 	Consistently(p.Predicate).Should(BeTrue())

	// 	spyDataReader.setRead([][]*loggregator_v2.Envelope{
	// 		{{
	// 			SourceId:  "some-id-1",
	// 			Timestamp: time.Now().UnixNano(),
	// 			Message: &loggregator_v2.Envelope_Counter{
	// 				Counter: &loggregator_v2.Counter{
	// 					Name:  "metric",
	// 					Total: 99,
	// 				},
	// 			},
	// 		}},
	// 		{{
	// 			SourceId:  "some-id-2",
	// 			Timestamp: time.Now().UnixNano(),
	// 			Message: &loggregator_v2.Envelope_Counter{
	// 				Counter: &loggregator_v2.Counter{
	// 					Name:  "metric",
	// 					Total: 99,
	// 				},
	// 			},
	// 		}},
	// 	},
	// 		[]error{nil, nil},
	// 	)

	// 	ticker <- time.Now()
	// 	Eventually(p.Predicate).Should(BeTrue())
	// })

	// It("it stays false once it fails enough times", func() {
	// 	// We have the max num of failures set to 3.
	// 	ticker <- time.Now()
	// 	ticker <- time.Now()
	// 	ticker <- time.Now()
	// 	ticker <- time.Now()
	// 	Eventually(p.Predicate).Should(BeFalse())

	// 	spyDataReader.setRead([][]*loggregator_v2.Envelope{
	// 		{{
	// 			SourceId:  "some-id-1",
	// 			Timestamp: time.Now().UnixNano(),
	// 			Message: &loggregator_v2.Envelope_Counter{
	// 				Counter: &loggregator_v2.Counter{
	// 					Name:  "metric",
	// 					Total: 99,
	// 				},
	// 			},
	// 		}},
	// 		{{
	// 			SourceId:  "some-id-2",
	// 			Timestamp: time.Now().UnixNano(),
	// 			Message: &loggregator_v2.Envelope_Counter{
	// 				Counter: &loggregator_v2.Counter{
	// 					Name:  "metric",
	// 					Total: 99,
	// 				},
	// 			},
	// 		}},
	// 	},
	// 		[]error{nil, nil},
	// 	)

	// 	ticker <- time.Now()
	// 	Consistently(p.Predicate).Should(BeFalse())
	// })
})

type spyDataReader struct {
	mu            sync.Mutex
	readSourceIDs []string
	readStarts    []time.Time

	readResults [][]*loggregator_v2.Envelope
	readErrs    []error
}

func newSpyDataReader() *spyDataReader {
	return &spyDataReader{}
}

func (s *spyDataReader) Read(
	ctx context.Context,
	sourceID string,
	start time.Time,
	opts ...logcache.ReadOption,
) ([]*loggregator_v2.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readSourceIDs = append(s.readSourceIDs, sourceID)
	s.readStarts = append(s.readStarts, start)

	if len(s.readResults) != len(s.readErrs) {
		panic("readResults and readErrs are out of sync")
	}

	if len(s.readResults) == 0 {
		return nil, nil
	}

	r := s.readResults[0]
	err := s.readErrs[0]

	s.readResults = s.readResults[1:]
	s.readErrs = s.readErrs[1:]

	return r, err
}

func (s *spyDataReader) ReadSourceIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]string, len(s.readSourceIDs))
	copy(result, s.readSourceIDs)

	return result
}

func (s *spyDataReader) setRead(es [][]*loggregator_v2.Envelope, errs []error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readResults = es
	s.readErrs = errs
}
