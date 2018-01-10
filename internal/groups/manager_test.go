package groups_test

import (
	"context"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"code.cloudfoundry.org/log-cache/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Manager", func() {
	var (
		m              *groups.Manager
		spyDataStorage *spyDataStorage
	)

	BeforeEach(func() {
		spyDataStorage = newSpyDataStorage()
		m = groups.NewManager(spyDataStorage, time.Hour)
	})

	It("keeps track of source IDs for groups", func() {
		r, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())
		Expect(spyDataStorage.addNames).To(ContainElement("a"))

		r, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())

		r, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "b",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())
		Expect(spyDataStorage.addNames).To(ContainElement("b"))

		resp, err := m.Group(context.Background(), &logcache.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("1", "2"))

		rr, err := m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())
		Expect(spyDataStorage.removes).To(ConsistOf("1"))
		Expect(spyDataStorage.removeNames).To(ContainElement("a"))

		resp, err = m.Group(context.Background(), &logcache.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.SourceIds).To(ConsistOf("2"))

		rr, err = m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rr).ToNot(BeNil())
	})

	It("keeps track of requester IDs for a group", func() {
		_, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.Read(context.Background(), &logcache.GroupReadRequest{
			Name:           "a",
			RequesterId:    1,
			FilterTemplate: "{{.}}",
		})
		Expect(err).ToNot(HaveOccurred())

		// Do RequestId 1 twice to ensure it is only reported once.
		_, err = m.Read(context.Background(), &logcache.GroupReadRequest{
			Name:           "a",
			RequesterId:    1,
			FilterTemplate: "{{.}}",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.Read(context.Background(), &logcache.GroupReadRequest{
			Name:           "a",
			RequesterId:    2,
			FilterTemplate: "{{.}}",
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := m.Group(context.Background(), &logcache.GroupRequest{
			Name: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.RequesterIds).To(ConsistOf(uint64(1), uint64(2)))

		Expect(spyDataStorage.addReqNames).To(ContainElement("a"))
		Expect(spyDataStorage.addReqIDs).To(ConsistOf(uint64(1), (uint64(2))))

		Expect(spyDataStorage.getRequestIDs).To(ContainElement(uint64(1)))
		Expect(spyDataStorage.getRequestIDs).To(ContainElement(uint64(2)))
	})

	It("expires requester IDs after a given time", func() {
		m = groups.NewManager(spyDataStorage, 10*time.Millisecond)

		_, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.Read(context.Background(), &logcache.GroupReadRequest{
			Name:           "a",
			RequesterId:    1,
			FilterTemplate: "{{.}}",
		})
		Expect(err).ToNot(HaveOccurred())

		f := func() []uint64 {
			_, err = m.Read(context.Background(), &logcache.GroupReadRequest{
				Name:           "a",
				RequesterId:    2,
				FilterTemplate: "{{.}}",
			})
			Expect(err).ToNot(HaveOccurred())

			resp, err := m.Group(context.Background(), &logcache.GroupRequest{
				Name: "a",
			})
			Expect(err).ToNot(HaveOccurred())
			return resp.RequesterIds
		}
		Eventually(f).Should(ConsistOf(uint64(2)))

		Expect(spyDataStorage.removeReqNames).To(ConsistOf("a"))
		Expect(spyDataStorage.removeReqIDs).To(ConsistOf(uint64(1)))
	})

	It("reads from a known group", func() {
		spyDataStorage.getResult = []*loggregator_v2.Envelope{
			{Timestamp: 1},
			{Timestamp: 2},
		}

		_, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "2",
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := m.Read(context.Background(), &logcache.GroupReadRequest{
			Name:           "a",
			FilterTemplate: "{{.}}",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.Envelopes.Batch).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 1},
			&loggregator_v2.Envelope{Timestamp: 2},
		))

		Expect(spyDataStorage.adds).To(ConsistOf("1", "2"))
		Expect(spyDataStorage.getFilterTemplates).To(ConsistOf("{{.}}"))
	})

	It("defaults startTime to 0, endTime to now, envelopeType to nil and limit to 100", func() {
		m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "1",
		})

		m.Read(context.Background(), &logcache.GroupReadRequest{
			Name: "a",
		})

		Expect(spyDataStorage.getStarts).To(ContainElement(int64(0)))
		Expect(spyDataStorage.getEnds).To(ContainElement(BeNumerically("~", time.Now().UnixNano(), 3*time.Second)))
		Expect(spyDataStorage.getLimits).To(ContainElement(100))
		Expect(spyDataStorage.getEnvelopeTypes).To(ContainElement(BeNil()))
	})

	It("returns an error for unknown groups", func() {
		_, err := m.Read(context.Background(), &logcache.GroupReadRequest{
			Name: "unknown-name",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.NotFound))
	})

	It("rejects empty group names and source IDs or either that are too long", func() {
		_, err := m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "",
			SourceId: "1",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     strings.Repeat("x", 129),
			SourceId: "1",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: "",
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
			Name:     "a",
			SourceId: strings.Repeat("x", 129),
		})
		Expect(err).To(HaveOccurred())
		Expect(grpc.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("survives the race detector", func() {
		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(2)
		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.AddToGroup(context.Background(), &logcache.AddToGroupRequest{
					Name:     "a",
					SourceId: "1",
				})
			}
		}(m)

		go func(m *groups.Manager) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.Read(context.Background(), &logcache.GroupReadRequest{
					Name: "a",
				})
			}
		}(m)

		for i := 0; i < 100; i++ {
			m.RemoveFromGroup(context.Background(), &logcache.RemoveFromGroupRequest{
				Name:     "a",
				SourceId: "1",
			})
		}
	})
})

type spyDataStorage struct {
	adds     []string
	addNames []string

	removes     []string
	removeNames []string

	addReqNames []string
	addReqIDs   []uint64

	removeReqNames []string
	removeReqIDs   []uint64

	getNames           []string
	getStarts          []int64
	getEnds            []int64
	getLimits          []int
	getEnvelopeTypes   []store.EnvelopeType
	getFilterTemplates []string
	getResult          []*loggregator_v2.Envelope
	getRequestIDs      []uint64
}

func newSpyDataStorage() *spyDataStorage {
	return &spyDataStorage{}
}

func (s *spyDataStorage) Get(
	name string,
	start time.Time,
	end time.Time,
	envelopeType store.EnvelopeType,
	limit int,
	filterTemplate string,
	requesterID uint64,
) []*loggregator_v2.Envelope {
	s.getNames = append(s.getNames, name)
	s.getStarts = append(s.getStarts, start.UnixNano())
	s.getEnds = append(s.getEnds, end.UnixNano())
	s.getLimits = append(s.getLimits, limit)
	s.getEnvelopeTypes = append(s.getEnvelopeTypes, envelopeType)
	s.getFilterTemplates = append(s.getFilterTemplates, filterTemplate)
	s.getRequestIDs = append(s.getRequestIDs, requesterID)

	return s.getResult
}

func (s *spyDataStorage) Add(name, sourceID string) {
	s.addNames = append(s.addNames, name)
	s.adds = append(s.adds, sourceID)
}

func (s *spyDataStorage) AddRequester(name string, requesterID uint64) {
	s.addReqNames = append(s.addReqNames, name)
	s.addReqIDs = append(s.addReqIDs, requesterID)
}

func (s *spyDataStorage) Remove(name, sourceID string) {
	s.removeNames = append(s.removeNames, name)
	s.removes = append(s.removes, sourceID)
}

func (s *spyDataStorage) RemoveRequester(name string, requesterID uint64) {
	s.removeReqNames = append(s.removeReqNames, name)
	s.removeReqIDs = append(s.removeReqIDs, requesterID)
}
