package groups_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/log-cache/internal/groups"
)

var _ = Describe("Group", func() {
	It("initializes a group", func() {
		g := createGroup("my-group", []string{"a", "b", "c"}, time.Minute)

		Expect(g.Name()).To(Equal("my-group"))
		Expect(g.SourceIDs()).To(Equal([]string{"a", "b", "c"}))
	})

	It("adds source IDs to the group", func() {
		g := createGroup("", []string{"a", "b", "c"}, time.Minute)
		Expect(g.SourceIDs()).To(ConsistOf("a", "b", "c"))

		g.Set(groups.NewSubGroup("d", "e", "f"))
		Expect(g.SourceIDs()).To(ConsistOf("a", "b", "c", "d", "e", "f"))
	})

	It("adds a requester ID", func() {
		g := createGroup("", []string{}, time.Minute)
		g.AddRequesterID(1)

		Expect(g.RequesterIDs()).To(Equal([]uint64{1}))
	})

	It("does not duplicate requester IDs", func() {
		g := createGroup("", []string{}, time.Minute)
		g.AddRequesterID(1)
		g.AddRequesterID(1)

		Expect(g.RequesterIDs()).To(Equal([]uint64{1}))
	})

	It("adds multiple requester IDs", func() {
		g := createGroup("", []string{}, time.Minute)
		g.AddRequesterID(1)
		g.AddRequesterID(2)

		Expect(g.RequesterIDs()).To(ConsistOf(uint64(1), uint64(2)))
	})

	It("is thread safe", func() {
		g := createGroup("", []string{}, 50*time.Millisecond)

		go func() {
			for i := 0; i < 1000; i++ {
				g.Set(groups.NewSubGroup("d"))
				g.AddRequesterID(1)

				time.Sleep(2 * time.Millisecond)
			}
		}()

		for i := 0; i < 1000; i++ {
			_ = g.SourceIDs()
			_ = g.RequesterIDs()
		}
	})
})

func createGroup(name string, sourceIDs []string, ttl time.Duration) *groups.Group {
	return groups.NewGroup(name, groups.NewSubGroup(sourceIDs...), ttl)
}
