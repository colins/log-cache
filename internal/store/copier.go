package store

import (
	"log"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"golang.org/x/net/context"
)

// Copier reads data for a given source ID from remote nodes to store in the
// local store.
type Copier struct {
	s   DestinationStore
	l   RemoteLookup
	log *log.Logger
}

type DestinationStore interface {
	LocalStore
	Put(e *loggregator_v2.Envelope, sourceID string)
}

// RemoteLookup takes a sourceID and returns each client that may have
// envelopes for that sourceID.
type RemoteLookup func(sourceID string) []rpc.EgressClient

// NewCopier returns a new Copier.
func NewCopier(s DestinationStore, l RemoteLookup, log *log.Logger) *Copier {
	return &Copier{
		s:   s,
		l:   l,
		log: log,
	}
}

// Get first reads from any remote store that has the requested source ID and
// writes it to the local store. It then reads from the local store.
func (c *Copier) Get(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {

	remotes := c.l(sourceID)
	for _, r := range remotes {
		go func(r rpc.EgressClient) {
			resp, err := r.Read(context.Background(), &rpc.ReadRequest{
				SourceId:     sourceID,
				StartTime:    start.UnixNano(),
				EndTime:      end.UnixNano(),
				EnvelopeType: convertEnvelopeType(envelopeType),
				Limit:        int64(limit),
				Descending:   descending,
			})

			if err != nil {
				c.log.Printf("failed to copy from remote: %s", err)
				return
			}

			for _, e := range resp.Envelopes.Batch {
				c.s.Put(e, e.GetSourceId())
			}
		}(r)
	}

	return c.s.Get(sourceID, start, end, envelopeType, limit, descending)
}

func (c *Copier) Meta() map[string]MetaInfo {
	return c.s.Meta()
}
