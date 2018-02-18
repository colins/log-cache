package ingress

import (
	"log"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// Pubsub stores subscriptions and publishes envelopes based on what the
// lookup function returns.
type Pubsub struct {
	lookup LookUp
	f      WriterFetcher
	log    *log.Logger
}

// NewPubsub creates and returns a new Pubsub.
func NewPubsub(l LookUp, f WriterFetcher, log *log.Logger) *Pubsub {
	return &Pubsub{
		lookup: l,
		f:      f,
		log:    log,
	}
}

// LookUp is used to convert an envelope into the routing address.
type LookUp func(sourceID string) string

// WriterFetcher is used to dial other Log Cache nodes.
type WriterFetcher func(addr string) func(*loggregator_v2.Envelope)

// Publish writes an envelope to any interested subscribers.
func (s *Pubsub) Publish(e *loggregator_v2.Envelope) {
	addr := s.lookup(e.GetSourceId())
	if addr == "" {
		return
	}

	w := s.f(addr)
	w(e)
}
