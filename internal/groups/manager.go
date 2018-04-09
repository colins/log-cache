package groups

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Manager manages groups. It implements logcache_v1.GroupReader.
type Manager struct {
	mu      sync.RWMutex
	groups  map[string]*Group
	s       DataStorage
	timeout time.Duration
}

// DataStorage is used to store data for a given group.
type DataStorage interface {
	// Get fetches envelopes from the store based on the source ID, start and
	// end time. Start is inclusive while end is not: [start..end).
	Get(
		name string,
		start time.Time,
		end time.Time,
		envelopeTypes []logcache_v1.EnvelopeType,
		limit int,
		descending bool,
		requesterID uint64,
	) []*loggregator_v2.Envelope

	// Add starts fetching data for the given sourceID group.
	Add(name string, sourceIDs []string)

	// AddRequester adds a requester ID for a given group.
	AddRequester(name string, requesterID uint64, remoteOnly bool)

	// Remove stops fetching data for the given sourceID group.
	Remove(name string, sourceIDs []string)

	// RemoveRequester removes a requester ID for a given group.
	RemoveRequester(name string, requesterID uint64)
}

// NewManager creates a new Manager to manage groups.
func NewManager(s DataStorage, timeout time.Duration) *Manager {
	return &Manager{
		groups:  make(map[string]*Group),
		s:       s,
		timeout: timeout,
	}
}

// SetShardGroup creates the given group if it does not exist or adds the
// sourceID if it does. The source ID will expire after a configurable amount
// of time. Therefore, the source ID should be constantly added. It is a NOP
// to add a source ID to a group if the source ID already exists.
func (m *Manager) SetShardGroup(ctx context.Context, r *logcache_v1.SetShardGroupRequest, _ ...grpc.CallOption) (*logcache_v1.SetShardGroupResponse, error) {
	if r.GetName() == "" || len(r.GetSubGroup().GetSourceIds()) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "name and source_id fields are required")
	}

	if len(r.GetName()) > 128 {
		return nil, grpc.Errorf(codes.InvalidArgument, "name and source_ids fields can only be 128 bytes long and must not be empty")
	}

	for _, sourceID := range r.GetSubGroup().GetSourceIds() {
		if len(sourceID) > 128 || sourceID == "" {
			return nil, grpc.Errorf(codes.InvalidArgument, "name and source_ids fields can only be 128 bytes long and must not be empty")
		}
	}

	// Ensure that sourceID is not already tracked.
	sourceIDs := r.GetSubGroup().GetSourceIds()
	sort.Strings(sourceIDs)
	allSourceIDs := strings.Join(sourceIDs, ",")

	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok := m.groups[r.GetName()]
	if !ok {
		g = NewGroup(r.GetName(), nil, m.timeout)
	}

	sg, ok := g.subGroups[allSourceIDs]
	if !ok {
		sg = &SubGroup{
			sourceIDs: sourceIDs,
			t: time.AfterFunc(m.timeout, func() {
				m.removeFromGroup(r.GetName(), allSourceIDs, sourceIDs)
			}),
		}
		g.Set(sg)

		m.groups[r.GetName()] = g
		m.s.Add(r.GetName(), sourceIDs)
	}

	m.resetExpire(sg.t)
	return &logcache_v1.SetShardGroupResponse{}, nil
}

func (m *Manager) removeFromGroup(name, allSourceIDs string, sourceIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok := m.groups[name]
	if !ok {
		return
	}

	if _, ok := g.subGroups[allSourceIDs]; ok {
		delete(g.subGroups, allSourceIDs)
		m.s.Remove(name, sourceIDs)
	}
	m.groups[name] = g

	if len(g.SourceIDs()) == 0 {
		delete(m.groups, name)
	}
}

// Read reads from a group. As a side effect, this first prunes any expired
// requesters for the group. This is to ensure that the current read will read
// from the most sourceIDs necessary.
func (m *Manager) Read(ctx context.Context, r *logcache_v1.ShardGroupReadRequest, _ ...grpc.CallOption) (*logcache_v1.ShardGroupReadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gi, ok := m.groups[r.Name]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "unknown group name: %s", r.GetName())
	}

	if _, ok := gi.requesterIDs[r.RequesterId]; !ok {
		// Negative limit implies that we are only pinging the requester ID
		// and don't want any data.
		m.s.AddRequester(r.Name, r.RequesterId, r.GetLimit() < 0)
	}
	gi.requesterIDs[r.RequesterId] = time.Now()

	// Check for expired requesters
	for k, v := range m.groups[r.Name].requesterIDs {
		if time.Since(v) >= m.timeout {
			delete(m.groups[r.Name].requesterIDs, k)
			m.s.RemoveRequester(r.Name, k)
		}
	}

	if r.GetEndTime() == 0 {
		r.EndTime = time.Now().UnixNano()
	}

	if r.GetLimit() < 0 {
		// Negative limit implies that we are only pinging the requester ID
		// and don't want any data.
		return &logcache_v1.ShardGroupReadResponse{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: nil,
			},
		}, nil
	}

	if r.GetLimit() == 0 {
		r.Limit = 100
	}
	batch := m.s.Get(
		r.GetName(),
		time.Unix(0, r.GetStartTime()),
		time.Unix(0, r.GetEndTime()),
		r.GetEnvelopeTypes(),
		int(r.GetLimit()),
		false,
		r.RequesterId,
	)

	return &logcache_v1.ShardGroupReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	}, nil
}

// ShardGroup returns information about the given group. If the group does not
// exist, the returned sourceID slice will be empty, but an error will not be
// returned.
func (m *Manager) ShardGroup(ctx context.Context, r *logcache_v1.ShardGroupRequest, _ ...grpc.CallOption) (*logcache_v1.ShardGroupResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.groups[r.Name]
	if !ok {
		return &logcache_v1.ShardGroupResponse{}, nil
	}

	var reqIds []uint64
	for k := range g.requesterIDs {
		reqIds = append(reqIds, k)
	}

	var subGroups []*logcache_v1.GroupedSourceIds
	for _, sg := range g.subGroups {
		subGroups = append(subGroups, &logcache_v1.GroupedSourceIds{
			SourceIds: sg.sourceIDs,
		})
	}

	return &logcache_v1.ShardGroupResponse{
		SubGroups:    subGroups,
		RequesterIds: reqIds,
	}, nil
}

// ListGroups returns all the group names.
func (m *Manager) ListGroups() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []string
	for name := range m.groups {
		results = append(results, name)
	}

	return results
}

func (m *Manager) resetExpire(t *time.Timer) {
	// cancel the timer
	if !t.Stop() && len(t.C) != 0 {
		<-t.C
	}
	// then reset it
	t.Reset(m.timeout)
}
