package groups

import (
	"strings"
	"sync"
	"time"
)

type Group struct {
	smu       sync.RWMutex
	subGroups map[string]*SubGroup

	rmu          sync.RWMutex
	requesterIDs map[uint64]time.Time

	name string
}

func NewGroup(name string, subGroup *SubGroup, ttl time.Duration) *Group {
	g := &Group{
		name:         name,
		subGroups:    make(map[string]*SubGroup),
		requesterIDs: make(map[uint64]time.Time),
	}
	if subGroup != nil {
		g.Set(subGroup)
	}

	return g
}

func (g *Group) Name() string {
	return g.name
}

func (g *Group) Set(sg *SubGroup) {
	groupID := strings.Join(sg.sourceIDs, ",")

	g.smu.Lock()
	defer g.smu.Unlock()
	g.subGroups[groupID] = sg
}

func (g *Group) SourceIDs() []string {
	var result []string
	g.smu.RLock()
	defer g.smu.RUnlock()
	for _, s := range g.subGroups {
		result = append(result, s.sourceIDs...)
	}
	return result
}

func (g *Group) AddRequesterID(id uint64) {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	g.requesterIDs[id] = time.Now()
}

func (g *Group) RequesterIDs() []uint64 {
	g.rmu.RLock()
	defer g.rmu.RUnlock()
	var values []uint64
	for k, _ := range g.requesterIDs {
		values = append(values, k)
	}
	return values
}

type SubGroup struct {
	sourceIDs []string
	t         *time.Timer
}

func NewSubGroup(sourceIDs ...string) *SubGroup {
	return &SubGroup{
		sourceIDs: sourceIDs,
	}
}
