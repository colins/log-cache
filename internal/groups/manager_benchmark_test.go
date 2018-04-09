package groups_test

import (
	"context"
	"io/ioutil"
	"log"
	"testing"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	lc "code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"code.cloudfoundry.org/log-cache/internal/store"
)

var reader = func(
	_ context.Context,
	_ string,
	_ time.Time,
	_ ...logcache.ReadOption,
) ([]*loggregator_v2.Envelope, error) {
	return nil, nil
}

func BenchmarkSetShardGroup(b *testing.B) {
	p := store.NewPruneConsultant(2, 70, lc.NewMemoryAnalyzer(nopMetrics{}))
	s := groups.NewStorage(reader, time.Minute, p, nopMetrics{}, log.New(ioutil.Discard, "", 0))
	m := groups.NewManager(s, time.Minute)

	req := &logcache_v1.SetShardGroupRequest{
		Name: "benchmark",
		SubGroup: &logcache_v1.GroupedSourceIds{
			SourceIds: []string{"a", "b", "c", "d"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.SetShardGroup(context.Background(), req)
	}
}

func BenchmarkRead(b *testing.B) {
	p := store.NewPruneConsultant(2, 70, lc.NewMemoryAnalyzer(nopMetrics{}))
	s := groups.NewStorage(reader, time.Minute, p, nopMetrics{}, log.New(ioutil.Discard, "", 0))
	m := groups.NewManager(s, time.Minute)

	ssgr := &logcache_v1.SetShardGroupRequest{
		Name: "benchmark",
		SubGroup: &logcache_v1.GroupedSourceIds{
			SourceIds: []string{"a", "b", "c", "d"},
		},
	}
	m.SetShardGroup(context.Background(), ssgr)

	sgr := &logcache_v1.ShardGroupReadRequest{
		Name:        "benchmark",
		RequesterId: uint64(95),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Read(context.Background(), sgr)
	}
}

func BenchmarkParallelSetShardGroup(b *testing.B) {
	p := store.NewPruneConsultant(2, 70, lc.NewMemoryAnalyzer(nopMetrics{}))
	s := groups.NewStorage(reader, time.Minute, p, nopMetrics{}, log.New(ioutil.Discard, "", 0))
	m := groups.NewManager(s, time.Minute)

	req := &logcache_v1.SetShardGroupRequest{
		Name: "benchmark",
		SubGroup: &logcache_v1.GroupedSourceIds{
			SourceIds: []string{"a", "b", "c", "d"},
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.SetShardGroup(context.Background(), req)
		}
	})
}

func BenchmarkParallelRead(b *testing.B) {
	p := store.NewPruneConsultant(2, 70, lc.NewMemoryAnalyzer(nopMetrics{}))
	s := groups.NewStorage(reader, time.Minute, p, nopMetrics{}, log.New(ioutil.Discard, "", 0))
	m := groups.NewManager(s, time.Minute)

	ssgr := &logcache_v1.SetShardGroupRequest{
		Name: "benchmark",
		SubGroup: &logcache_v1.GroupedSourceIds{
			SourceIds: []string{"a", "b", "c", "d"},
		},
	}
	m.SetShardGroup(context.Background(), ssgr)

	sgr := &logcache_v1.ShardGroupReadRequest{
		Name:        "benchmark",
		RequesterId: uint64(95),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Read(context.Background(), sgr)
		}
	})
}

func BenchmarkParallelReadAndSetShardGroup(b *testing.B) {
	p := store.NewPruneConsultant(2, 70, lc.NewMemoryAnalyzer(nopMetrics{}))
	s := groups.NewStorage(reader, time.Minute, p, nopMetrics{}, log.New(ioutil.Discard, "", 0))
	m := groups.NewManager(s, time.Minute)

	ssgr := &logcache_v1.SetShardGroupRequest{
		Name: "benchmark",
		SubGroup: &logcache_v1.GroupedSourceIds{
			SourceIds: []string{"a", "b", "c", "d"},
		},
	}

	sgr := &logcache_v1.ShardGroupReadRequest{
		Name:        "benchmark",
		RequesterId: uint64(95),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.SetShardGroup(context.Background(), ssgr)
			m.Read(context.Background(), sgr)
		}
	})
}

// nopMetrics are the default metrics.
type nopMetrics struct{}

func (m nopMetrics) NewCounter(name string) func(uint64) {
	return func(uint64) {}
}

func (m nopMetrics) NewGauge(name string) func(float64) {
	return func(float64) {}
}
