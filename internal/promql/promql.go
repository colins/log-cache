package promql

import (
	"context"
	"log"
	"sort"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type PromQL struct {
	r           DataReader
	log         *log.Logger
	maxFailures int
	failures    int

	result int64
}

type DataReader interface {
	Read(
		ctx context.Context,
		sourceID string,
		start time.Time,
		opts ...logcache.ReadOption,
	) ([]*loggregator_v2.Envelope, error)
}

func New(
	maxFailures int,
	r DataReader,
	log *log.Logger,
) *PromQL {
	q := &PromQL{
		r:           r,
		log:         log,
		result:      1,
		maxFailures: maxFailures,
	}

	return q
}

func (q *PromQL) InstantQuery(query string) (*logcache_v1.PromQL_QueryResult, error) {
	interval := time.Second
	queryable := promql.NewEngine(&logCacheQueryable{
		log:        q.log,
		interval:   interval,
		dataReader: q.r,
	}, nil)

	qq, err := queryable.NewInstantQuery(query, time.Now())
	if err != nil {
		return nil, err
	}

	r := qq.Exec(context.Background())

	return q.toQueryResult(r)
}

func (q *PromQL) toQueryResult(r *promql.Result) (*logcache_v1.PromQL_QueryResult, error) {
	switch r.Value.Type() {
	case promql.ValueTypeScalar:

		s := r.Value.(promql.Scalar)
		return &logcache_v1.PromQL_QueryResult{
			Result: &logcache_v1.PromQL_QueryResult_Scalar{
				Scalar: &logcache_v1.PromQL_Scalar{
					Time:  s.T * int64(time.Millisecond),
					Value: s.V,
				},
			},
		}, nil

	case promql.ValueTypeVector:
		var samples []*logcache_v1.PromQL_Sample
		for _, s := range r.Value.(promql.Vector) {
			metric := make(map[string]string)
			for _, m := range s.Metric {
				metric[m.Name] = m.Value
			}
			samples = append(samples, &logcache_v1.PromQL_Sample{
				Metric: metric,
				Point: &logcache_v1.PromQL_Point{
					Time:  s.T * int64(time.Millisecond),
					Value: s.V,
				},
			})
		}

		return &logcache_v1.PromQL_QueryResult{
			Result: &logcache_v1.PromQL_QueryResult_Vector{
				Vector: &logcache_v1.PromQL_Vector{
					Samples: samples,
				},
			},
		}, nil
	case promql.ValueTypeMatrix:
		var series []*logcache_v1.PromQL_Series
		for _, s := range r.Value.(promql.Matrix) {
			metric := make(map[string]string)
			for _, m := range s.Metric {
				metric[m.Name] = m.Value
			}
			var points []*logcache_v1.PromQL_Point
			for _, p := range s.Points {
				points = append(points, &logcache_v1.PromQL_Point{
					Time:  p.T * int64(time.Millisecond),
					Value: p.V,
				})
			}

			series = append(series, &logcache_v1.PromQL_Series{
				Metric: metric,
				Points: points,
			})
		}

		return &logcache_v1.PromQL_QueryResult{
			Result: &logcache_v1.PromQL_QueryResult_Matrix{
				Matrix: &logcache_v1.PromQL_Matrix{
					Series: series,
				},
			},
		}, nil
	default:
		q.log.Panicf("unknown type: %s", r.Value.Type())
		return nil, nil
	}
}

// func (e *PromQL) start() {
// 	interval := time.Second
// 	queryable := promql.NewEngine(&logCacheQueryable{
// 		log:        e.log,
// 		interval:   interval,
// 		dataReader: e.r,
// 	}, nil)

// 	for range e.ticker {
// 		q, err := queryable.NewInstantQuery(e.query, time.Now())
// 		if err != nil {
// 			log.Fatalf("Invalid query: %s", err)
// 		}

// 		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
// 		result := q.Exec(ctx)

// 		if result.Err != nil {
// 			e.log.Printf("promQL error: %s", result.Err)
// 			atomic.StoreInt64(&e.result, 0)
// 			continue
// 		}

// 		if len(result.String()) == 0 {
// 			e.failures++
// 			if e.failures >= e.maxFailures {
// 				atomic.StoreInt64(&e.result, 0)
// 				return
// 			}

// 			continue
// 		}

// 		e.failures = 0
// 		atomic.StoreInt64(&e.result, 1)
// 	}
// }

type logCacheQueryable struct {
	log        *log.Logger
	interval   time.Duration
	dataReader DataReader
}

func (l *logCacheQueryable) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	return &LogCacheQuerier{
		log:        l.log,
		ctx:        ctx,
		start:      time.Unix(0, mint*int64(time.Millisecond)),
		end:        time.Unix(0, maxt*int64(time.Millisecond)),
		interval:   l.interval,
		dataReader: l.dataReader,
	}, nil
}

type LogCacheQuerier struct {
	log        *log.Logger
	ctx        context.Context
	start      time.Time
	end        time.Time
	interval   time.Duration
	dataReader DataReader
}

func (l *LogCacheQuerier) Select(ll ...*labels.Matcher) (storage.SeriesSet, error) {
	var (
		sourceID string
		metric   string
		ls       []labels.Label
	)
	for _, l := range ll {
		ls = append(ls, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
		if l.Name == "__name__" {
			metric = l.Value
			continue
		}
		if l.Name == "source_id" {
			sourceID = l.Value
			continue
		}
	}

	if sourceID == "" {
		l.log.Fatalf("Metric '%s' does not have a 'source_id' label.", metric)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	envelopes, err := l.dataReader.Read(ctx, sourceID, l.start, logcache.WithEndTime(l.end))
	if err != nil {
		l.log.Printf("failed to read envelopes: %s", err)
		return nil, err
	}

	builder := newSeriesBuilder()
	for _, e := range envelopes {
		if e.GetCounter().GetName() != metric &&
			e.GetTimer().GetName() != metric &&
			e.GetGauge().GetMetrics()[metric] == nil {
			continue
		}

		if !l.hasLabels(e.GetTags(), ls) {
			continue
		}

		e.Timestamp = time.Unix(0, e.GetTimestamp()).Truncate(l.interval).UnixNano()

		var f float64
		switch e.Message.(type) {
		case *loggregator_v2.Envelope_Counter:
			f = float64(e.GetCounter().GetTotal())
		case *loggregator_v2.Envelope_Gauge:
			f = e.GetGauge().GetMetrics()[metric].GetValue()
		case *loggregator_v2.Envelope_Timer:
			timer := e.GetTimer()
			f = float64(timer.GetStop() - timer.GetStart())
		}

		builder.add(e.Tags, sample{
			t: e.GetTimestamp() / int64(time.Millisecond),
			v: f,
		})
	}

	return builder.buildSeriesSet(), nil
}

func convertToLabels(tags map[string]string) []labels.Label {
	ls := make([]labels.Label, 0, len(tags))
	for n, v := range tags {
		ls = append(ls, labels.Label{
			Name:  n,
			Value: v,
		})
	}
	return ls
}

func (l *LogCacheQuerier) hasLabels(tags map[string]string, ls []labels.Label) bool {
	for _, l := range ls {
		if l.Name == "__name__" || l.Name == "source_id" {
			continue
		}

		if v, ok := tags[l.Name]; !ok || v != l.Value {
			return false
		}
	}

	return true
}

func (l *LogCacheQuerier) LabelValues(name string) ([]string, error) {
	panic("not implemented")
}

func (l *LogCacheQuerier) Close() error {
	return nil
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []storage.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

// concreteSeries implementes storage.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []sample
}

type sample struct {
	t int64
	v float64
}

func (c *concreteSeries) Labels() labels.Labels {
	return labels.New(c.labels...)
}

func (c *concreteSeries) Iterator() storage.SeriesIterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements storage.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) storage.SeriesIterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].t >= t
	})
	return c.cur < len(c.series.samples)
}

// At implements storage.SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]

	return s.t, s.v
}

// Next implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}

type seriesData struct {
	tags    map[string]string
	samples []sample
}

func newSeriesBuilder() *seriesSetBuilder {
	return &seriesSetBuilder{
		data: make(map[string]seriesData),
	}
}

type seriesSetBuilder struct {
	data map[string]seriesData
}

func (b *seriesSetBuilder) add(tags map[string]string, s sample) {
	seriesID := b.getSeriesID(tags)
	d, ok := b.data[seriesID]

	if !ok {
		b.data[seriesID] = seriesData{
			tags:    tags,
			samples: make([]sample, 0),
		}

		d = b.data[seriesID]
	}

	d.samples = append(d.samples, s)
	b.data[seriesID] = d
}

func (b *seriesSetBuilder) getSeriesID(tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var seriesID string
	for _, k := range keys {
		seriesID = seriesID + "-" + k + "-" + tags[k]
	}

	return seriesID
}

func (b *seriesSetBuilder) buildSeriesSet() storage.SeriesSet {
	set := &concreteSeriesSet{
		series: []storage.Series{},
	}

	for _, v := range b.data {
		set.series = append(set.series, &concreteSeries{
			labels:  convertToLabels(v.tags),
			samples: v.samples,
		})
	}

	return set
}
