package pmon

import (
	"aardappel/internal/config"
	"aardappel/internal/util/misc"
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Metrics interface {
	ModificationCount(c int)
	ModificationCountFromTopic(c int, tag string)
	CommitDuration(s float64)
	RequestSize(c int)
	QuorumWaitingDuration(s float64)
	HeapAllocated(b uint64)
	ReplicationLagEst(s float32)
	TopicWithoutHB(noHb bool, tag string)
}

type HealthApi interface {
	SetCompleted()
}

type MonPerTable struct {
	modificationsCountByTopic prometheus.Counter
	topicWithoutHb            prometheus.Gauge
}

func CreatePerTableCounters(tag string, reg *prometheus.Registry, feedID string) *MonPerTable {
	m := &MonPerTable{
		modificationsCountByTopic: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "modifications_count_per_table",
			Help:        "Total count of modifications per table on the destination cluster.",
			ConstLabels: prometheus.Labels{"feed_id": feedID, "stream_tag": tag},
		}),

		topicWithoutHb: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "topic_without_hb",
			Help:        "topic where hb was not recieved during interval",
			ConstLabels: prometheus.Labels{"feed_id": feedID, "stream_tag": tag},
		}),
	}

	reg.MustRegister(m.topicWithoutHb)
	reg.MustRegister(m.modificationsCountByTopic)

	return m
}

type PromServer struct {
	reg  *prometheus.Registry
	khz  khzHandler
	Stop func()
}

type PromMon struct {
	*PromServer
	feedID               string
	modificationsCount   prometheus.Counter
	commitLatency        prometheus.Histogram
	requestSize          prometheus.Counter
	quorumWaitingLatency prometheus.Histogram
	heapAllocated        prometheus.Gauge
	replicationLagEst    prometheus.Gauge
	perTableCounters     map[string]*MonPerTable
	lock                 sync.Mutex
}

func lazyMetrics(mon *PromMon, tag string) *MonPerTable {
	mon.lock.Lock()
	defer mon.lock.Unlock()

	counter, ok := mon.perTableCounters[tag]
	if ok {
		return counter
	} else {
		tmp := CreatePerTableCounters(tag, mon.reg, mon.feedID)
		mon.perTableCounters[tag] = tmp
		return tmp
	}
}

func NewMetrics(reg *prometheus.Registry, feedID string) *PromMon {
	m := &PromMon{
		PromServer:       &PromServer{reg: reg},
		perTableCounters: make(map[string]*MonPerTable),
		feedID:           feedID,
		modificationsCount: prometheus.NewCounter(prometheus.CounterOpts{
			ConstLabels: prometheus.Labels{"feed_id": feedID},
			Name:        "modifications_count",
			Help:        "Total count of modifications on the destination cluster.",
		}),
		commitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			ConstLabels: prometheus.Labels{"feed_id": feedID},
			Name:        "commit_latency",
			Help:        "Latency of commit changes on the destination cluster (seconds).",
			Buckets:     prometheus.DefBuckets,
		}),
		requestSize: prometheus.NewCounter(prometheus.CounterOpts{
			ConstLabels: prometheus.Labels{"feed_id": feedID},
			Name:        "request_size_bytes",
			Help:        "Size of request on the destination cluster (bytes).",
		}),
		quorumWaitingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			ConstLabels: prometheus.Labels{"feed_id": feedID},
			Name:        "quorum_waiting_latency",
			Help:        "Latency of waiting quorum changes on the destination cluster (seconds).",
			Buckets:     prometheus.DefBuckets,
		}),
		heapAllocated: prometheus.NewGauge(prometheus.GaugeOpts{
			ConstLabels: prometheus.Labels{"feed_id": feedID},
			Name:        "go_heap_allocated",
			Help:        "Size is bytes of allocated heap objects",
		}),
		replicationLagEst: prometheus.NewGauge(prometheus.GaugeOpts{
			ConstLabels: prometheus.Labels{"feed_id": feedID},
			Name:        "replication_lag_estimation",
			Help:        "The time difference between coordination time of quorum hb and local time",
		}),
	}
	reg.MustRegister(m.modificationsCount)
	reg.MustRegister(m.commitLatency)
	reg.MustRegister(m.requestSize)
	reg.MustRegister(m.quorumWaitingLatency)
	reg.MustRegister(m.heapAllocated)
	reg.MustRegister(m.replicationLagEst)
	return m
}

type khzHandler struct {
	ready atomic.Bool
}

func (p *PromServer) SetCompleted() {
	p.khz.ready.Store(true)
}

func (h *khzHandler) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	if h.ready.Load() {
		writer.WriteHeader(200)
	} else {
		writer.WriteHeader(503)
	}
}

// NewFeed registers metrics for the feed selected by this worker.
func (p *PromServer) NewFeed(feedID string) *PromMon {
	m := NewMetrics(p.reg, feedID)
	m.PromServer = p
	return m
}

func NewPromServer(ctx context.Context, config *config.MonServer) *PromServer {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(
				collectors.MetricsAll,
			),
		),
	)

	p := &PromServer{reg: reg}
	mux := http.NewServeMux()

	server := &http.Server{
		Addr:    config.Listen,
		Handler: mux,
	}

	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	mux.Handle("/readyz", &p.khz)

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			xlog.Fatal(ctx, "Monitoring HTTP server failed", zap.String("listen_address", config.Listen), zap.Error(err))
		}
		xlog.Info(ctx, "Monitoring HTTP server stopped")
	}()

	p.Stop = func() {
		shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownRelease()
		if err := server.Shutdown(shutdownCtx); err != nil {
			xlog.Fatal(ctx, "Unable to shut down monitoring HTTP server", zap.String("listen_address", config.Listen), zap.Error(err))
		}
	}

	return p
}

func (p *PromMon) ModificationCount(c int) {
	p.modificationsCount.Add(float64(c))
}

func (p *PromMon) CommitDuration(s float64) {
	p.commitLatency.Observe(s)
}

func (p *PromMon) RequestSize(c int) {
	p.requestSize.Add(float64(c))
}

func (p *PromMon) QuorumWaitingDuration(s float64) {
	p.quorumWaitingLatency.Observe(s)
}

func (p *PromMon) HeapAllocated(b uint64) {
	p.heapAllocated.Set(float64(b))
}

func (p *PromMon) ReplicationLagEst(s float32) {
	p.replicationLagEst.Set(float64(s))
}

func (p *PromMon) ModificationCountFromTopic(c int, tag string) {
	lazyMetrics(p, tag).modificationsCountByTopic.Add(float64(c))
}

func (p *PromMon) TopicWithoutHB(c bool, tag string) {
	val := misc.TernaryIf(c, 1.0, 0.0)
	lazyMetrics(p, tag).topicWithoutHb.Set(val)
}
