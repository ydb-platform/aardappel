package pmon

import (
	"aardappel/internal/config"
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type Metrics interface {
	ModificationCount(c int)
	CommitDuration(s float64)
	RequestSize(c int)
	QuorumWaitingDuration(s float64)
	HeapAllocated(b uint64)
	ReplicationLagEst(s float32)
}

type PromMon struct {
	reg                  *prometheus.Registry
	modificationsCount   prometheus.Counter
	commitLatency        prometheus.Histogram
	requestSize          prometheus.Counter
	quorumWaitingLatency prometheus.Histogram
	heapAllocated        prometheus.Gauge
	replicationLagEst    prometheus.Gauge
	Stop                 func()
}

func NewMetrics(reg *prometheus.Registry) *PromMon {
	m := &PromMon{
		modificationsCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "modifications_count",
			Help: "Total count of modifications on the destination cluster.",
		}),
		commitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "commit_latency",
			Help:    "Latency of commit changes on the destination cluster (seconds).",
			Buckets: prometheus.DefBuckets,
		}),
		requestSize: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "request_size_bytes",
			Help: "Size of request on the destination cluster (bytes).",
		}),
		quorumWaitingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "quorum_waiting_latency",
			Help:    "Latency of waiting quorum changes on the destination cluster (seconds).",
			Buckets: prometheus.DefBuckets,
		}),
		heapAllocated: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "go_heap_allocated",
			Help: "Size is bytes of allocated heap objects",
		}),
		replicationLagEst: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "replication_lag_estimation",
			Help: "The time difference between coordination time of quorum hb and local time",
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

func NewPromMon(ctx context.Context, config *config.MonServer) *PromMon {
	reg := prometheus.NewRegistry()

	p := NewMetrics(reg)

	server := &http.Server{
		Addr: config.Listen,
	}

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			xlog.Fatal(ctx, "Unable to shutdown http mon", zap.Error(err))
		}
		xlog.Info(ctx, "mon server stopped")
	}()

	p.Stop = func() {
		shutdownCtx, shutdownRelease := context.WithTimeout(ctx, 10*time.Second)
		defer shutdownRelease()
		if err := server.Shutdown(shutdownCtx); err != nil {
			xlog.Fatal(ctx, "HTTP shutdown error", zap.Error(err))
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
