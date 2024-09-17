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
}

type PromMon struct {
	reg                  *prometheus.Registry
	modificationsCount   prometheus.Counter
	commitLatency        prometheus.Histogram
	requestSize          prometheus.Counter
	quorumWaitingLatency prometheus.Histogram
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
	}
	reg.MustRegister(m.modificationsCount)
	reg.MustRegister(m.commitLatency)
	reg.MustRegister(m.requestSize)
	reg.MustRegister(m.quorumWaitingLatency)
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
