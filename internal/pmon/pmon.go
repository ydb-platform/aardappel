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
}

type PromMon struct {
	reg                *prometheus.Registry
	modificationsCount prometheus.Counter
	commitLatency      prometheus.Histogram
	Stop               func()
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
	}
	reg.MustRegister(m.modificationsCount)
	reg.MustRegister(m.commitLatency)
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
		if len(config.Key) != 0 && len(config.Cert) != 0 {
			err := server.ListenAndServeTLS(config.Cert, config.Key)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				xlog.Fatal(ctx, "Unable to shutdown https mon", zap.Error(err))
			}
		} else {
			err := server.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				xlog.Fatal(ctx, "Unable to shutdown http mon", zap.Error(err))
			}
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
