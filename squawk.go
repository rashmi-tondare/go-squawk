// Package squawk persists trace.FlightRecorder snapshots and emits
// observable signals (OTel metrics + log record) when an anomaly occurs.
//
// This is a Go execution trace library, NOT an OpenTelemetry distributed trace library.
package squawk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/trace"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// Ref identifies a persisted snapshot
type Ref struct {
	URI string // provider-specific URI (e.g. s3://bucket/key, file:///path)
	Key string // object key within the storage backend
}

// Extractor is an optional async consumer of snapshot bytes (Phase 2)
type Extractor interface {
	Extract(ctx context.Context, r io.Reader) error
}

// Recorder wraps a FlightRecorder, adding persistence, rate-limiting, and observability
type Recorder struct {
	fr        *trace.FlightRecorder
	storage   Storage
	signal    *signaler
	limiter   *rateLimiter
	extractor Extractor
	bufPool   sync.Pool
}

// Option configures a Recorder
type Option func(*config)

type config struct {
	storage        Storage
	meterProvider  metric.MeterProvider
	loggerProvider otellog.LoggerProvider
	minInterval    time.Duration
	burst          int
	attrs          []attribute.KeyValue
	extractor      Extractor
}

// WithStorage sets the persistence backend
func WithStorage(s Storage) Option {
	return func(c *config) { c.storage = s }
}

// WithMeterProvider sets the OTel meter provider
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) { c.meterProvider = mp }
}

// WithLoggerProvider sets the OTel logger provider
func WithLoggerProvider(lp otellog.LoggerProvider) Option {
	return func(c *config) { c.loggerProvider = lp }
}

// WithRateLimit configures a token-bucket rate limiter
func WithRateLimit(min time.Duration, burst int) Option {
	return func(c *config) { c.minInterval = min; c.burst = burst }
}

// WithResourceAttrs attaches resource attributes (e.g. service.name, host.name) to every metric data point and log record emitted by this recorder
func WithResourceAttrs(kvs ...attribute.KeyValue) Option {
	return func(c *config) { c.attrs = append(c.attrs, kvs...) }
}

// WithExtractor registers an optional async trace-byte consumer (Phase 2)
func WithExtractor(e Extractor) Option {
	return func(c *config) { c.extractor = e }
}

// New creates a Recorder
func New(fr *trace.FlightRecorder, opts ...Option) (*Recorder, error) {
	cfg := &config{
		minInterval: time.Second,
		burst:       1,
	}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.storage == nil {
		return nil, fmt.Errorf("squawk: WithStorage is required")
	}

	mp := cfg.meterProvider
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}
	lp := cfg.loggerProvider
	if lp == nil {
		lp = noop.NewLoggerProvider()
	}

	sig, err := newSignaler(mp, lp, cfg.attrs)
	if err != nil {
		return nil, fmt.Errorf("squawk: init metrics: %w", err)
	}

	return &Recorder{
		fr:        fr,
		storage:   cfg.storage,
		signal:    sig,
		limiter:   newRateLimiter(cfg.minInterval, cfg.burst),
		extractor: cfg.extractor,
		bufPool:   sync.Pool{New: func() any { return new(bytes.Buffer) }},
	}, nil
}

// Snapshot captures the current flight-recorder window, persists it via the configured
// Storage, and emits OTel metrics + a WARN log record. Rate-limited snapshots return a
// zero Ref and no error; the squawk.dropped counter is incremented instead.
//
// Snapshot performs blocking I/O (storage writes) on the calling goroutine; callers on a
// latency-sensitive path should invoke it from their own goroutine.
func (r *Recorder) Snapshot(ctx context.Context, reason string) (Ref, error) {
	if !r.limiter.Allow() {
		r.signal.recordDropped(ctx, reason)
		return Ref{}, nil
	}

	start := time.Now()

	buf := r.bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	n, err := r.fr.WriteTo(buf)
	if err != nil {
		r.bufPool.Put(buf)
		r.signal.recordSnapshot(ctx, reason, "write_error", n, time.Since(start))
		return Ref{}, fmt.Errorf("squawk: WriteTo: %w", err)
	}

	// data is valid for the lifetime of buf; pass via bytes.Reader so Put can read it while buf remains usable for the pool
	data := buf.Bytes()
	key := buildKey(reason, r.signal.attrs, start)

	ref, storErr := r.storage.Put(ctx, key, bytes.NewReader(data), n)

	// always emit alert so monitoring fires even on storage errors
	uri := ""
	if storErr == nil {
		uri = ref.URI
	}
	r.signal.emitAlert(ctx, reason, uri, n)

	if r.extractor != nil {
		extData := append([]byte(nil), data...) // independent copy for async goroutine
		ext := r.extractor
		go func() { _ = ext.Extract(context.WithoutCancel(ctx), bytes.NewReader(extData)) }()
	}

	buf.Reset()
	r.bufPool.Put(buf)

	if storErr != nil {
		r.signal.recordSnapshot(ctx, reason, "storage_error", n, time.Since(start))
		return Ref{}, fmt.Errorf("squawk: Put: %w", storErr)
	}

	r.signal.recordSnapshot(ctx, reason, "ok", n, time.Since(start))
	return ref, nil
}

// buildKey returns traces/{service}/{YYYY-MM-DD}/{unixnano}-{reason}.trace
func buildKey(reason string, attrs []attribute.KeyValue, t time.Time) string {
	service := "unknown"
	for _, kv := range attrs {
		if string(kv.Key) == "service.name" {
			service = kv.Value.AsString()
			break
		}
	}
	return fmt.Sprintf("traces/%s/%s/%d-%s.trace",
		service,
		t.UTC().Format(time.DateOnly),
		t.UnixNano(),
		sanitizeReason(reason),
	)
}

func sanitizeReason(r string) string {
	out := make([]byte, 0, len(r))
	for i := 0; i < len(r); i++ {
		c := r[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '-')
		}
	}
	return string(out)
}
