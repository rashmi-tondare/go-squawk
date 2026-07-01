package squawk

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
)

type signaler struct {
	logger       otellog.Logger
	snapshots    metric.Int64Counter
	bytesWritten metric.Int64Histogram
	duration     metric.Float64Histogram
	dropped      metric.Int64Counter
	attrs        []attribute.KeyValue
}

func newSignaler(mp metric.MeterProvider, lp otellog.LoggerProvider, attrs []attribute.KeyValue) (*signaler, error) {
	m := mp.Meter("squawk", metric.WithInstrumentationVersion("0.1.0"))

	snapshots, err := m.Int64Counter("squawk.snapshots",
		metric.WithDescription("Number of flight-recorder snapshots attempted"))
	if err != nil {
		return nil, err
	}
	bytesWritten, err := m.Int64Histogram("squawk.bytes_written",
		metric.WithDescription("Size of each snapshot in bytes"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	dur, err := m.Float64Histogram("squawk.snapshot.duration",
		metric.WithDescription("Wall time to capture and persist a snapshot"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	dropped, err := m.Int64Counter("squawk.dropped",
		metric.WithDescription("Snapshots suppressed by the rate limiter; suppression is never silent."))
	if err != nil {
		return nil, err
	}

	return &signaler{
		logger:       lp.Logger("squawk"),
		snapshots:    snapshots,
		bytesWritten: bytesWritten,
		duration:     dur,
		dropped:      dropped,
		attrs:        attrs,
	}, nil
}

func (s *signaler) recordSnapshot(ctx context.Context, reason, outcome string, n int64, elapsed time.Duration) {
	set := metric.WithAttributes(append(s.attrs,
		attribute.String("reason", reason),
		attribute.String("outcome", outcome),
	)...)
	s.snapshots.Add(ctx, 1, set)
	s.bytesWritten.Record(ctx, n, set)
	s.duration.Record(ctx, elapsed.Seconds(), set)
}

func (s *signaler) recordDropped(ctx context.Context, reason string) {
	s.dropped.Add(ctx, 1, metric.WithAttributes(append(s.attrs,
		attribute.String("reason", reason),
	)...))
}

// emitAlert fires a WARN log record so backends can alert on this event.
// It fires on both success and storage_error so monitoring never misses a snapshot attempt.
func (s *signaler) emitAlert(ctx context.Context, reason, uri string, n int64) {
	var rec otellog.Record
	rec.SetSeverity(otellog.SeverityWarn)
	rec.SetSeverityText("WARN")
	rec.SetBody(otellog.StringValue("squawk: flight-recorder snapshot captured"))

	logAttrs := []otellog.KeyValue{
		otellog.String("squawk.reason", reason),
		otellog.String("squawk.uri", uri),
		otellog.Int64("squawk.bytes", n),
	}
	for _, kv := range s.attrs {
		logAttrs = append(logAttrs, otellog.String(string(kv.Key), kv.Value.AsString()))
	}
	rec.AddAttributes(logAttrs...)
	s.logger.Emit(ctx, rec)
}
