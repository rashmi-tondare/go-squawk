// Command basic demonstrates go-squawk with local filesystem storage.
// It starts a FlightRecorder, triggers a Snapshot, prints the ref, and
// shows how to open the resulting file with go tool trace.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/trace"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/rashmi-tondare/go-squawk"
)

// consoleLogProvider is a minimal OTel LoggerProvider that prints records to stdout.
// Production code should use go.opentelemetry.io/otel/sdk/log with a real exporter.
type consoleLogProvider struct {
	embedded.LoggerProvider
}

func (p *consoleLogProvider) Logger(name string, _ ...otellog.LoggerOption) otellog.Logger {
	return &consoleLogger{name: name}
}

type consoleLogger struct {
	embedded.Logger
	name string
}

func (l *consoleLogger) Emit(_ context.Context, rec otellog.Record) {
	fmt.Printf("[%s] %s: %s\n", rec.Severity(), l.name, rec.Body().AsString())
	rec.WalkAttributes(func(kv otellog.KeyValue) bool {
		fmt.Printf("  %s=%v\n", kv.Key, fmtLogValue(kv.Value))
		return true
	})
}

func fmtLogValue(v otellog.Value) any {
	switch v.Kind() {
	case otellog.KindString:
		return v.AsString()
	case otellog.KindInt64:
		return v.AsInt64()
	case otellog.KindFloat64:
		return v.AsFloat64()
	case otellog.KindBool:
		return v.AsBool()
	default:
		return v.String()
	}
}

func (l *consoleLogger) Enabled(_ context.Context, _ otellog.EnabledParameters) bool { return true }

func main() {
	ctx := context.Background()

	// -- OTel metric setup (manual reader so we can print at the end) --
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	// -- Local storage --
	dir, err := os.MkdirTemp("", "squawk-example-*")
	if err != nil {
		log.Fatal(err)
	}
	// dir is intentionally not removed so you can run: go tool trace <path>
	fmt.Printf("Storing traces in: %s\n\n", dir)

	// -- Flight recorder --
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{
		MinAge: 2 * time.Second,
	})
	if err := fr.Start(); err != nil {
		log.Fatal(err)
	}
	defer fr.Stop()

	// Let the recorder gather a bit of data before snapshotting.
	time.Sleep(100 * time.Millisecond)
	doSomeWork()

	// -- Squawk recorder --
	rec, err := squawk.New(fr,
		squawk.WithStorage(&squawk.LocalStorage{Dir: dir}),
		squawk.WithServiceName("example-service"),
		squawk.WithMeterProvider(mp),
		squawk.WithLoggerProvider(&consoleLogProvider{}),
		squawk.WithRateLimit(500*time.Millisecond, 3),
		squawk.WithResourceAttrs(
			attribute.String("host.name", "localhost"),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	// First snapshot.
	ref, err := rec.Snapshot(ctx, "high-latency")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nSnapshot 1 saved:\n  key=%s\n  uri=%s\n", ref.Key, ref.URI)
	if len(ref.URI) > len("file://") {
		fmt.Printf("  Inspect: go tool trace %s\n", ref.URI[len("file://"):])
	}

	// Second snapshot after rate-limit refill.
	time.Sleep(600 * time.Millisecond)
	ref2, err := rec.Snapshot(ctx, "oom-pressure")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nSnapshot 2 saved:\n  key=%s\n", ref2.Key)

	// Print collected metrics.
	fmt.Printf("\n--- Metrics ---\n")
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		log.Printf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			fmt.Printf("  %s\n", m.Name)
		}
	}
}

func doSomeWork() {
	ch := make(chan int, 10)
	for i := range 5 {
		ch <- i
	}
	for range 5 {
		<-ch
	}
}
