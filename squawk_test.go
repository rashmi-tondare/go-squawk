package squawk_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/trace"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	squawk "github.com/rashmi-tondare/go-squawk"
)

// ---- in-memory Storage fake ----

type memStorage struct {
	mu   sync.Mutex
	puts []memPut
	err  error // if non-nil, Put returns this error
}

type memPut struct {
	key  string
	data []byte
}

func (m *memStorage) Put(_ context.Context, key string, r io.Reader, _ int64) (squawk.Ref, error) {
	if m.err != nil {
		return squawk.Ref{}, m.err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return squawk.Ref{}, err
	}
	m.mu.Lock()
	m.puts = append(m.puts, memPut{key: key, data: data})
	m.mu.Unlock()
	return squawk.Ref{Key: key, URI: "mem://" + key}, nil
}

func (m *memStorage) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.puts)
}

// ---- in-memory OTel log provider ----

type memLogProvider struct {
	embedded.LoggerProvider
	logger *memLogger
}

func newMemLogProvider() *memLogProvider { return &memLogProvider{logger: &memLogger{}} }

func (p *memLogProvider) Logger(_ string, _ ...otellog.LoggerOption) otellog.Logger {
	return p.logger
}

type memLogger struct {
	embedded.Logger
	mu      sync.Mutex
	records []otellog.Record
}

func (l *memLogger) Emit(_ context.Context, rec otellog.Record) {
	l.mu.Lock()
	l.records = append(l.records, rec)
	l.mu.Unlock()
}

func (l *memLogger) Enabled(_ context.Context, _ otellog.EnabledParameters) bool { return true }

func (l *memLogger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.records)
}

func (l *memLogger) last() (otellog.Record, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.records) == 0 {
		return otellog.Record{}, false
	}
	return l.records[len(l.records)-1], true
}

// ---- helpers ----

func startFR(t *testing.T) *trace.FlightRecorder {
	t.Helper()
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{})
	if err := fr.Start(); err != nil {
		t.Fatalf("FlightRecorder.Start: %v", err)
	}
	t.Cleanup(fr.Stop)
	return fr
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return rm
}

func findCounter(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				if d, ok := m.Data.(metricdata.Sum[int64]); ok && len(d.DataPoints) > 0 {
					var total int64
					for _, dp := range d.DataPoints {
						total += dp.Value
					}
					return total
				}
			}
		}
	}
	return 0
}

// ---- tests ----

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		opts    []squawk.Option
		wantErr bool
	}{
		{
			name:    "no storage returns error",
			wantErr: true,
		},
		{
			name:    "with storage succeeds",
			opts:    []squawk.Option{squawk.WithStorage(&memStorage{})},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fr := startFR(t)
			_, err := squawk.New(fr, tc.opts...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestSnapshot(t *testing.T) {
	tests := []struct {
		name              string
		reason            string
		storageErr        error
		wantErr           bool
		wantKeyContains   []string
		wantKeyPrefix     string
		wantPuts          int
		wantLogs          int
		wantSnapshotCount int64 // squawk.snapshots counter value; fires on both success and storage error
		wantDroppedCount  int64 // squawk.dropped counter; must be 0 for non-rate-limited calls
	}{
		{
			name:              "persists and returns ref",
			reason:            "high-latency",
			wantKeyContains:   []string{"test-svc", "high-latency"},
			wantKeyPrefix:     "traces/test-svc/",
			wantPuts:          1,
			wantLogs:          1,
			wantSnapshotCount: 1,
			wantDroppedCount:  0,
		},
		{
			name:              "sanitizes spaces in reason",
			reason:            "oom pressure",
			wantKeyContains:   []string{"oom-pressure"},
			wantKeyPrefix:     "traces/test-svc/",
			wantPuts:          1,
			wantLogs:          1,
			wantSnapshotCount: 1,
			wantDroppedCount:  0,
		},
		{
			// Storage failures still emit the observable signal so monitoring never misses an attempt.
			name:              "storage error still emits alert and increments snapshot counter",
			reason:            "test",
			storageErr:        bytes.ErrTooLarge,
			wantErr:           true,
			wantPuts:          0,
			wantLogs:          1,
			wantSnapshotCount: 1,
			wantDroppedCount:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fr := startFR(t)
			store := &memStorage{err: tc.storageErr}
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			lp := newMemLogProvider()

			rec, err := squawk.New(fr,
				squawk.WithStorage(store),
				squawk.WithMeterProvider(mp),
				squawk.WithLoggerProvider(lp),
				squawk.WithResourceAttrs(attribute.String("service.name", "test-svc")),
			)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			ref, err := rec.Snapshot(context.Background(), tc.reason)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Snapshot() error = %v, wantErr %v", err, tc.wantErr)
			}

			if !tc.wantErr {
				if ref.Key == "" || ref.URI == "" {
					t.Errorf("got empty Ref: %+v", ref)
				}
				if !strings.HasSuffix(ref.Key, ".trace") {
					t.Errorf("key %q missing .trace suffix", ref.Key)
				}
				if tc.wantKeyPrefix != "" && !strings.HasPrefix(ref.Key, tc.wantKeyPrefix) {
					t.Errorf("key %q missing prefix %q", ref.Key, tc.wantKeyPrefix)
				}
				for _, want := range tc.wantKeyContains {
					if !strings.Contains(ref.Key, want) {
						t.Errorf("key %q missing %q", ref.Key, want)
					}
				}
				if store.count() > 0 && len(store.puts[0].data) == 0 {
					t.Error("expected non-empty trace bytes")
				}
			}

			if store.count() != tc.wantPuts {
				t.Errorf("puts: got %d, want %d", store.count(), tc.wantPuts)
			}
			if lp.logger.count() != tc.wantLogs {
				t.Errorf("log records: got %d, want %d", lp.logger.count(), tc.wantLogs)
			}
			if logRec, ok := lp.logger.last(); ok {
				if logRec.Severity() != otellog.SeverityWarn {
					t.Errorf("log severity: got %v, want WARN", logRec.Severity())
				}
			}

			rm := collectMetrics(t, reader)
			if n := findCounter(t, rm, "squawk.snapshots"); n != tc.wantSnapshotCount {
				t.Errorf("squawk.snapshots: got %d, want %d", n, tc.wantSnapshotCount)
			}
			if n := findCounter(t, rm, "squawk.dropped"); n != tc.wantDroppedCount {
				t.Errorf("squawk.dropped: got %d, want %d", n, tc.wantDroppedCount)
			}
		})
	}
}

func TestRateLimit(t *testing.T) {
	tests := []struct {
		name        string
		burst       int
		snapshots   int
		wantPuts    int
		wantDropped int64
	}{
		{
			name:        "burst=2 drops third",
			burst:       2,
			snapshots:   3,
			wantPuts:    2,
			wantDropped: 1,
		},
		{
			name:        "burst=1 drops after first",
			burst:       1,
			snapshots:   3,
			wantPuts:    1,
			wantDropped: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fr := startFR(t)
			store := &memStorage{}
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

			rec, err := squawk.New(fr,
				squawk.WithStorage(store),
				squawk.WithMeterProvider(mp),
				squawk.WithRateLimit(10*time.Second, tc.burst),
			)
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			for i := range tc.snapshots {
				ref, err := rec.Snapshot(ctx, fmt.Sprintf("r%d", i+1))
				if err != nil {
					t.Fatalf("snapshot %d: %v", i+1, err)
				}
				// calls beyond burst must return an empty Ref with no error
				if i >= tc.burst && ref.Key != "" {
					t.Errorf("snapshot %d (over burst): expected empty Ref, got %+v", i+1, ref)
				}
			}

			if store.count() != tc.wantPuts {
				t.Errorf("puts: got %d, want %d", store.count(), tc.wantPuts)
			}

			rm := collectMetrics(t, reader)
			if n := findCounter(t, rm, "squawk.dropped"); n != tc.wantDropped {
				t.Errorf("squawk.dropped: got %d, want %d", n, tc.wantDropped)
			}
		})
	}
}
