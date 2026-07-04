# go-squawk

The name comes from aviation: a squawk code is the four-digit transponder signal a pilot sets to declare an emergency.
It pairs intentionally with Go's `runtime/trace.FlightRecorder`: when your program detects something wrong, it squawks.

**go-squawk** captures a [Go execution trace](https://pkg.go.dev/runtime/trace) snapshot when your program detects an
anomaly, persists it to durable storage, and emits an observable signal so your monitoring backend can alert on it.

> **Note:** This library works with Go *execution* traces (viewable with `go tool trace`). It has nothing to do with
> OpenTelemetry distributed tracing.

## How it works

When you call `Snapshot`, squawk:

1. Calls `FlightRecorder.WriteTo` to capture the recent trace window.
2. Uploads the bytes to your configured storage backend.
3. Increments an OTel metric and emits a WARN log record so you can alert on the event.

Storage failures still emit the signal. A rate limiter prevents a crash-loop from flooding storage; suppressed snapshots
increment a `squawk.dropped` counter so suppression is never silent.

`Snapshot` performs blocking I/O (the storage write) on the calling goroutine. If you're calling it from a
latency-sensitive path, invoke it from your own goroutine instead.

## Installation

```
go get github.com/rashmi-tondare/go-squawk
```

Requires Go 1.25+.

## Quick start

```go
import (
"runtime/trace"
squawk "github.com/rashmi-tondare/go-squawk"
)

fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: 5 * time.Second})
fr.Start()
defer fr.Stop()

rec, err := squawk.New(fr,
squawk.WithStorage(&squawk.LocalStorage{Dir: "/var/traces"}),
squawk.WithServiceName("my-service"),
squawk.WithMeterProvider(mp), // your OTel MeterProvider
squawk.WithLoggerProvider(lp), // your OTel LoggerProvider
)

// Call this whenever you detect an anomaly.
ref, err := rec.Snapshot(ctx, "high-latency")
// ref.URI -> "file:///var/traces/traces/my-service/2025-01-15/1234567890-high-latency.trace"
```

Open the resulting file with `go tool trace <path>`.

A runnable version of this example is in [examples/basic](./examples/basic).

## Storage

### Local filesystem

```go
squawk.WithStorage(&squawk.LocalStorage{Dir: "/var/traces"})
```

No extra dependencies.

### Cloud (S3, GCS, Azure, and others)

```go
import (
"github.com/rashmi-tondare/go-squawk/storage/cloud"
_ "gocloud.dev/blob/s3blob" // or gcsblob, azureblob, fileblob, memblob
)

bucket, err := cloud.Open(ctx, "s3://my-bucket")
defer bucket.Close()

squawk.WithStorage(bucket)
```

The `storage/cloud` package wraps [gocloud.dev/blob](https://gocloud.dev/howto/blob/), so any backend it supports works
here. Import the driver you need alongside the package.

Object keys follow the format `traces/{service}/{YYYY-MM-DD}/{unixnano}-{reason}.trace`. Retention is not managed by the
library; use your bucket's lifecycle policies.

## OTel signals

Every snapshot attempt emits:

| Signal                     | Type      | Attributes                                                   |
|----------------------------|-----------|--------------------------------------------------------------|
| `squawk.snapshots`         | counter   | `reason`, `outcome` (`ok` / `storage_error` / `write_error`) |
| `squawk.bytes_written`     | histogram | same                                                         |
| `squawk.snapshot.duration` | histogram | same                                                         |
| `squawk.dropped`           | counter   | `reason`                                                     |

A WARN log record is also emitted with attributes `squawk.reason`, `squawk.uri`, `squawk.bytes`, `service.name`
plus any resource attributes you set via `WithResourceAttrs`. You can alert on either the metric or the log record in
your monitoring backend.

`WithMeterProvider` accepts any standard OTel `metric.MeterProvider`, so any exporter works: Prometheus, OTLP, Datadog,
etc. For Prometheus:

```go
import (
"go.opentelemetry.io/otel/exporters/prometheus"
sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

exp, _ := prometheus.New()
mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))

squawk.WithMeterProvider(mp) // squawk.snapshots, squawk.dropped, etc. appear automatically
```

## Rate limiting

By default squawk allows one snapshot per second with a burst of one. Override with:

```go
squawk.WithRateLimit(500*time.Millisecond, 3) // min gap, burst
```

Suppressed snapshots return an empty `Ref` and no error, but always increment `squawk.dropped`.

## Options reference

| Option                      | Description                                                                   |
|-----------------------------|-------------------------------------------------------------------------------|
| `WithStorage(s)`            | Storage backend. Required.                                                   |
| `WithServiceName(name)`     | Service name used in the storage key path and as the `service.name` attribute on every metric and log record. Required. |
| `WithMeterProvider(mp)`     | OTel meter provider                                                           |
| `WithLoggerProvider(lp)`    | OTel logger provider                                                          |
| `WithRateLimit(min, burst)` | Token-bucket rate limit. Default: 1s / burst 1.                               |
| `WithResourceAttrs(kvs...)` | Additional resource attributes added to every metric and log record          |

## Implementing your own storage

```go
type Storage interface {
    Put(ctx context.Context, key string, r io.Reader, size int64) (squawk.Ref, error)
}
```

Any type implementing this interface works with `WithStorage`.
