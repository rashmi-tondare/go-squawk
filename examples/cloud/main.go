// Command cloud demonstrates go-squawk with the storage/cloud backend, which wraps
// gocloud.dev/blob so the same code works against S3, GCS, Azure Blob, or (as used
// here) an in-memory bucket that needs no credentials to run.
//
// To point this at a real bucket, swap the blank import and the URL passed to
// cloud.Open, e.g.:
//
//	import _ "gocloud.dev/blob/s3blob"
//	cloud.Open(ctx, "s3://my-bucket?region=us-west-2")
//
// Each driver has its own required/optional URL parameters (region, credentials,
// endpoints); see https://gocloud.dev/howto/blob/ for the full reference.
package main

import (
	"context"
	"fmt"
	"log"
	"runtime/trace"
	"time"

	_ "gocloud.dev/blob/memblob" // registers the mem:// driver

	"github.com/rashmi-tondare/go-squawk"
	"github.com/rashmi-tondare/go-squawk/storage/cloud"
)

func main() {
	ctx := context.Background()

	// -- Cloud storage (mem:// here so the example runs with no setup) --
	bucket, err := cloud.Open(ctx, "mem://")
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()

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
		squawk.WithStorage(bucket),
		squawk.WithServiceName("example-service"),
	)
	if err != nil {
		log.Fatal(err)
	}

	ref, err := rec.Snapshot(ctx, "high-latency")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Snapshot saved:\n  key=%s\n  uri=%s\n", ref.Key, ref.URI)
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
