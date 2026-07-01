// Package cloud provides a squawk.Storage adapter over gocloud.dev/blob,
// supporting S3, GCS, Azure Blob, local filesystem, and in-memory backends
// through a single URL-based configuration string.
//
// Import the provider driver you need alongside this package, e.g.:
//
//	import _ "gocloud.dev/blob/s3blob"    // AWS S3 / S3-compatible
//	import _ "gocloud.dev/blob/gcsblob"   // Google Cloud Storage
//	import _ "gocloud.dev/blob/azureblob" // Azure Blob Storage
//	import _ "gocloud.dev/blob/fileblob"  // local filesystem (file://)
//	import _ "gocloud.dev/blob/memblob"   // in-memory (mem://)
package cloud

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gocloud.dev/blob"

	"github.com/rashmi-tondare/go-squawk"
)

// Bucket is a squawk.Storage backed by a gocloud.dev/blob bucket.
type Bucket struct {
	bucket    *blob.Bucket
	bucketURL string
}

// Open opens a blob bucket at the given URL and returns a Bucket.
// URL examples: "s3://my-bucket", "gs://my-bucket", "file:///data/traces", "mem://".
func Open(ctx context.Context, url string) (*Bucket, error) {
	b, err := blob.OpenBucket(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("squawk/cloud: open bucket %q: %w", url, err)
	}
	return &Bucket{bucket: b, bucketURL: url}, nil
}

// Close releases the underlying bucket.
func (s *Bucket) Close() error { return s.bucket.Close() }

// Put uploads r to the bucket under key and returns a Ref with a URI derived from the bucket URL.
func (s *Bucket) Put(ctx context.Context, key string, r io.Reader, _ int64) (squawk.Ref, error) {
	w, err := s.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return squawk.Ref{}, fmt.Errorf("squawk/cloud: new writer for %q: %w", key, err)
	}
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return squawk.Ref{}, fmt.Errorf("squawk/cloud: copy to %q: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return squawk.Ref{}, fmt.Errorf("squawk/cloud: close writer for %q: %w", key, err)
	}
	return squawk.Ref{Key: key, URI: s.objectURI(key)}, nil
}

// objectURI builds a URI from the bucket URL and the object key.
// For "s3://my-bucket" + "traces/svc/2025-01-01/123-foo.trace" → "s3://my-bucket/traces/svc/…"
func (s *Bucket) objectURI(key string) string {
	base := strings.TrimSuffix(s.bucketURL, "/") // remove at most one trailing slash
	return base + "/" + key
}
