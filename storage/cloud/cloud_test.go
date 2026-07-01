package cloud_test

import (
	"bytes"
	"context"
	"testing"

	_ "gocloud.dev/blob/memblob" // register mem:// driver

	"github.com/rashmi-tondare/go-squawk/storage/cloud"
)

func TestBucket(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		wantOpenErr bool
		// put fields — only used when wantOpenErr is false
		putKey  string
		putData []byte
		wantURI string
	}{
		{
			name:    "stores object and returns addressable ref",
			url:     "mem://",
			putKey:  "traces/svc/2025-01-01/123-test.trace",
			putData: []byte("fake trace data"),
			wantURI: "mem://traces/svc/2025-01-01/123-test.trace",
		},
		{
			name:        "unknown scheme returns error",
			url:         "bogusscheme://bucket",
			wantOpenErr: true,
		},
		{
			name:        "unregistered driver returns error",
			url:         "s3://bucket", // valid scheme but s3blob driver not imported
			wantOpenErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := cloud.Open(ctx, tc.url)
			if (err != nil) != tc.wantOpenErr {
				t.Fatalf("Open(%q) error = %v, wantOpenErr %v", tc.url, err, tc.wantOpenErr)
			}
			if tc.wantOpenErr {
				return
			}
			defer store.Close()

			ref, err := store.Put(ctx, tc.putKey, bytes.NewReader(tc.putData), int64(len(tc.putData)))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if ref.Key != tc.putKey {
				t.Errorf("ref.Key: got %q, want %q", ref.Key, tc.putKey)
			}
			if ref.URI != tc.wantURI {
				t.Errorf("ref.URI: got %q, want %q", ref.URI, tc.wantURI)
			}
		})
	}
}
