package squawk

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Storage persists snapshot bytes and returns a Ref identifying the object.
type Storage interface {
	Put(ctx context.Context, key string, r io.Reader, size int64) (Ref, error)
}

// LocalStorage writes snapshots to the local filesystem under Dir.
// Subdirectories are created automatically.
type LocalStorage struct {
	Dir string
}

func (s *LocalStorage) Put(_ context.Context, key string, r io.Reader, _ int64) (Ref, error) {
	path := filepath.Join(s.Dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Ref{}, fmt.Errorf("squawk/local: mkdir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return Ref{}, fmt.Errorf("squawk/local: create: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return Ref{}, fmt.Errorf("squawk/local: write: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	return Ref{Key: key, URI: "file://" + abs}, nil
}
