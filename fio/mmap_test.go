package fio

import (
	"bitcask-my/common"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func createMMapTestFile(t *testing.T, content []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap-test.data")
	if err := os.WriteFile(path, content, DataFilePerm); err != nil {
		t.Fatalf("write test file error = %v", err)
	}
	return path
}

func TestNewMMapIOManagerAndReadAt(t *testing.T) {
	content := []byte("hello mmap world")
	path := createMMapTestFile(t, content)

	m, err := NewMMapIOManager(path)
	if err != nil {
		t.Fatalf("NewMMapIOManager() error = %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	sz, err := m.Size()
	if err != nil {
		t.Fatalf("Size() error = %v", err)
	}
	if sz != int64(len(content)) {
		t.Fatalf("Size() = %d, want %d", sz, len(content))
	}

	buf := make([]byte, 4)
	n, err := m.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt(off=6) error = %v", err)
	}
	if n != 4 {
		t.Fatalf("ReadAt(off=6) n = %d, want 4", n)
	}
	if string(buf) != "mmap" {
		t.Fatalf("ReadAt(off=6) = %q, want %q", string(buf), "mmap")
	}

	partial := make([]byte, 4)
	n, err = m.ReadAt(partial, int64(len(content)-2))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt(partial) error = %v, want io.EOF", err)
	}
	if n != 2 {
		t.Fatalf("ReadAt(partial) n = %d, want 2", n)
	}
	if string(partial[:2]) != "ld" {
		t.Fatalf("ReadAt(partial) bytes = %q, want %q", string(partial[:2]), "ld")
	}
}

func TestMMapWriteSyncAndClose(t *testing.T) {
	path := createMMapTestFile(t, []byte("abc"))

	m, err := NewMMapIOManager(path)
	if err != nil {
		t.Fatalf("NewMMapIOManager() error = %v", err)
	}

	n, err := m.Write([]byte("x"))
	if n != 0 {
		t.Fatalf("Write() n = %d, want 0", n)
	}
	if !errors.Is(err, common.ErrMMapWriteNotSupported) {
		t.Fatalf("Write() error = %v, want %v", err, common.ErrMMapWriteNotSupported)
	}

	if err := m.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close() second error = %v", err)
	}
}

func TestNewMMapIOManagerFileNotFound(t *testing.T) {
	_, err := NewMMapIOManager(filepath.Join(t.TempDir(), "not-exist.data"))
	if err == nil {
		t.Fatalf("NewMMapIOManager(not-exist) error = nil, want non-nil")
	}
}
