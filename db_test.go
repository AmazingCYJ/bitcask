package bitcaskmy

import (
	"bitcask-my/common"
	"errors"
	"testing"
)

func testOptions(dir string) common.Options {
	return common.Options{
		DirPath:      dir,
		DataFileSize: 1024 * 1024,
		SyncWrites:   true,
	}
}

func TestCheckOptions(t *testing.T) {
	tests := []struct {
		name    string
		opt     common.Options
		wantErr bool
	}{
		{
			name: "empty dir path",
			opt: common.Options{
				DirPath:      "",
				DataFileSize: 1024,
			},
			wantErr: true,
		},
		{
			name: "invalid data file size",
			opt: common.Options{
				DirPath:      t.TempDir(),
				DataFileSize: 0,
			},
			wantErr: true,
		},
		{
			name:    "valid options",
			opt:     testOptions(t.TempDir()),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkOptions(tt.opt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("checkOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOpenAndBasicCRUD(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(testOptions(dir))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := db.Put([]byte{}, []byte("v")); !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("Put(empty key) error = %v, want ErrKeyNotFound", err)
	}

	if err := db.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	v, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(v) != "alice" {
		t.Fatalf("Get() = %q, want %q", v, "alice")
	}

	if err := db.Put([]byte("name"), []byte("bob")); err != nil {
		t.Fatalf("Put(overwrite) error = %v", err)
	}
	v, err = db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get(overwrite) error = %v", err)
	}
	if string(v) != "bob" {
		t.Fatalf("Get(overwrite) = %q, want %q", v, "bob")
	}

	if err := db.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := db.Get([]byte("name")); !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("Get(after delete) error = %v, want ErrKeyNotFound", err)
	}

	if err := db.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete(missing) error = %v, want nil", err)
	}

	if _, err := db.Get([]byte{}); !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("Get(empty key) error = %v, want ErrKeyNotFound", err)
	}
}

func TestOpenReloadIndexFromDataFiles(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)

	db1, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	if err := db1.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put(k1) error = %v", err)
	}
	if err := db1.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put(k2) error = %v", err)
	}
	if err := db1.Delete([]byte("k2")); err != nil {
		t.Fatalf("Delete(k2) error = %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(reopen) error = %v", err)
	}

	v, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get(k1) after reopen error = %v", err)
	}
	if string(v) != "v1" {
		t.Fatalf("Get(k1) after reopen = %q, want %q", v, "v1")
	}

	if _, err := db2.Get([]byte("k2")); !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("Get(k2) after reopen error = %v, want ErrKeyNotFound", err)
	}
}
