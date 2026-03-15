package fio

import (
	"path/filepath"
	"testing"
)

// tempFilePath 在测试临时目录中生成一个不会冲突的文件路径，测试结束后自动清理。
func tempFilePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.data")
}

func TestNewFileIO(t *testing.T) {
	tests := []struct {
		name     string
		filePath string // 空字符串时使用临时路径
		wantErr  bool
	}{
		{
			name:    "valid path creates file",
			wantErr: false,
		},
		{
			name:     "invalid directory returns error",
			filePath: "/no_exist_dir_xyz/test.data",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.filePath
			if path == "" {
				path = tempFilePath(t)
			}
			fio, err := NewFileIO(path)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileIO() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if fio == nil {
					t.Error("NewFileIO() = nil, want non-nil")
				} else {
					fio.Close()
				}
			}
		})
	}
}

func TestFileIO_WriteAt(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		off     int64
		wantN   int
		wantErr bool
	}{
		{
			name:  "write at offset 0",
			data:  []byte("hello"),
			off:   0,
			wantN: 5,
		},
		{
			name:  "write at non-zero offset",
			data:  []byte("world"),
			off:   10,
			wantN: 5,
		},
		{
			name:  "write empty bytes",
			data:  []byte{},
			off:   0,
			wantN: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileIO(tempFilePath(t))
			if err != nil {
				t.Fatalf("NewFileIO() error = %v", err)
			}
			defer fio.Close()

			n, err := fio.WriteAt(tt.data, tt.off)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileIO.WriteAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if n != tt.wantN {
				t.Errorf("FileIO.WriteAt() n = %v, want %v", n, tt.wantN)
			}
		})
	}
}

func TestFileIO_ReadAt(t *testing.T) {
	tests := []struct {
		name    string
		write   []byte // 预先写入的内容
		writeAt int64
		readLen int
		readAt  int64
		want    string
		wantN   int
		wantErr bool
	}{
		{
			name:    "read at offset 0",
			write:   []byte("hello"),
			writeAt: 0,
			readLen: 5,
			readAt:  0,
			want:    "hello",
			wantN:   5,
		},
		{
			name:    "read at non-zero offset",
			write:   []byte("hello world"),
			writeAt: 0,
			readLen: 5,
			readAt:  6,
			want:    "world",
			wantN:   5,
		},
		{
			name:    "read beyond EOF returns error",
			write:   []byte("hi"),
			writeAt: 0,
			readLen: 10,
			readAt:  100,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileIO(tempFilePath(t))
			if err != nil {
				t.Fatalf("NewFileIO() error = %v", err)
			}
			defer fio.Close()

			if _, err := fio.WriteAt(tt.write, tt.writeAt); err != nil {
				t.Fatalf("WriteAt() setup error = %v", err)
			}

			buf := make([]byte, tt.readLen)
			n, err := fio.ReadAt(buf, tt.readAt)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileIO.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if n != tt.wantN {
					t.Errorf("FileIO.ReadAt() n = %v, want %v", n, tt.wantN)
				}
				if string(buf[:n]) != tt.want {
					t.Errorf("FileIO.ReadAt() = %q, want %q", buf[:n], tt.want)
				}
			}
		})
	}
}

func TestFileIO_Sync(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "sync open file succeeds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileIO(tempFilePath(t))
			if err != nil {
				t.Fatalf("NewFileIO() error = %v", err)
			}
			defer fio.Close()
			fio.WriteAt([]byte("data"), 0)
			if err := fio.Sync(); (err != nil) != tt.wantErr {
				t.Errorf("FileIO.Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileIO_Close(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "close open file succeeds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileIO(tempFilePath(t))
			if err != nil {
				t.Fatalf("NewFileIO() error = %v", err)
			}
			if err := fio.Close(); (err != nil) != tt.wantErr {
				t.Errorf("FileIO.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
