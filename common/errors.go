package common

import "errors"

var (
	// ErrKeyNotFound 表示索引中不存在对应 key。
	ErrKeyNotFound            = errors.New("key not found")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory is corrupted")
)
