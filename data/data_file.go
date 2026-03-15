package data

import "bitcask-my/fio"

type DataFile struct {
	FileID    uint32        // 文件ID
	WriteOff  int64         // 当前写入偏移
	IoManager fio.IOManager //io读写管理
}

func OpenDataFile(dirPath string, fileID uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
func (df *DataFile) WriteAt(p []byte, off int64) error {
	return nil
}
func (df *DataFile) ReadLogRecord(off int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
