package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecordPos 数据内存索引,描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id
	Offset int64  // 文件内偏移
}

// LogRecord 数据记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
