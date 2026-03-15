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

// LogRecordHeader 日志记录头部信息
type LogRecordHeader struct {
	crc        uint32        // 数据校验码
	recordType LogRecordType // 记录类型
	keySize    uint32        // key的长度
	valueSize  uint32        // value的长度
}

// EncodeLogRecord 将 LogRecord 序列化为字节数组，并返回序列化后的字节数组和记录的总大小（包括头部和数据）。
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecord 从字节数组中解析出 LogRecordHeader，并返回解析出的 LogRecordHeader 和头部的总大小（包括 CRC、recordType、keySize 和 valueSize）。
func decodeLogRecord(data []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	return 0
}
