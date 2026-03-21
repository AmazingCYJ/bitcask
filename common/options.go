package common

import "os"

type Options struct {
	DirPath      string      // 数据文件存储路径
	DataFileSize int64       // 每个数据文件的最大大小，单位字节
	IndexType    IndexerType // 索引类型
	SyncWrites   bool        // 是否在每次写入后立即将数据刷新到磁盘
}
type IteratorOptions struct {
	Prefix  []byte // 迭代器只返回以该前缀开头的 key
	Reverse bool   // 是否反向迭代
}

type IndexerType = int8

const (
	// BTreeIndex 基于 BTree 实现的索引
	BTreeIndex IndexerType = iota + 1
	// 未来可以添加其他索引类型，如 HashIndex、LSMTreeIndex 等
	//ARTreeIndex
	ARTreeIndex
)

const (
	DataFileSuffix = ".data" // 数据文件后缀
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),      // 默认使用系统临时目录
	DataFileSize: 256 * 1024 * 1024, // 默认数据文件大小为 256MB
	IndexType:    BTreeIndex,        // 默认使用 BTree 索引
	SyncWrites:   false,             // 默认启用同步写入
}
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,   // 默认不使用前缀过滤
	Reverse: false, // 默认正向迭代
}
