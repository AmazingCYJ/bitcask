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

type WriteBatchOptions struct {
	MaxBatchSize uint // 批量写入的最大操作数
	SyncWrite    bool // 是否在批量写入后立即将数据刷新到磁盘
}

type IndexerType = int8

const (
	// BTreeIndex 基于 BTree 实现的索引
	BTreeIndex IndexerType = iota + 1
	// 未来可以添加其他索引类型，如 HashIndex、LSMTreeIndex 等
	//ARTreeIndex
	ARTreeIndex
	// BPlusTreeIndex 基于 B+ Tree 实现的索引
	BPlusTreeIndex
)

const (
	DataFileSuffix        = ".data"          // 数据文件后缀
	HintFileName          = "hin-index"      // Hint 文件名
	MergeFinishedFileName = "merge-finished" // Merge 完成标识文件名
	SeqNoFileName         = "seq-no"         // 序列号文件名
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),      // 默认使用系统临时目录
	DataFileSize: 256 * 1024 * 1024, // 默认数据文件大小为 256MB
	// IndexType:    BTreeIndex,        // 默认使用 BTree 索引
	IndexType:  BPlusTreeIndex, // 默认使用 B+ Tree 索引
	SyncWrites: false,          // 默认启用同步写入
}
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,   // 默认不使用前缀过滤
	Reverse: false, // 默认正向迭代
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 100,   // 默认批量写入的最大操作数为 100
	SyncWrite:    false, // 默认启用同步写入
}
