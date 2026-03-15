package index

import (
	"bitcask-my/data"

	. "bitcask-my/common"

	"github.com/google/btree"
)

// Indexer 索引器接口
type Indexer interface {
	// Put 索引数据
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 获取索引数据
	Get(key []byte) *data.LogRecordPos
	// Delete 删除索引数据
	Delete(key []byte) error
}

func NewIndexer(idextype IndexType) Indexer {
	switch idextype {
	case BTreeIndex:
		return NewBTree()
	case ARTreeIndex:
		// 返回 ARTree 索引实现
		return nil
	default:
		panic("unsupported index type")
	}
}

// Item 表示索引中的一条键位置信息。
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 定义 BTree 中 Item 的有序比较规则（按 key 字典序）。
func (i *Item) Less(than btree.Item) bool {
	return string(i.key) < string(than.(*Item).key)
}
