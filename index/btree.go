package index

import (
	// . "bitcask-my/common"
	"bitcask-my/data"

	"sync"

	"github.com/google/btree"
)

// BTree 基于 google/btree 实现内存索引，并通过读写锁保证并发安全。
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 32 是 BTree 的 degree，可以根据实际情况调整
		lock: &sync.RWMutex{},
	}
}

// Put 写入或更新 key 对应的位置。
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	bt.tree.ReplaceOrInsert(&Item{key: key, pos: pos})
	return true
}

// Get 查询 key 对应的位置，不存在时返回 ErrKeyNotFound。
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	item := bt.tree.Get(&Item{key: key})
	if item == nil {
		return nil
	}
	return item.(*Item).pos
}

// Delete 删除 key 对应的位置。
func (bt *BTree) Delete(key []byte) error {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	bt.tree.Delete(&Item{key: key})
	return nil
}

// BTreeIterator 基于 BTree 实现的索引迭代器。
type BTreeIterator struct {
	currIndex int     // 当前索引位置
	reverse   bool    // 是否反向迭代
	item      []*Item // key + 位置索引信息
}

// Rewind 将迭代器重置到起始位置。
func (bti *BTreeIterator) Rewind() {
}

// Seek 将迭代器移动到指定 key 的位置。
func (bti *BTreeIterator) Seek(key []byte) {
}

// Next 将迭代器移动到下一个位置。
func (bti *BTreeIterator) Next() {
}

// Vaild 检查迭代器当前是否有效。
func (bti *BTreeIterator) Vaild() bool {
	return false
}

// Key 返回当前迭代器位置的 key。
func (bti *BTreeIterator) Key() []byte {
	return nil
}

// Value 返回当前迭代器位置的 value。
func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return nil
}

// Close 关闭迭代器，释放相关资源。
func (bti *BTreeIterator) Close() {
}
