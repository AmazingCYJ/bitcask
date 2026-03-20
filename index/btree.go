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

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 32 是 BTree 的 degree，可以根据实际情况调整
		lock: &sync.RWMutex{},
	}
}
