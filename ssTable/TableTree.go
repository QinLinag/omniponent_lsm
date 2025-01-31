package ssTable

import (
	"sync"

	"github.com/QinLinag/omniponent_lsm/kv"
)


type TableTree struct {
	levels []*tableNode

	lock *sync.RWMutex
}


//链表，tableTree每一层都是一个链表
type tableNode struct {
	index int
	table *SSTable
	next *tableNode
}


func (tree *TableTree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	//todo 可优化
	for _, node := range tree.levels {
		tables := make([]*tableNode, 0)
		for node != nil {
			tables = append(tables, node)
			node = node.next
		}
		//从最后一个table开始找，
		for i := len(tables) - 1; i >= 0; i-- {
			
		}
	}
}