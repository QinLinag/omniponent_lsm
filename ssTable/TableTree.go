package ssTable

import (
	"sync"

	"github.com/QinLinag/omniponent_lsm/kv"
)

type TableTree struct {
	levels []*tableNode
	lock   *sync.RWMutex
}

func (tree *TableTree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	//todo 可优化
	for _, node := range tree.levels {
		tables := make([]*SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}
		//从最后一个table开始找，
		for i := len(tables) - 1; i >= 0; i-- {
			value, result := tables[i].Search(key)
			if kv.IsNone(result) { //没找到就下一个table
				continue
			}
			return value, result //可能是deleted/success
		}
	}
	return kv.Value{}, kv.None
}

// 返回某层index最大的table的index
func (tree *TableTree) getMaxIndexFromCertainLevel(level int) int {
	tree.lock.RLock()
	defer tree.lock.Unlock()
	if level>= len(tree.levels) { //非法层
		return -1 
	}
	node := tree.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}
//某层节点数
func (tree *TableTree) getCountFromCertainLevel(level int) int {
	tree.lock.RLock()
	defer tree.lock.Unlock()
	if level>= len(tree.levels) { //非法层
		return -1 
	}
	node := tree.levels[level]
	count := 0

	for node != nil {
		count++
		node = node.next
	}
	return count
}
// 内存满了后，创建一个新的tableNode插入tree第一层
func (tree *TableTree) CreateNewTable(values []kv.Value) *SSTable {
	return tree.createTable(values, 0)
}

//根据values创建一个tableNode插入到Tabletree
func (tree *TableTree) createTable(values []kv.Value, level int) *SSTable {
	//返回sstable内存对象
	index := tree.getMaxIndexFromCertainLevel(level)
	newSSTable := NewSSTableWithValues(values, level, index + 1)
	//插入tableNode节点
	tree.insert(newSSTable, level)
	return newSSTable
}
// 插入一个sstable到tableTree的指定层链表最后一个位置，并返回对应的index
func (tree *TableTree) insert(table *SSTable, level int) int {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	if level >= len(tree.levels) { //代表level不合法
		return -1
	}
	newNode := tableNode{
		index: 0,
		table: table,
		next:  nil,
	}
	node := tree.levels[level]

	if node == nil {
		tree.levels[level] = &newNode
	} else {
		for node.next != nil {
			node = node.next
		}
		node.next = &newNode
		newNode.index = node.index + 1
	}
	return newNode.index
}
