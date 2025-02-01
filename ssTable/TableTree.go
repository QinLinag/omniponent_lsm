package ssTable

import (
	"strconv"
	"sync"

	config "github.com/QinLinag/omniponent_lsm/config"
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

// 插入一个sstable到tableTree的指定层链表最后一个位置，并返回对应的index
func (tree *TableTree) insert(table *SSTable, level int) int {
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

// 返回某层index最大的table的index
func (tree *TableTree) getMaxIndex(level int) int {
	node := tree.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}

func (tree *TableTree) getCount(level int) int {
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
	//创建文件名、利用文件名创建文件sstable文件，返回sstable内存对象
	index := tree.getMaxIndex(level) + 1
	filePath := config.GetConfig().DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	newSSTable := NewSSTableWithValues(values, filePath)
	//插入tableNode节点
	tree.insert(newSSTable, level)
	return newSSTable
}
