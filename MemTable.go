package lsm

import (
	"log"
	"sync"

	config "github.com/QinLinag/omniponent_lsm/Config"
	"github.com/QinLinag/omniponent_lsm/kv"
	"github.com/QinLinag/omniponent_lsm/sortTree"
	"github.com/QinLinag/omniponent_lsm/wal"
)

//内存表
type MemTable struct {
	MemoryTree *sortTree.Tree
	//wal文件句柄
	Wal *wal.Wal
	lock *sync.RWMutex
}


/*
database启动时，初始化内存表。
*/
func NewMemTree() *MemTable {
	conf := config.GetConfig()
	table := MemTable{}
	table.Init(conf.DataDir)
	return &table
}
func (table *MemTable) Init(dir string) {
	log.Println("Init MemTable..")
	table.lock = &sync.RWMutex{}
	table.MemoryTree = sortTree.NewSortTree()
	table.Wal = wal.NewWal(table.MemoryTree)
}


/*
内存二叉树转化为只读内存树,并且reset原来的内存树
*/
func (table *MemTable) Swap() *MemTable {
	table.lock.Lock()
	defer table.lock.Unlock()
	tree := table.MemoryTree
	newTree := table.MemoryTree.Swap()
	newTable := MemTable{
		MemoryTree: newTree,
		Wal: table.Wal,
		lock: &sync.RWMutex{},
	}
	table.Wal = wal.NewWal(tree)
	return &newTable
}




/*
增删查功能接口
*/
func (table *MemTable) Delete(key string) (kv.Value, bool) {
	table.lock.RLock()
	defer table.lock.RUnlock()
	value, success := table.MemoryTree.Delete(key)
	if success {
		//写入wal文件
		table.Wal.Write(value)
	}
	return value, success
}
func(table *MemTable) Insert(key string, value []byte) (kv.Value, bool) {
	table.lock.RLock()
	defer table.lock.RUnlock()
	KV := kv.Value{
		Key: key,
		Value: value,
		Deleted: false,
	}
	//写入wal文件中
	table.Wal.Write(KV)
	//插入内存树中
	return table.MemoryTree.Insert(&KV)
}
func(table *MemTable) Search(key string) (kv.Value, kv.SearchResult){
	table.lock.RLock()
	defer table.lock.RUnlock()
	return table.MemoryTree.Search(key)
}