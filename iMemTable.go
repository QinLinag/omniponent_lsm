package lsm

import (
	"log"
	"os"
	"path"
	"sync"

	config "github.com/QinLinag/omniponent_lsm/Config"
	"github.com/QinLinag/omniponent_lsm/kv"
	"github.com/QinLinag/omniponent_lsm/wal"
)

// 只读的内存表
type ReadOnlyMemTable struct {
	readOnlyTable []*MemTable
	lock          *sync.RWMutex
}

/*
只读内存表，初始化模块,同时加载wal.log文件。
*/
func NewReadOnlyMemTable() *ReadOnlyMemTable {
	conf := config.GetConfig()
	dir := conf.DataDir
	table := ReadOnlyMemTable{}
	table.Init(dir)
	return &table
}
func (rTable *ReadOnlyMemTable) Init(dir string) {
	rTable.readOnlyTable = make([]*MemTable, 0)
	rTable.lock = &sync.RWMutex{}
	//判断是否有.log文件，如果有不需要重新创建log文件，并且要读log数据到内存中
	infos, err := os.ReadDir(dir)
	if err != nil {
		log.Println("Failed to new wal, directory is: ", dir)
		panic(err)
	}
	for _, info := range infos {
		fileName := info.Name()
		if path.Ext(fileName) == ".log" { //如果有log文件
			preWal := &wal.Wal{}
			tree := preWal.LoadFromFile(fileName)
			table := MemTable{
				MemoryTree: tree,
				Wal:        preWal,
				lock:       &sync.RWMutex{},
			}
			rTable.Insert(&table)
		}
	}
}

/*
只读内存表，搜索模块
*/
func (rTable *ReadOnlyMemTable) Search(key string) (kv.Value, kv.SearchResult) {
	rTable.lock.Lock()
	defer rTable.lock.RUnlock()
	memTables := rTable.readOnlyTable
	//最后的最新
	for i := len(memTables) - 1; i >= 0; i-- {
		value, result := memTables[i].Search(key)
		if kv.IsNone(result) {
			continue
		}
		return value, result
	}

	return kv.Value{}, kv.None
}

/*
只读内存表，转化为一个sstable
*/

/*
功能性模块
*/
//返回只读内存表中有多少课二叉排序树
func (rTable *ReadOnlyMemTable) GetLen() int {
	rTable.lock.RLock()
	defer rTable.lock.RUnlock()
	return len(rTable.readOnlyTable)
}

// 获得只读内存表中，最老的一颗树
func (rTable *ReadOnlyMemTable) GetAndDeleteTable() *MemTable {
	len := rTable.GetLen()
	if len == 0 {
		return nil
	}
	rTable.lock.RLock()
	defer rTable.lock.RUnlock()

	table := rTable.readOnlyTable[0]
	rTable.readOnlyTable = rTable.readOnlyTable[1:len]
	return table
}
func (rTable *ReadOnlyMemTable) Insert(table *MemTable) {
	rTable.lock.Lock()
	defer rTable.lock.Unlock()
	rTable.readOnlyTable = append(rTable.readOnlyTable, table)
}
