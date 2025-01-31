package ssTable

import (
	"os"
	"sync"
)


type SSTable struct {
	f *os.File
	filePath string

	tableMetaInfo MetaInfo

	//稀疏索引列表   在内存中
	sparseIndex map[string]Position

	//内存中排序的key
	sortIndex []string

	lock sync.Locker
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.lock = &sync.Mutex{}
	table.loadFileHandle()
}