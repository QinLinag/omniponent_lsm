package ssTable

import (
	"log"
	"os"
	"sync"

	"github.com/QinLinag/omniponent_lsm/kv"
)


type SSTable struct {
	f *os.File
	filePath string

	tableMetaInfo MetaInfo

	//稀疏索引列表   在内存中
	sparseIndex map[string]Position

	//内存中排序的key
	sortIndex []string

	//sstable 虽然不需要修改，但是有合并的过程，所以需要加锁
	lock sync.Locker
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.lock = &sync.Mutex{}
	table.loadFileHandle()
}

//从sstable中找到 kv.value对象
func (table *SSTable) Search(key string) (kv.Value, kv.SearchResult) {
	table.lock.Lock()
	defer table.lock.Unlock()

	position, has := table.sparseIndex[key]
	if !has {
		return kv.Value{}, kv.None
	} 
	if position.Deleted {
		return kv.Value{}, kv.Deleted
	}

	bytes := make([]byte, position.Len)

	_, err := table.f.Seek(0, int(position.Start))
	if err != nil {
		log.Println("Failed to Search SSTable!", table.filePath)
		panic(err)
	}

	_, err = table.f.Read(bytes)
	if err != nil {
		log.Println("Failed to Search SSTable!", table.filePath)
		panic(err)
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println("Failed to Search SSTable!", table.filePath)
		panic(err)
	}
	return value, kv.Success
}