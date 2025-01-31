package ssTable

import (
	"log"
	"os"
	"sync"
	"encoding/binary"
	"encoding/json"
	"sort"
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


	//从磁盘中读出单个kv.value序列话的对象，然后反序列化为kv.value
	bytes := make([]byte, position.Len)
	_, err := table.f.Seek(position.Start, 0)
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

func (table *SSTable) loadFileHandle() {
	if table.f == nil {
		f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			log.Println("error open file ", table.filePath)
			panic(err)
		}
		table.f = f
	}
	
	table.loadMetainfo()
	table.loadSparseIndex()
}

func (table *SSTable) loadSparseIndex() {
	f := table.f
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	_, err := f.Seek(table.tableMetaInfo.indexStart,0)
	if err != nil {
		log.Println("Failed to load sparseIndex! ", table.filePath)
		panic(err)
	}
	_, err = f.Read(bytes)
	if err != nil {
		log.Println("Failed to load sparseIndex! ", table.filePath)
		panic(err)
	}

	//反序列化
	table.sparseIndex = make(map[string]Position)
	err = json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		log.Println("Failed to load sparseIndex! ", table.filePath)
		panic(err)
	}

	table.sortIndex = make([]string, len(table.sparseIndex))
	for key := range table.sparseIndex {
		table.sortIndex = append(table.sortIndex, key)
	}
	sort.Strings(table.sortIndex)
}


func (table *SSTable) loadMetainfo() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		loadMetainfoHandler(err, table.filePath)
	}

	info, _ := f.Stat()

	_, err = f.Seek(-(info.Size() - 8*5), 0) //移动到文件的倒数第5*8个字节
	if err != nil {
		loadMetainfoHandler(err, table.filePath)
	}
	binary.Read(f, binary.LittleEndian, table.tableMetaInfo.version)

	_, err = f.Seek(-(info.Size() - 8*4), 0)
	if err != nil {
		loadMetainfoHandler(err, table.filePath)
	}
	binary.Read(f, binary.LittleEndian, table.tableMetaInfo.dataStart)

	_, err = f.Seek(-(info.Size() - 8*3), 0)
	if err != nil {
		loadMetainfoHandler(err, table.filePath)
	}
	binary.Read(f, binary.LittleEndian, table.tableMetaInfo.dataLen)

	_, err = f.Seek(-(info.Size() - 8*2), 0)
	if err != nil {
		loadMetainfoHandler(err, table.filePath)
	}
	binary.Read(f, binary.LittleEndian, table.tableMetaInfo.indexStart)

	_, err = f.Seek(-(info.Size() - 8*1), 0)
	if err != nil {
		loadMetainfoHandler(err, table.filePath)
	}
	binary.Read(f, binary.LittleEndian, table.tableMetaInfo.indexLen)
}

func loadMetainfoHandler(err error, file string) {
	log.Println("Failed to load meta info! ", file)
	panic(err)
}