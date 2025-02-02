package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	config "github.com/QinLinag/omniponent_lsm/config"
	"github.com/QinLinag/omniponent_lsm/kv"
)

type SSTable struct {
	f        *os.File
	filePath string

	tableMetaInfo MetaInfo

	//稀疏索引列表   在内存中
	sparseIndex map[string]Position

	//内存中排序的key
	sortIndex []string

	//sstable 虽然不需要修改，但是有合并的过程，所以需要加锁
	lock *sync.RWMutex
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.lock = &sync.RWMutex{}
	table.loadFileHandle()
}

// 获得sstable磁盘文件的总大小
func (table *SSTable) getSSTableSize() int64 {
	table.lock.RLock()
	defer table.lock.RUnlock()
	info, err := table.f.Stat()
	if err != nil {
		log.Println("Failed to get SSTable size!")
		panic(err)
	}
	return info.Size()
}

/*
磁盘sstable文件信息加载如内存sstable对象模块
*/
func loadMetainfoHandler(err error, file string) {
	log.Println("Failed to load meta info! ", file)
	panic(err)
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
	_, err := f.Seek(table.tableMetaInfo.indexStart, 0)
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

// 从sstable中找到 kv.value对象
func (table *SSTable) Search(key string) (kv.Value, kv.SearchResult) {
	table.lock.RLock()
	defer table.lock.RUnlock()

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
		NewSSTableWithValuesErrHandler(err)
	}

	_, err = table.f.Read(bytes)
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}
	return value, kv.Success
}

// 根据values创建一个新的sstable内存对象以及磁盘文件
func NewSSTableWithValues(values []kv.Value, level int, index int) *SSTable {
	//文件数据准备（序列化数据区、索引数据、元数据）
	values_bytes := make([]byte, 0)
	positions := make(map[string]Position)
	keys := make([]string, 0)
	dataLen := int64(0)
	for _, value := range values {
		bytes, err := kv.Encode(value)
		if err != nil {
			log.Println("Failed to insert key: ", value.Key)
			continue
		}
		keys = append(keys, value.Key)
		position := Position{
			Start:   dataLen,
			Len:     int64(len(bytes)),
			Deleted: value.Deleted,
		}
		positions[value.Key] = position
		values_bytes = append(values_bytes, bytes...)
		dataLen += int64(len(bytes))
	}
	sort.Strings(keys)
	positions_bytes, err := json.Marshal(positions)
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}

	meta := MetaInfo{ //meta不需要序列化，字节binary写入
		version:    0,
		dataStart:  0,
		dataLen:    dataLen,
		indexStart: dataLen,
		indexLen:   int64(len(positions_bytes)),
	}

	//创建文件，并且写入数据   其中呢数据区、索引区数据直接序列化写入，元数据区通过二进制写入
	filePath := config.GetConfig().DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}

	_, err = f.Write(values_bytes)
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}
	_, err = f.Write(positions_bytes)
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}
	binary.Write(f, binary.LittleEndian, &meta.version)
	binary.Write(f, binary.LittleEndian, &meta.dataStart)
	binary.Write(f, binary.LittleEndian, &meta.dataLen)
	binary.Write(f, binary.LittleEndian, &meta.indexStart)
	binary.Write(f, binary.LittleEndian, &meta.indexLen)
	err = f.Sync()
	if err != nil {
		NewSSTableWithValuesErrHandler(err)
	}
	newSSTable := SSTable{
		f:             f,
		tableMetaInfo: meta,
		filePath:      filePath,
		sparseIndex:   positions,
		sortIndex:     keys,
		lock:          &sync.RWMutex{},
	}
	return &newSSTable
}
func NewSSTableWithValuesErrHandler(err error) {
	log.Println("Failed to NewSSTable!")
	panic(err)
}
