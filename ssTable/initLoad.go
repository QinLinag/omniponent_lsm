package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"sort"
)

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