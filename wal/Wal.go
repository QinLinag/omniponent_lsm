package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/QinLinag/omniponent_lsm/kv"
	"github.com/QinLinag/omniponent_lsm/sortTree"
)




type Wal struct {
	f *os.File
	path string
	lock sync.Locker
}

//创建wal文件
func(w *Wal) Init(dir string) {
	log.Println("Creating wal.log...")
	start := time.Now()
	defer func() {
		elasp := time.Since(start)
		log.Println("the wal.log has been created,and the consumption of time: ", elasp)
	}()
	uuidStr := time.Now().Format("2006-01-02-15-04-05")
	walPath := path.Join(dir, fmt.Sprintf("%s_wal.go", uuidStr))

	f, err := os.OpenFile(walPath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Println("the wal.log file can't be created")
	}
	w.f = f
	w.path = walPath
	w.lock = &sync.Mutex{}
}

func(w *Wal) LoadFromFile(path string, tree *sortTree.Tree) *sortTree.Tree{
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("the wal.log file can't be open")
	}

	w.f = f
	w.path = path
	w.lock = &sync.Mutex{}
	return w.LoadToMemory(tree)
}

func(w *Wal) LoadToMemory(tree *sortTree.Tree) *sortTree.Tree {
	w.lock.Lock()
	defer w.lock.Unlock()

	preTree := sortTree.NewSortTree()
	info, _ := os.Stat(w.path)
	size := info.Size()

	if size == 0 {  //磁盘wal文件为空
		return preTree
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("Failed to move the file'pointer to start of the wal.log file")
		panic(err)
	}

	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Failed to move file'potinter to the end of the wal.log file!")
			panic(err)
		}
	}(w.f, size - 1, 0)

	data := make([]byte, size)
	newSize, err := w.f.Read(data)
	if err != nil || int64(newSize) != size {
		log.Println("Failed to read wal.log to memory")
		panic(err)
	}

	dataLen := int64(0)
	index := int64(0)
	for index < size {
		//每条数据前8个字节是元素长度
		indexData := data[index:(index + 8)]
		//读书该条数据的长度
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			log.Println("Failed to read per-data's len")
			panic(err)
		}
		
		//利用数据长度，将该条数据读出，并反序列化为kv.Value对象
		index += 8
		dataArea := data[index:(index + dataLen)]
		value, err := kv.Decode(dataArea)
		if err != nil {
			log.Println("Failed to unmarshal binary data to kv.value")
			panic(err)
		}

		if value.Deleted {
			tree.Delete(value.Key)
			preTree.Delete(value.Key)
		} else {
			tree.Insert(&value)
			preTree.Insert(&value)
		}
		index += dataLen
	}
	return preTree
}

//记录日志
func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	
}
