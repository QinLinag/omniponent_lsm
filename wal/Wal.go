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

	config "github.com/QinLinag/omniponent_lsm/Config"
	"github.com/QinLinag/omniponent_lsm/kv"
	"github.com/QinLinag/omniponent_lsm/sortTree"
)

type Wal struct {
	f    *os.File
	path string
	lock *sync.Mutex
	dir  string
}

/*
为内存表创建全新的wal文件、wal对象
*/
func NewWal(tree *sortTree.Tree) *Wal {
	conf := config.GetConfig()
	dir := conf.DataDir
	wal := Wal{}
	wal.lock = &sync.Mutex{}
	wal.dir = dir
	wal.CreateWal(dir) //没有log文件，创建新的
	return &wal
}
func (w *Wal) CreateWal(dir string) {
	log.Println("Creating wal.log...")
	walPath, f := CreateNewWalFileHandler(dir)
	w.f = f
	w.path = walPath
}
func CreateNewWalFileHandler(dir string) (string, *os.File) {
	uuidStr := time.Now().Format("2006-01-02-15-04-05")
	walPath := path.Join(dir, fmt.Sprintf("%s_wal.go", uuidStr))
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("the wal.log file can't be created")
	}
	return walPath, f
}

/*
database启动时，加载wal.log文件到内存，初始化wal对象
*/
func (w *Wal) LoadFromFile(path string) *sortTree.Tree {
	log.Println("Loading wal.log file...")
	start := time.Now()
	defer func() {
		elaps := time.Since(start)
		log.Println("wal.log file has been loaded, it spent time: ", elaps)
	}()

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("the wal.log file can't be open")
	}
	conf := config.GetConfig()
	w.dir = conf.DataDir
	w.f = f
	w.path = path
	w.lock = &sync.Mutex{}
	return w.LoadToMemory()
}
func (w *Wal) LoadToMemory() *sortTree.Tree {
	w.lock.Lock()
	defer w.lock.Unlock()

	preTree := sortTree.NewSortTree()
	info, _ := os.Stat(w.path)
	size := info.Size()

	if size == 0 { //磁盘wal文件为空
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
	}(w.f, size-1, 0)

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

		if value.Isdeleted() { //越往后面，数据时越新的，就要删除
			preTree.Delete(value.GetKey())
		} else {
			preTree.Insert(&value)
		}
		index += dataLen
	}
	return preTree
}

/*
记录日志
*/
func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	dataArea, err := kv.Encode(value)
	if err != nil {
		log.Println("Failed to marshal kv.value")
		panic(err)
	}
	//将value序列话后的长度(将长度转为8个字节的int后写入)，写入wal.log中
	dataLen := len(dataArea)
	err = binary.Write(w.f, binary.LittleEndian, int64(dataLen))
	if err != nil {
		log.Println("Failed to write value's len")
		panic(err)
	}

	err = binary.Write(w.f, binary.LittleEndian, dataArea)
	if err != nil {
		log.Println("Failed to write value")
		panic(err)
	}
}

/*
删除原有的wal.log文件，创建一个新的wal.log文件
*/
func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.DeleteFile()
	walPath, f := CreateNewWalFileHandler(w.dir)
	w.f = f
	w.path = walPath
}

/*
删除log文件、资源释放
*/
func (w *Wal) DeleteFile() {
	w.lock.Lock()
	defer w.lock.Unlock()
	log.Println("Deleting wal.log file")
	err := w.f.Close()
	if err != nil {
		log.Println("Failed to close wal.log file")
		panic(err)
	}

	err = os.Remove(w.path)
	if err != nil {
		log.Println("Failed to remove wal.log file")
		panic(err)
	}
	w.f = nil
}
// database退出时，释放资源，但是不需要删除wal.log文件
func (w *Wal) Clear() {
	w.lock.Lock()
	defer w.lock.Unlock()
	if err := w.f.Close(); err != nil {
		log.Println("Failed to clear wal, file is: ", w.path)
	}
	w.f = nil
}
