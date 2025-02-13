package ssTable

import (
	"log"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	config "github.com/QinLinag/omniponent_lsm/Config"
	"github.com/QinLinag/omniponent_lsm/kv"
	"github.com/QinLinag/omniponent_lsm/sortTree"
)

type TableTree struct {
	levels []*tableNode
	lock   *sync.RWMutex
}

// 字节为单位
var levelMaxSize []int

/*
TableTree初始化模块:需要将目录下的所有sstable磁盘文件文件用于初始化sstable，构成一个tableTree
*/
func NewTableTree() *TableTree {
	tableTree := TableTree{}
	tableTree.init()
	return &tableTree
}
func (tree *TableTree) init() {
	log.Println("The SSTable list are being loaded")
	conf := config.GetConfig()
	dir := conf.DataDir
	//目前tabletree最多十层，每一层的所有数据比上一层大10倍，
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = conf.Level0Size
	levelMaxSize[1] = levelMaxSize[0] * 10
	levelMaxSize[2] = levelMaxSize[1] * 10
	levelMaxSize[3] = levelMaxSize[2] * 10
	levelMaxSize[4] = levelMaxSize[3] * 10
	levelMaxSize[5] = levelMaxSize[4] * 10
	levelMaxSize[6] = levelMaxSize[5] * 10
	levelMaxSize[7] = levelMaxSize[6] * 10
	levelMaxSize[8] = levelMaxSize[7] * 10
	levelMaxSize[9] = levelMaxSize[8] * 10

	tree.levels = make([]*tableNode, 10)
	tree.lock = &sync.RWMutex{}
	//加载sstable磁盘文件
	infos, err := os.ReadDir(dir)
	if err != nil {
		log.Println("Failed to init tableTree, and the diractory is: ", dir)
		panic(err)
	}

	files := make([]string, len(infos))
	for i, info := range infos {
		files[i] = info.Name()
	}
	sort.Strings(files)
	for _, fileName := range files {
		if path.Ext(fileName) == ".db" {
			tree.loadDbFile(dir, fileName)
		}
	}
}
func (tree *TableTree) loadDbFile(dir string, fileName string) {
	table := newSSTableFromFile(path.Join(dir, fileName))
	level, _, err := getLevelAndIndex(fileName)
	if err != nil {
		log.Println("Failed to init tableTree, and the diractory is: ", dir)
		panic(err)
	}
	tree.insert(table, level)
}

/*
tableTree 搜索模块
*/
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
			value, result := tables[i].search(key)
			if kv.IsNone(result) { //没找到就下一个table
				continue
			}
			return value, result //可能是deleted/success
		}
	}
	return kv.Value{}, kv.None
}

/*
创建tableNode并插入tableTree模块
*/
// 内存满了后，创建一个新的tableNode插入tree第一层
func (tree *TableTree) CreateNewTable(values []kv.Value) *SSTable {
	return tree.createTable(values, 0)
}

// 根据values创建一个tableNode插入到Tabletree
func (tree *TableTree) createTable(values []kv.Value, level int) *SSTable {
	//返回sstable内存对象
	index := tree.getMaxIndexFromCertainLevel(level)
	newSSTable := newSSTableWithValues(values, level, index+1)
	//插入tableNode节点
	tree.insert(newSSTable, level)
	return newSSTable
}

// 插入一个sstable到tableTree的指定层链表最后一个位置，并返回对应的index
func (tree *TableTree) insert(table *SSTable, level int) int {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	if level >= len(tree.levels) { //代表level不合法
		return -1
	}
	newNode := tableNode{
		index: 0,
		table: table,
		next:  nil,
	}
	node := tree.levels[level]

	if node == nil { //该层还没有节点
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

/*
文件压缩模块   一层一层压缩至下一层
*/
func (tree *TableTree) Check() {
	tree.majorCompaction()
}

// 循环便利每一层，判断是否需要压缩
func (tree *TableTree) majorCompaction() {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	con := config.GetConfig()
	for level := range tree.levels {
		tablesSize := int(tree.getLevelSize(level) / 1024 / 1024) //mb
		if tree.getCountFromCertainLevel(level) > con.PartSize || tablesSize > levelMaxSize[level] {
			tree.compactionCertainLevel(level)
		}
	}
}

// 对某一层进行压缩：基本思路，将某一层的sstable磁盘文件加载到内存，构成一颗sortTree，然后将sortTree转化为sstable，插入下一层
func (tree *TableTree) compactionCertainLevel(level int) {
	log.Println("Compressing start,level: ", level)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Compressing finish, and it spent: ", elapse)
	}()
	currentNode := tree.levels[level]
	tempMemorySortTree := sortTree.NewSortTree()

	//将该层的所有sstable磁盘数据区加载到内存中，并构成一颗新sortTree
	values_bytes := make([]byte, levelMaxSize[level])

	tree.lock.Unlock()
	for currentNode != nil {
		table := currentNode.table
		if table.tableMetaInfo.dataLen > int64(levelMaxSize[level]) {
			values_bytes = make([]byte, table.tableMetaInfo.dataLen)
		}
		values_bytes = values_bytes[0:table.tableMetaInfo.dataLen]

		_, err := table.f.Seek(0, 0)
		if err != nil {
			log.Println("Failed to compaction, level is : ", level, "file is: ", table.filePath)
			panic(err)
		}
		_, err = table.f.Read(values_bytes)
		if err != nil {
			log.Println("Failed to compaction, level is : ", level, "file is: ", table.filePath)
			panic(err)
		}

		for key, position := range table.sparseIndex {
			if !position.Deleted {
				value_bytes := values_bytes[position.Start:(position.Start + position.Len)]
				value, err := kv.Decode(value_bytes)
				if err != nil {
					log.Println("Failed to compaction, level is : ", level, "file is: ", table.filePath)
					continue
				}
				tempMemorySortTree.Insert(&value)
			} else {
				tempMemorySortTree.Delete(key)
			}
		}
		currentNode = currentNode.next
	}
	tree.lock.Unlock()

	//将新的sortTree转化为一个sstable，并且插入到下一层
	values := tempMemorySortTree.GetValues()
	newLevel := level + 1
	if level > 10 { //最多十层
		newLevel = 10
	}
	tree.createTable(values, newLevel)

	//重置该层
	oldNode := tree.levels[level]
	if level < 10 {
		tree.levels[level] = nil
		tree.clearLevel(oldNode)
	}
}

/*
清理模块、资源释放
*/
//清理整个tableTree:sstable文件关闭、资源释放
func (tree *TableTree) Clear() {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	tree.lock = nil
	for _, node := range tree.levels {
		for node != nil {
			tempNode := node
			tempNode.table.clear()
			node = node.next
		}
	}
}

// 重置某个tableNode及其以后node节点（删除node、sstable、磁盘文件）
func (tree *TableTree) clearLevel(node *tableNode) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	for node != nil {
		table := node.table
		err := table.f.Close()
		if err != nil {
			log.Println("Failed to compaction, level is : ", "file is: ", table.filePath)
			continue
		}
		err = os.Remove(table.filePath)
		if err != nil {
			log.Println("Failed to compaction, level is : ", "file is: ", table.filePath)
			continue
		}
		table.f = nil
		node.table = nil
		node = node.next
	}
}

/*
tableTree 功能接口
*/
//获得某层最大index
func (tree *TableTree) getMaxIndexFromCertainLevel(level int) int {
	tree.lock.RLock()
	defer tree.lock.Unlock()
	if level >= len(tree.levels) { //非法层
		return -1
	}
	node := tree.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}

// 获得某层节点数
func (tree *TableTree) getCountFromCertainLevel(level int) int {
	tree.lock.RLock()
	defer tree.lock.Unlock()
	if level >= len(tree.levels) { //非法层
		return -1
	}
	node := tree.levels[level]
	count := 0

	for node != nil {
		count++
		node = node.next
	}
	return count
}

// 获得某层所有sstable磁盘文件大小总和
func (tree *TableTree) getLevelSize(level int) int64 {
	if level > len(tree.levels) { //非法level
		return -1
	}
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	node := tree.levels[level]
	size := int64(0)
	for node != nil {
		size += node.table.getSSTableSize()
		node = node.next
	}
	return size
}
