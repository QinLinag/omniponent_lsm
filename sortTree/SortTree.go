package sortTree

import (
	"log"
	"sync"

	"github.com/QinLinag/omniponent_lsm/kv"
)

type treeNode struct {
	KV    *kv.Value
	Left  *treeNode
	Right *treeNode
}

// lsm中，常驻内存的是一颗排序二叉树     二叉排序树通过key进行排序
type Tree struct {
	root   *treeNode
	count  int
	rwLock *sync.RWMutex
}

func NewSortTree() *Tree {
	tree := Tree{}
	tree.Init()
	return &tree
}

func (tree *Tree) Init() {
	tree.root = nil
	tree.count = 0
	tree.rwLock = &sync.RWMutex{}
}

func (tree *Tree) GetCount() int {
	return tree.count
}

func (tree *Tree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.rwLock.RLock() //读锁
	defer tree.rwLock.RUnlock()

	if tree.root == nil {
		return kv.Value{}, kv.None
	}
	//排序树查找
	current := tree.root

	for current != nil {
		if current.KV.Key == key {
			if current.KV.Deleted {
				return kv.Value{}, kv.Deleted
			} else {
				return *current.KV, kv.Success
			}
		} else if current.KV.Key < key {
			current = current.Right
		} else {
			current = current.Left
		}
	}
	return kv.Value{}, kv.None
}

func (tree *Tree) InsertByKeyAndBytes(key string, value []byte) (kv.Value, bool) {
	kv := &kv.Value{
		Key:     key,
		Value:   value,
		Deleted: false,
	}
	return tree.Insert(kv)
}

func (tree *Tree) InsertByKeyAndValue(key string, value any) (kv.Value, bool, error) {
	value_bytes, err := kv.Convert(value)
	if err != nil {
		return kv.Value{}, false, err
	}
	oldValue, hasOld := tree.InsertByKeyAndBytes(key, value_bytes)
	return oldValue, hasOld, nil
}

// 如果有旧值，就返回旧值
func (tree *Tree) Insert(keyValue *kv.Value) (kv.Value, bool) {
	newNode := treeNode{
		KV:    keyValue,
		Left:  nil,
		Right: nil,
	}

	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	current := tree.root
	if current == nil && tree.count == 0 { //空树
		tree.count++
		tree.root = &newNode
		return kv.Value{}, false
	}

	for current != nil {
		if current.KV.Key == newNode.KV.Key {
			if current.KV.Deleted { //已经被删了
				current.KV.Deleted = false
				current.KV.Value = keyValue.Value
				tree.count++
				return kv.Value{}, false
			} else {
				oldKV := current.KV.Copy()
				current.KV.Value = keyValue.Value
				return *oldKV, true
			}
		} else if current.KV.Key < newNode.KV.Key {
			if current.Right == nil {
				current.Right = &newNode
				tree.count++
				return kv.Value{}, false
			}
			current = current.Right
		} else {
			if current.Left == nil {
				current.Left = &newNode
				tree.count++
				return kv.Value{}, false
			}
			current = current.Left
		}
	}

	log.Fatal("insert fatal")
	return kv.Value{}, false
}

// 存在就删除，并返回
func (tree *Tree) Delete(key string) (kv.Value, bool) {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	current := tree.root

	for current != nil {
		if current.KV.Key == key {
			if !current.KV.Deleted {
				oldKV := current.KV.Copy()
				tree.count--
				current.KV.Deleted = true
				current.KV.Value = nil
				return *oldKV, true
			} else {
				return kv.Value{}, false
			}
		} else if current.KV.Key < key {
			current = current.Right
		} else {
			current = current.Left
		}
	}
	return kv.Value{}, false
}

func (tree *Tree) GetValues() []kv.Value {
	tree.rwLock.RLock()
	defer tree.rwLock.RUnlock()

	//利用非递归方式
	stack := InitialStack(tree.count / 2)
	current := tree.root
	values := make([]kv.Value, 0)
	for {
		if current != nil {
			stack.Push(current)
			current = current.Left
		} else {
			popNode, succ := stack.Pop()
			if !succ {
				break
			}
			values = append(values, *popNode.KV)
			current = popNode.Right
		}
	}
	return values
}

func (tree *Tree) Swap() *Tree {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	newTree := &Tree{}
	newTree.Init()

	tree.root = nil
	tree.count = 0
	tree.rwLock = nil
	return newTree
}
