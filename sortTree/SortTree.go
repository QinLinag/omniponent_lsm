package sortTree

import (
	"log"
	"sync"

	"github.com/QinLinag/omniponent_lsm/kv"
)

// lsm中，常驻内存的是一颗排序二叉树     二叉排序树通过key进行排序
type Tree struct {
	root   *treeNode
	count  int
	rwLock *sync.RWMutex
}

/*
sortTree初始化模块
*/
func NewSortTree() *Tree {
	tree := Tree{}
	tree.init()
	return &tree
}
func (tree *Tree) init() {
	tree.root = nil
	tree.count = 0
	tree.rwLock = &sync.RWMutex{}
}

/*
功能性接口模块
*/
func (tree *Tree) GetCount() int {
	return tree.count
}

/*
搜索模块
*/
func (tree *Tree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.rwLock.RLock()
	defer tree.rwLock.RUnlock()

	if tree.root == nil {
		return kv.Value{}, kv.None
	}

	//排序树查找
	current := tree.root
	for current != nil {
		if current.kv.GetKey() == key {
			if current.kv.Isdeleted() {
				return kv.Value{}, kv.Deleted
			} else {
				return *current.kv, kv.Success
			}
		} else if current.kv.GetKey() < key {
			current = current.right
		} else {
			current = current.left
		}
	}
	return kv.Value{}, kv.None
}

/*
插入kv模块
*/
func (tree *Tree) InsertByKeyAndBytes(key string, value []byte) (kv.Value, bool) {
	KV := kv.NewValue(key, value)
	return tree.Insert(KV)
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
		kv:    keyValue,
		left:  nil,
		right: nil,
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
		if current.kv.GetKey() == newNode.kv.GetKey() {
			if current.kv.Isdeleted() { //已经被删了
				current.kv.SetDeleted(false)
				current.kv.SetValue(keyValue.GetValue())
				tree.count++
				return kv.Value{}, false
			} else {
				oldkv := current.kv.Copy()
				current.kv.SetValue(keyValue.GetValue())
				return *oldkv, true
			}
		} else if current.kv.GetKey() < newNode.kv.GetKey() {
			if current.right == nil {
				current.right = &newNode
				tree.count++
				return kv.Value{}, false
			}
			current = current.right
		} else {
			if current.left == nil {
				current.left = &newNode
				tree.count++
				return kv.Value{}, false
			}
			current = current.left
		}
	}

	log.Fatal("insert fatal")
	return kv.Value{}, false
}

/*
删除kv模块
*/
// 存在就删除，并返回
func (tree *Tree) Delete(key string) (kv.Value, bool) {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	current := tree.root

	for current != nil {
		if current.kv.GetKey() == key {
			if !current.kv.Isdeleted() {
				oldkv := current.kv.Copy()
				tree.count--
				current.kv.SetDeleted(true)
				current.kv.SetValue(nil)
				return *oldkv, true
			} else {
				return kv.Value{}, false
			}
		} else if current.kv.GetKey() < key {
			current = current.right
		} else {
			current = current.left
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
			current = current.left
		} else {
			popNode, succ := stack.Pop()
			if !succ {
				break
			}
			values = append(values, *popNode.kv)
			current = popNode.right
		}
	}
	return values
}

/*
sortTree转化为内存只读表中的一棵树
*/
func (tree *Tree) Swap() *Tree {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	newTree := NewSortTree()
	newTree.root = tree.root
	newTree.count = tree.count
	//内存树reset
	tree.reset()
	return newTree
}
func (tree *Tree) reset() {
	tree.root = nil
	tree.count = 0
}

/*
清理、资源释放模块
*/
func (tree *Tree) Clear() {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()
	tree.root = nil
	tree.count = 0
}
