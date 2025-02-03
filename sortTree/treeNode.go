package sortTree

import "github.com/QinLinag/omniponent_lsm/kv"

// 内存树中的节点：持有KV
type treeNode struct {
	kv    *kv.Value
	left  *treeNode
	right *treeNode
}
