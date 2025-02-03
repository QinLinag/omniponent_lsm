package ssTable

import "fmt"

//tableTree每一层都是一个链表, tableNode是节点，持有sstable
type tableNode struct {
	index int
	table *SSTable
	next *tableNode
}


/*
功能性接口
*/
//从一个db文件名中获得所在层数和index
func getLevelAndIndex(name string) (int, int, error) {
	level := 0
	index := 0
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n!= 2 || err != nil {
		return -1, -1, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}