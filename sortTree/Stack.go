package sortTree

import "log"

const (
	DEFAULTSTACKSIZE = 1000
)

type Stack struct { //无限增长的stack
	stack []*treeNode
	top   int //栈顶元素的index  初始为-1，表示没有元素
}

func InitialStack(size int) *Stack {
	if size <= 0 {
		size = DEFAULTSTACKSIZE
	}
	return &Stack{
		stack: make([]*treeNode, size),
		top:   -1,
	}
}

func (s *Stack) FreeStack() {
	s.top = -1
	s.stack = s.stack[:0]
}

func (s *Stack) Push(node *treeNode) {
	if s.top != len(s.stack)-1 {
		s.top++
		s.stack[s.top] = node
	} else {
		s.stack = append(s.stack, node)
		s.top++
	}
}

func (s *Stack) Pop() (*treeNode, bool) {
	if s.top == -1 {
		return nil, false
	}
	node := s.stack[s.top]
	if node == nil {
		log.Fatal("node should not be nil")
	}
	s.top--
	return node, true
}
