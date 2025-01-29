package sortTree

const (
	DEFAULTSTACKSIZE = 1000
)


type Stack struct {  //无限增长的stack
	stack []*treeNode
	top int
}


func InitialStack(size int) Stack{
	if size <= 0 {
		size = DEFAULTSTACKSIZE
	}
	return Stack{
		stack: make([]*treeNode, size),
		top: -1,
	}
}

func(s *Stack) Push(node *treeNode) {
	if s.top == len(s.stack) {
		s.top++
		s.stack[s.top] = node
	} else {
		s.stack = append(s.stack, node)
		s.top++
	}
}

func(s *Stack) Pop() (*treeNode, bool) {
	if s.top == -1 {
		return nil, false
	}
	node := s.stack[s.top]
	s.stack[s.top] = nil
	s.top--
	return node, true
}



