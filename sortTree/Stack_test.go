package sortTree

import (
	"strconv"
	"testing"

	"github.com/QinLinag/omniponent_lsm/kv"
	"github.com/stretchr/testify/require"
)

func TestStack(t *testing.T) {
	stack := InitialStack(100)
	require.Equal(t, 100, len(stack.stack), "initial stack's len should be 100")
	require.Equal(t, -1, stack.top, "stack's member should be zero")

	for i := 0; i < 50; i++ {
		value_bytes, _ := kv.Convert[string]("test")
		stack.Push(&treeNode{
			KV: &kv.Value{
				Key:     strconv.Itoa(i) + "test",
				Value:   value_bytes,
				Deleted: false,
			},
		})
	}
	require.Equal(t, 49, stack.top)

	for i := 0; i < 40; i++ {
		deleteNode, has := stack.Pop()
		require.Equal(t, true, has, "pop should be successful")
		require.NotNil(t, deleteNode, "the member that poped should not be nil")
	}
	require.Equal(t, 9, stack.top)

	for i := 0; i < 10; i++ {
		deleteNode, has := stack.Pop()
		require.Equal(t, true, has, "pop should be successful")
		require.NotNil(t, deleteNode, "the member that poped should not be nil")
	}
	require.Equal(t, -1, stack.top, "stack should be nil")

	for i := 0; i < 200; i++ {
		value_bytes, _ := kv.Convert[string]("test")
		stack.Push(&treeNode{
			KV: &kv.Value{
				Key:     strconv.Itoa(i) + "test",
				Value:   value_bytes,
				Deleted: false,
			},
		})
	}
	require.Equal(t, 199, stack.top)

	for i := 0; i < 100; i++ {
		deleteNode, has := stack.Pop()
		require.Equal(t, true, has, "pop should be successful")
		require.NotNil(t, deleteNode, "the member that poped should not be nil")
	}
	require.Equal(t, 99, stack.top)

	for i := 0; i < 100; i++ {
		deleteNode, has := stack.Pop()
		require.Equal(t, true, has, "pop should be successful")
		require.NotNil(t, deleteNode, "the member that poped should not be nil")
	}
	require.Equal(t, -1, stack.top, "stack should be nil")

	for i := 0; i < 100; i++ {
		deleteNode, has := stack.Pop()
		require.Equal(t, false, has, "pop should be false")
		require.Nil(t, deleteNode, "the member that poped should be nil")
	}
	require.Equal(t, -1, stack.top)

	stack.FreeStack()
	require.Equal(t, 0, len(stack.stack))
}
