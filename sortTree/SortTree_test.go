package sortTree

import (
	"strconv"
	"testing"

	"github.com/QinLinag/omniponent_lsm/kv"

	"github.com/stretchr/testify/require"
)

func TestSortTree(t *testing.T) {
	//test1
	sortTree := NewSortTree()
	require.Equal(t, 0, sortTree.GetCount())
	_, kvResult := sortTree.Search("test")
	require.Equal(t, kv.None, kvResult)

	//test2
	for i := 0; i < 10000; i++ {
		value, _ := kv.Convert[string]("test")
		_, hasOld := sortTree.Insert(&kv.Value{
			Key:     strconv.Itoa(i) + "test",
			Value:   value,
			Deleted: false,
		})
		require.Equal(t, false, hasOld)
	}
	require.NotNil(t, sortTree.root)
	require.Equal(t, 10000, sortTree.GetCount())

	searchValue, kvResult := sortTree.Search(strconv.Itoa(5000) + "test")
	require.Equal(t, kv.Success, kvResult)
	require.NotNil(t, searchValue)

	//test3
	for i := 1000; i < 5000; i++ {
		key := strconv.Itoa(i) + "test"
		oldValue, hasOld := sortTree.Delete(key)
		require.Equal(t, true, hasOld)
		require.Equal(t, key, oldValue.Key)
	}
	require.Equal(t, 6000, sortTree.GetCount())

	for i := 1000; i < 5000; i++ {
		key := strconv.Itoa(i) + "test"
		searchValue, kvResult = sortTree.Search(key)
		require.Equal(t, kv.Deleted, kvResult)
	}

	//test4
	for i := 1000; i < 5000; i++ {
		value, _ := kv.Convert[string]("test")
		sortTree.Insert(&kv.Value{
			Key:     strconv.Itoa(i) + "test",
			Value:   value,
			Deleted: false,
		})
	}
	require.Equal(t, 10000, sortTree.GetCount())

	for i := 1000; i < 5000; i++ {
		key := strconv.Itoa(i) + "test"
		searchValue, kvResult = sortTree.Search(key)
		require.Equal(t, kv.Success, kvResult)
	}

	//test5
	for i := 0; i < 10000; i++ {
		value, _ := kv.Convert[string]("test-1")
		oldKV, hasOld := sortTree.Insert(&kv.Value{
			Key:     strconv.Itoa(i) + "test",
			Value:   value,
			Deleted: false,
		})
		require.Equal(t, true, hasOld)
		oldValue, _ := kv.Get[string](&oldKV)
		require.Equal(t, oldValue, "test")
	}
	require.Equal(t, 10000, sortTree.GetCount())

}
