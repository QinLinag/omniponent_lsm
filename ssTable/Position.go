package ssTable

// 需要序列化，故必须大写
type Position struct {
	//磁盘KV其实地址
	Start int64
	//磁盘KV长度
	Len int64
	//是否已经删除
	Deleted bool
}
