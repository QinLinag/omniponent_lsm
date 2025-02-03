package ssTable


//不需要序列化，直接二进制写入，故不需要大写
type MetaInfo struct {
	version int64
	//数据区起始地址  0
	dataStart int64
	//数据区长度
	dataLen int64
	//索引区起始地址
	indexStart int64
	//索引区长度
	indexLen int64
}

func newMetaInfo(dataLen int64, indexLen int64) *MetaInfo{
	return &MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    dataLen,
		indexStart: dataLen,
		indexLen:   indexLen,
	}
}
