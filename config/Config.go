package config

import "sync"


type Config struct {
    //数据目录
    DataDir string
    //0层sstable文件总大小
    Level0Size int
    //每一层sstable表数量阀值
    PartSize int
    //内存表的kv最大数量
    Threshold int
    //检查内存树大小的时间间隔，如果超出就放入iMemTable
    CheckInterval int
    //压缩内存时间间隔
    CompressInterval int
}

var once *sync.Once = &sync.Once{}

var config Config

func Init(con Config) {
    once.Do(func () {
        config = con
    })
}

func GetConfig() Config{
    return config
}
