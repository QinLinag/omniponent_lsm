package lsm

import (
	"context"
	"log"
	"time"

	config "github.com/QinLinag/omniponent_lsm/Config"
)

func Check(ctx context.Context) {
	con := config.GetConfig()
	ticker := time.NewTicker(time.Duration(con.CheckInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Println("Start to check...")
			checkMemory()              //内存检查
			database.TableTree.Check() //sstable文件检查
		case <-ctx.Done():
			log.Println("Check goroutine Stoped")
			return
		}
	}
}
func checkMemory() {
	count := database.MemTable.MemoryTree.GetCount()
	conf := config.GetConfig()
	if conf.Threshold > count {
		return
	}
	database.swap()
}

func CompressMemory(ctx context.Context) {
	log.Println("Start to compress memory...")
	con := config.GetConfig()
	ticker := time.NewTicker(time.Duration(con.CompressInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			for database.iMemTable.GetLen() != 0 { //一次性将所有的只读内存tree数据持久化为sstable
				table := database.iMemTable.GetAndDeleteTable()
				values := table.MemoryTree.GetValues()
				database.TableTree.CreateNewTable(values)
				table.Wal.DeleteFile()
			}
		case <-ctx.Done():
			log.Println("Compress memory goroutine stoped")
			return
		}
	}
}
