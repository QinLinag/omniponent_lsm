package lsm

import (
	"context"
	"log"
	"os"
	"time"

	config "github.com/QinLinag/omniponent_lsm/Config"
	"github.com/QinLinag/omniponent_lsm/ssTable"
)

type Database struct {
	//内存二叉排序树
	MemTable *MemTable
	//内存只读表
	iMemTable *ReadOnlyMemTable
	//sstable磁盘文件树
	TableTree *ssTable.TableTree
	//控制子协程的上下文、以及cancel函数
	ctx       context.Context
	cancelFun context.CancelFunc
}

// lsm全局唯一实例
var database *Database

/*
全局暂停
*/
func Stop() {

	//子协程结束
	database.cancelFun()
	//资源释放
	database.MemTable.Clear()
	database.TableTree.Clear()

	//将只读内存表中数据写入磁盘
	for database.iMemTable.GetLen() != 0 { //一次性将所有的只读内存tree数据持久化为sstable
		table := database.iMemTable.GetAndDeleteTable()
		values := table.MemoryTree.GetValues()
		database.TableTree.CreateNewTable(values)
		table.Wal.DeleteFile()
	}
}

/*
全局启动
*/
func Start(con config.Config) {
	log.Println("lsm-database is starting...")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Success to start database, and is cost: ", elapse)
	}()
	if database != nil {
		return
	}
	config.Init(con)
	//初始化database
	initDatabase()
	//定时任务启动，
	go Check(database.ctx)
	go CompressMemory(database.ctx)
}
func initDatabase() {
	config := config.GetConfig()
	dir := config.DataDir
	// 如果目录不存在，则为空数据库
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.MkdirAll(dir, 0700) //创建一个目录
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}
	//初始化database，并且加载文件
	log.Println("Loading files...")
	ctx, cancelFun := context.WithCancel(context.Background())
	database = &Database{
		ctx:       ctx,
		cancelFun: cancelFun,
	}
	//初始化database，并加载数据
	database.TableTree = ssTable.NewTableTree()
	database.iMemTable = NewReadOnlyMemTable()
	database.MemTable = NewMemTree()
}

/*
内存树转化为只读
*/
func (d *Database) swap() {
	table := d.MemTable.Swap()
	d.iMemTable.Insert(table)
}

/*
database对外开放的接口
*/
