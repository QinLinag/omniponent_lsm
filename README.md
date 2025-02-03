a lsm, write by goland

一.内存相关数据结构：
    1️⃣Value，就是KV对象
    功能：
        1.序列化与反序列化。

    2️⃣sortTree内存搜索树
    功能：
        1.插入KV、删除KV（软删除）
        2.内存树转化为内存只读表中的一颗内存树。
        3.根据key搜索value
        4.资源释放
    
    3️⃣database，lsm数据库。全局唯一。
    功能：
        1.启动数据库，初始化内存树、tableTree、只读表
        2.启动定时任务协程
        3.停止数据库，清理资源

二.磁盘相关数据结构：
    1️⃣一个sstable内存对象，对应一个磁盘文件。sstable持有文件路径、文件元信、每个kv.value在磁盘文件中的索引（map集合）
    功能：
        1.根据sstable磁盘文件名，加载文件数据初始化sstable对象。
        2.内存只读表中的一颗搜索二叉树转化为sstable对象（根据valuse初始化一个sstable对象（写入磁盘））。
        3.根据key，搜索value。
        4.资源释放。
    sstable磁盘文件格式：其中数据区和索引数据区序列化内存数据后写入， 元数据区通过binary二进制写入
            ![alt text](https://github.com/QinLinag/omniponent_lsm/blob/main/meta.jpg)
    2️⃣tableNode封装了sstable。tableTree的节点

    3️⃣tableTree用于维护多层tableNode链表
    功能：
        1.插入和删除tableNode。
        2.日志合并（数据合并到下一层）。
        3.lsm数据库启动时，加载所有的sstable磁盘文件，初始化sstable对象、tableNode对象，最后初始化tableTree对象。
        4.根据key搜索value。

    4️⃣Wal预写日志
    功能：
        1.通过内存树中的写操作
        2.lsm启动时加载所有的wal.log,初始化对应的sortTree（全部都是只读内存表中的树）
        3.资源释放、wal.log文件删除


        
