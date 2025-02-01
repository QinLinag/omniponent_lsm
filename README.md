a lsm, write by goland


1️⃣一个sstable内存对象，对应一个磁盘文件（只读）。sstable持有文件路径、文件元信、每个kv.value在磁盘文件中的索引（map集合）
    对应接口：
        search：从sstable对应的磁盘文件中找一个kv.value
        loadMetaInfo：从磁盘文件中读取文件元信息
        loadSparseIndex：从磁盘文件中读取kv.value的所有索引信息
        NewSSTableWithValues(非结构体专有)：根据values创建一个新的sstable内存对象以及磁盘文件
        
        

2️⃣tableNode封装了sstable--->>>一个链表，代表一层

3️⃣tableTree用于维护多层tableNode链表
    对应接口：
        createTable：创建一个新的sstable和tableNode，并将tableNode插入tableTree对应的层
        
