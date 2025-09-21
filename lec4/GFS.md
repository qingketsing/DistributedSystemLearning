# Lec4 & GFS 

## 论文部分: GFS

### GFS介绍

GFS是一个大型的分布式文件系统，主要目的是在大量廉价商用设备上满足存储需求。

### GFS设计

#### 接口

基本操作: create, delete, open, close, read, write

两个关键操作:
1. 快照(SnapShot): 以低成本创建文件或目录树的副本
2. 记录追加(Record Append): 它允许多个客户端并发地、原子地向同一个文件追加数据，而无需它们之间进行复杂的同步

#### 架构

三个主要组成部分:
1. Master: 管理所有文件系统元数据，协调系统活动(块租约管理、垃圾回收、块迁移等)，定期心跳连接检测ChunkServer是否存活。但是客户端从不通过Master读写文件数据，Master只提供元数据，以避免成为瓶颈。
2. ChunkServer: 将每个块（Chunk）作为一个普通的Linux文件存储在本地磁盘上，默认情况下，每个块会在多个不同的Chunkserver（跨机架） 上存储3个副本。
3. Client: 实现文件系统API，与Master交互进行元数据操作，直接从Chunkserver读写数据

一个简单的读取流程: 
1. Client将(文件名, 字节偏移)转换为(文件名, 块索引)。
2. Client向Master询问该块所在的Chunkserver。
3. Master回复该块的句柄和所有副本的位置。
4. Client缓存这些信息。
5. Client从最近的一个副本（通常是其中之一）请求数据，指定(块句柄, 字节范围)。
6. Chunkserver将数据返回给客户端。

#### 单Master设计

只设计了一个Master，同时只存储元数据，包含文件相关的内容，但是不包含文件本身的内容。最小化master节点在读写中的参与，以避免其成为系统瓶颈。

#### Chunk大小

一般设置为64MB，其远大于通常的文件系统的块大小。

对于超出64MB的文件，GFS会自动将该文件分割成多个chunk进行存储。GFS客户端库在收到应用程序的写入请求时，会根据固定的64MB chunk大小，将文件在逻辑上划分为一个连续的chunk序列。当需要读写某个特定偏移量的数据时，客户端会通过一个简单的计算将(文件名, 字节偏移量)转换为(文件名, chunk索引)。例如，一个128MB的文件会被分割为chunk 0（0-64MB）和chunk 1（64MB-128MB）。

Master如何管理多chunk文件？

Master在内存中维护着文件到chunk的映射关系。这个映射通常是一个列表或数组，记录着：

文件/data/largefile.log

对应 chunk 句柄列表: [chunk_handle_123, chunk_handle_456, chunk_handle_789, ...]

每个chunk句柄都指向一组存储该chunk副本的Chunkserver地址。


写入流程（以一个大文件的连续写入为例）：

1. 客户端告诉Master要写入文件/data/largefile.log的起始位置。
2. Master查看该文件的chunk映射列表。
3. 如果写入操作覆盖了最后一个chunk的一部分，Master会将该chunk的租约授予一个Primary Chunkserver。
4. 如果写入操作超出了最后一个chunk的末尾，Master会创建一个新的chunk，将其添加到文件的chunk列表中，并为这个新chunk选择初始的副本位置，然后授予租约。
5. 客户端从Master获取到当前应该写入的chunk（可能是最后一个chunk或一个新chunk）的Primary和Secondary副本的位置。
6. 客户端开始向这个chunk的所有副本推送数据。当这个chunk被写满（接近64MB）后，流程重复：客户端联系Master，Master分配下一个新chunk，如此往复，直到所有数据写完。

读取流程：

1. 客户端想读取文件/data/largefile.log中从偏移量70MB开始的1MB数据。
2. 客户端进行计算：chunk_index = (70MB / 64MB) = 1（索引从0开始）。要读取的数据在这个chunk内的偏移量是 70MB - 1*64MB = 6MB。
3. 客户端向Master询问文件/data/largefile.log的第1号chunk的位置信息。
4. Master回复chunk句柄和所有副本的位置。
5. 客户端联系其中一个副本（通常是最近的），请求读取chunk句柄对应的chunk中从6MB开始到7MB结束的数据。

不是所有文件的大小都正好是64MB的整数倍。因此，文件的最后一个chunk通常是不满的。GFS对此有高效的处理：

惰性空间分配：每个chunk在Chunkserver上最初只是一个空文件。它只在数据被实际写入时才会分配磁盘空间。这意味着一个32MB的文件只占用32MB的物理磁盘空间，而不是64MB，避免了内部碎片化浪费。

精确记录大小：Master的元数据中会记录每个chunk的实际大小。客户端读取最后一个chunk时，Chunkserver只会返回有效范围内的数据，不会返回未写入的“垃圾”数据。

#### 元数据

master主要存储三种元数据：文件和chunk的命名空间（namespace）、文件到chunk的映射和chunk的每个副本的位置

前两种元数据通过操作日志（Operation Log） 持久化到磁盘并复制到远程机器。

块的位置信息不持久化。Master在启动时或Chunkserver加入时通过询问Chunkserver来获取这些信息。这简化了设计，避免了Master和Chunkserver之间的状态同步问题。

#### 一致性模型

所有客户端无论从哪个副本读取，看到的数据都是一样的。

### 系统交互