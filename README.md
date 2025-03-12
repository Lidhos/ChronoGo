# ChronoGo

ChronoGo是一个基于MongoDB接口的高性能Go时序数据库系统。它提供与MongoDB完全兼容的接口与BSON协议，同时针对时序数据进行了专门的优化。

## 特点

- 提供与MongoDB完全兼容的接口与BSON协议
- 纯Go语言实现，不依赖CGO，降低复杂度
- 专为时序数据优化的存储与查询引擎
- 为未来集群扩展预留架构基础，借鉴TDengine的设计
- 高吞吐、低延迟、高效压缩

## 性能指标

ChronoGo经过多轮优化，写入性能表现出色。以下是在Intel(R) Core(TM) i5-8259U CPU @ 2.30GHz处理器上的基准测试结果：

### 写入性能

| 操作类型 | 每操作耗时 (ns/op) | 每秒处理能力 |
|---------|------------------|------------|
| 单点写入 | 118,091 | 约 8,469 次/秒 |
| 批量写入 (每批100点) | 751,130 | 约 133,100 点/秒 |

### 优化技术

ChronoGo采用了多种优化技术来提高写入性能：

1. **异步写前日志(WAL)**：写操作不再阻塞等待WAL写入完成，显著降低写入延迟
2. **写缓冲区**：将多个写操作批量处理，减少I/O操作次数和锁竞争
3. **异步索引更新**：索引更新不再阻塞写入操作，提高写入吞吐量
4. **对象池化**：减少内存分配和垃圾回收，降低系统开销
5. **批量索引更新**：合并相同键的索引更新，减少索引操作次数

这些优化使得ChronoGo在高并发写入场景下表现出色，特别适合IoT、监控、日志等需要高吞吐量写入的时序数据应用场景。

## 系统架构

```
+-------------------------------------+
|      MongoDB兼容协议层(Wire协议)    |
+-------------------------------------+
                  |
+-------------------------------------+
|        MongoDB兼容BSON接口层        |
+-------------------------------------+
                  |
+-------------------------------------+
|             查询处理引擎            |
+-------------------------------------+
                  |
+-------------------------------------+
|             时序存储引擎            |
+-------------------------------------+
        |                  |
+----------------+  +------------------+
|  内存数据结构  |  |   磁盘存储结构   |
| (定制化跳表)   |  | (列式存储+压缩)  |
+----------------+  +------------------+
```

### 集群架构

ChronoGo采用分布式架构设计，包含以下几种类型的节点：

- **管理节点(MNode)**: 负责集群元数据管理、节点状态监控和主节点选举
- **数据节点(DNode)**: 负责数据存储和查询处理
- **虚拟节点(VNode)**: 数据分片的最小单位，每个DNode可以包含多个VNode
- **虚拟节点组(VGroup)**: 由多个VNode组成，用于数据复制和故障转移

集群架构图：

```
+----------------+     +----------------+     +----------------+
|    MNode 1     |     |    MNode 2     |     |    MNode 3     |
| (Master)       |<--->|    (Slave)     |<--->|    (Slave)     |
+----------------+     +----------------+     +----------------+
        ^                     ^                      ^
        |                     |                      |
        v                     v                      v
+----------------+     +----------------+     +----------------+
|    DNode 1     |     |    DNode 2     |     |    DNode 3     |
|  VNode1,2,3    |<--->|  VNode4,5,6    |<--->|  VNode7,8,9    |
+----------------+     +----------------+     +----------------+
```

### 集群功能

1. **元数据管理**
   - 集群配置信息管理
   - 节点状态监控
   - 数据库和表结构管理

2. **节点管理**
   - 动态节点加入/退出
   - 主节点选举(基于简单选举算法)
   - 节点心跳检测和健康监控

3. **数据分片**
   - 基于虚拟节点的数据分片
   - 自动负载均衡
   - 数据迁移和重平衡

4. **高可用性**
   - 管理节点主从切换
   - 数据多副本存储
   - 故障自动检测和恢复

### 集群配置

在启动时可以通过以下参数配置集群：

```bash
# 启动管理节点
./chronogo --cluster-role=mnode --node-id=mnode1 --cluster-id=cluster1 --data-dir=/path/to/data

# 启动数据节点
./chronogo --cluster-role=dnode --node-id=dnode1 --cluster-id=cluster1 --data-dir=/path/to/data --mnode-list=mnode1:8091,mnode2:8091
```

参数说明：
- `--cluster-role`: 节点角色，可选值：mnode(管理节点)、dnode(数据节点)
- `--node-id`: 节点唯一标识
- `--cluster-id`: 集群唯一标识
- `--mnode-list`: 管理节点列表，用于数据节点连接集群

## 快速开始

### 编译

```bash
go build -o chronogo
```

### 运行

```bash
./chronogo --data-dir=/path/to/data --mongo-addr=:27017
```

### 参数说明

- `--data-dir`: 数据存储目录，默认为"./data"
- `--mongo-addr`: MongoDB协议服务地址，默认为":27017"
- `--http-addr`: HTTP服务地址，默认为":8080"（未来支持）

## 使用示例

由于ChronoGo提供与MongoDB完全兼容的接口，您可以使用任何MongoDB客户端或驱动程序连接ChronoGo。

### 创建时序集合

```javascript
db.createCollection("sensors", {
  timeseries: {
    timeField: "timestamp",
    metaField: "tags",
    granularity: "seconds"
  },
  expireAfterSeconds: 2592000 // 30天
})
```

### 插入数据

```javascript
db.sensors.insertOne({
  timestamp: ISODate("2023-03-09T12:00:00Z"),
  tags: {
    location: "room1",
    device_id: "dev001"
  },
  temperature: 25.3,
  humidity: 60.1
})
```

### 查询数据

```javascript
// 时间范围查询
db.sensors.find({
  "tags.location": "room1",
  timestamp: {
    $gte: ISODate("2023-03-09T00:00:00Z"),
    $lt: ISODate("2023-03-10T00:00:00Z")
  }
})

// 时间窗口聚合
db.sensors.aggregate([
  {
    $match: {
      "tags.location": "room1",
      timestamp: {
        $gte: ISODate("2023-03-09T00:00:00Z"),
        $lt: ISODate("2023-03-10T00:00:00Z")
      }
    }
  },
  {
    $timeWindow: {
      timestamp: "$timestamp",
      window: "1h",
      output: {
        avgTemp: { $avg: "$temperature" },
        maxTemp: { $max: "$temperature" }
      }
    }
  },
  {
    $sort: { "_id.timestamp": 1 }
  }
])
```

## 当前状态

ChronoGo目前处于早期开发阶段，基本功能已经实现，但仍有许多功能需要完善。

### 已实现功能

- 基本的MongoDB协议兼容层
- 基于跳表的内存存储引擎
- 预写日志(WAL)系统
- 基本的查询处理引擎

### 待实现功能

- 完整的MongoDB命令支持
- 列式存储与压缩算法
- 时序特有的聚合操作
- Raft共识算法支持
- 自动负载均衡
- 数据迁移工具

## 性能测试工具

ChronoGo 提供了专门的性能测试和分析工具，帮助用户评估系统性能并识别潜在的性能瓶颈。

### 使用方法

```bash
# 编译性能测试工具
cd cmd/benchmark
go build -o chronogo_benchmark

# 运行性能测试
./chronogo_benchmark -test all -duration 30 -parallel 4 -batchsize 100 -cpuprofile profiles/cpu.prof -memprofile profiles/mem.prof
```

### 测试选项

- `-test`: 测试类型，可选 single（单点写入）、batch（批量写入）或 all（全部）
- `-duration`: 测试持续时间（秒）
- `-parallel`: 并行客户端数量
- `-batchsize`: 批量写入的批次大小
- `-cpuprofile`: CPU 性能分析文件路径
- `-memprofile`: 内存性能分析文件路径
- `-blockprofile`: 阻塞性能分析文件路径
- `-server`: ChronoGo 服务器地址，默认 localhost:27017

### 性能分析

使用 Go 的 pprof 工具分析性能数据：

```bash
go tool pprof -http=:8080 profiles/cpu.prof
```

更多详细信息，请参阅 [性能测试与分析指南](cmd/benchmark/PERFORMANCE_GUIDE.md)。

## 贡献

欢迎贡献代码、报告问题或提出建议。请通过GitHub Issues或Pull Requests参与项目开发。

## 许可证

本项目采用MIT许可证。详见[LICENSE](LICENSE)文件。 