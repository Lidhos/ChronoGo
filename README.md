# ChronoGo

ChronoGo是一个基于MongoDB接口的高性能Go时序数据库系统。它提供与MongoDB完全兼容的接口与BSON协议，同时针对时序数据进行了专门的优化。

## 特点

- 提供与MongoDB完全兼容的接口与BSON协议
- 纯Go语言实现，不依赖CGO，降低复杂度
- 专为时序数据优化的存储与查询引擎
- 为未来集群扩展预留架构基础，借鉴TDengine的设计
- 高吞吐、低延迟、高效压缩

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
- 分布式集群支持

## 贡献

欢迎贡献代码、报告问题或提出建议。请通过GitHub Issues或Pull Requests参与项目开发。

## 许可证

本项目采用MIT许可证。详见[LICENSE](LICENSE)文件。 