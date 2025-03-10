package model

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// TimeSeriesPoint 表示时序数据点的内部表示
type TimeSeriesPoint struct {
	Timestamp int64                  `bson:"timestamp"` // Unix纳秒时间戳
	Tags      map[string]string      `bson:"tags"`      // 标签，索引字段
	Fields    map[string]interface{} `bson:"fields"`    // 测量值
}

// Database 表示数据库定义
type Database struct {
	Name            string
	RetentionPolicy RetentionPolicy
	Tables          map[string]*Table
	mu              sync.RWMutex
}

// Table 表示表定义
type Table struct {
	Name       string
	Database   string
	Schema     Schema
	TagIndexes []TagIndex
	mu         sync.RWMutex
}

// Schema 表示表的模式定义
type Schema struct {
	TimeField string            // 时间戳字段名
	TagFields map[string]string // 标签字段类型
	Fields    map[string]string // 值字段类型
}

// RetentionPolicy 表示数据保留策略
type RetentionPolicy struct {
	Duration   time.Duration      // 数据保留时长
	Precision  string             // 时间精度(ns, us, ms, s)
	Downsample []DownsamplePolicy // 降采样策略
}

// DownsamplePolicy 表示降采样策略
type DownsamplePolicy struct {
	Interval    time.Duration // 降采样间隔
	Function    string        // 降采样函数: "avg", "sum", "min", "max", "count", "first", "last"
	Retention   time.Duration // 降采样数据保留时长
	Destination string        // 降采样数据存储目标，空表示存储在原表中
}

// StorageTier 表示存储层级
type StorageTier struct {
	Name      string        // 层级名称
	Type      string        // 存储类型: "memory", "ssd", "hdd", "s3", "gcs", "azure"
	Priority  int           // 优先级，数字越小优先级越高
	MaxAge    time.Duration // 数据在此层级的最大存储时间
	ReadOnly  bool          // 是否只读
	Compress  bool          // 是否压缩
	BatchSize int           // 批处理大小
}

// TagIndex 表示标签索引
type TagIndex struct {
	Name                string   // 索引名称
	Fields              []string // 索引字段（支持复合索引）
	Type                string   // 索引类型: "inverted", "btree", "bitmap", "hash", "composite", "time", "auto"
	Unique              bool     // 是否唯一索引
	CardinalityEstimate int      // 基数估计（用于自动选择索引类型）
	SupportRange        bool     // 是否支持范围查询
	SupportPrefix       bool     // 是否支持前缀匹配
	SupportRegex        bool     // 是否支持正则表达式
}

// TimeSeriesOptions 表示时序特有的集合配置
type TimeSeriesOptions struct {
	TimeField     string // 时间戳字段名
	MetaField     string // 标签字段名
	Granularity   string // 时间粒度
	ExpirySeconds int64  // 过期时间
}

// ToBSON 将TimeSeriesPoint转换为BSON文档
func (p *TimeSeriesPoint) ToBSON() bson.D {
	doc := bson.D{
		{Key: "timestamp", Value: p.Timestamp},
	}

	// 添加标签
	if len(p.Tags) > 0 {
		tags := bson.D{}
		for k, v := range p.Tags {
			tags = append(tags, bson.E{Key: k, Value: v})
		}
		doc = append(doc, bson.E{Key: "tags", Value: tags})
	}

	// 添加字段
	for k, v := range p.Fields {
		doc = append(doc, bson.E{Key: k, Value: v})
	}

	return doc
}

// FromBSON 从BSON文档创建TimeSeriesPoint
func FromBSON(doc bson.D) (*TimeSeriesPoint, error) {
	point := &TimeSeriesPoint{
		Tags:   make(map[string]string),
		Fields: make(map[string]interface{}),
	}

	for _, elem := range doc {
		switch elem.Key {
		case "timestamp":
			if ts, ok := elem.Value.(int64); ok {
				point.Timestamp = ts
			}
		case "tags":
			if tags, ok := elem.Value.(bson.D); ok {
				for _, tag := range tags {
					if strVal, ok := tag.Value.(string); ok {
						point.Tags[tag.Key] = strVal
					}
				}
			}
		default:
			// 其他字段视为测量值
			point.Fields[elem.Key] = elem.Value
		}
	}

	return point, nil
}
