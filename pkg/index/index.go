package index

import (
	"context"
)

// IndexType 表示索引类型
type IndexType byte

const (
	// IndexTypeInverted 倒排索引
	IndexTypeInverted IndexType = 1
	// IndexTypeBTree B+树索引
	IndexTypeBTree IndexType = 2
	// IndexTypeBitmap 位图索引
	IndexTypeBitmap IndexType = 3
	// IndexTypeHash 哈希索引
	IndexTypeHash IndexType = 4
	// IndexTypeComposite 复合索引
	IndexTypeComposite IndexType = 5
)

// IndexOptions 索引选项
type IndexOptions struct {
	// 索引类型
	Type IndexType
	// 索引名称
	Name string
	// 是否唯一索引
	Unique bool
	// 索引字段
	Fields []string
	// 索引基数估计（用于自动选择索引类型）
	CardinalityEstimate int
	// 是否支持范围查询
	SupportRange bool
	// 是否支持前缀匹配
	SupportPrefix bool
	// 是否支持正则表达式
	SupportRegex bool
}

// IndexStats 索引统计信息
type IndexStats struct {
	// 索引名称
	Name string
	// 索引类型
	Type IndexType
	// 索引大小（字节）
	SizeBytes int64
	// 索引项数量
	ItemCount int64
	// 查询次数
	QueryCount int64
	// 命中次数
	HitCount int64
	// 平均查询时间（纳秒）
	AvgQueryTimeNs int64
	// 最后更新时间
	LastUpdateTime int64
}

// IndexCondition 索引查询条件
type IndexCondition struct {
	// 字段名
	Field string
	// 操作符（=, >, <, >=, <=, LIKE, IN, REGEX）
	Operator string
	// 值
	Value interface{}
}

// Index 索引接口
type Index interface {
	// Name 返回索引名称
	Name() string

	// Type 返回索引类型
	Type() IndexType

	// Fields 返回索引字段
	Fields() []string

	// Insert 插入索引项
	Insert(ctx context.Context, key interface{}, seriesID string) error

	// Remove 删除索引项
	Remove(ctx context.Context, key interface{}, seriesID string) error

	// Update 更新索引项
	Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error

	// Search 搜索索引
	Search(ctx context.Context, condition IndexCondition) ([]string, error)

	// SearchRange 范围搜索
	SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error)

	// Clear 清空索引
	Clear(ctx context.Context) error

	// Stats 返回索引统计信息
	Stats(ctx context.Context) (IndexStats, error)

	// Close 关闭索引
	Close() error
}

// IndexManager 索引管理器接口
type IndexManager interface {
	// CreateIndex 创建索引
	CreateIndex(ctx context.Context, database, collection string, options IndexOptions) (Index, error)

	// DropIndex 删除索引
	DropIndex(ctx context.Context, database, collection, indexName string) error

	// GetIndex 获取索引
	GetIndex(ctx context.Context, database, collection, indexName string) (Index, error)

	// ListIndexes 列出集合的所有索引
	ListIndexes(ctx context.Context, database, collection string) ([]Index, error)

	// RebuildIndex 重建索引
	RebuildIndex(ctx context.Context, database, collection, indexName string) error

	// GetStats 获取索引统计信息
	GetStats(ctx context.Context, database, collection, indexName string) (IndexStats, error)

	// SelectBestIndex 根据查询条件选择最佳索引
	SelectBestIndex(ctx context.Context, database, collection string, conditions []IndexCondition) (Index, error)

	// Close 关闭索引管理器
	Close() error
}

// SeriesID 表示时间序列ID
type SeriesID string

// SeriesIDSet 表示时间序列ID集合
type SeriesIDSet map[SeriesID]struct{}

// NewSeriesIDSet 创建新的时间序列ID集合
func NewSeriesIDSet() SeriesIDSet {
	return make(SeriesIDSet)
}

// Add 添加时间序列ID
func (s SeriesIDSet) Add(id SeriesID) {
	s[id] = struct{}{}
}

// Remove 删除时间序列ID
func (s SeriesIDSet) Remove(id SeriesID) {
	delete(s, id)
}

// Contains 检查是否包含时间序列ID
func (s SeriesIDSet) Contains(id SeriesID) bool {
	_, ok := s[id]
	return ok
}

// Size 返回集合大小
func (s SeriesIDSet) Size() int {
	return len(s)
}

// ToSlice 转换为切片
func (s SeriesIDSet) ToSlice() []string {
	result := make([]string, 0, len(s))
	for id := range s {
		result = append(result, string(id))
	}
	return result
}

// Intersection 计算交集
func (s SeriesIDSet) Intersection(other SeriesIDSet) SeriesIDSet {
	result := NewSeriesIDSet()
	// 选择较小的集合进行遍历
	if len(s) > len(other) {
		s, other = other, s
	}
	for id := range s {
		if other.Contains(id) {
			result.Add(id)
		}
	}
	return result
}

// Union 计算并集
func (s SeriesIDSet) Union(other SeriesIDSet) SeriesIDSet {
	result := NewSeriesIDSet()
	for id := range s {
		result.Add(id)
	}
	for id := range other {
		result.Add(id)
	}
	return result
}

// Difference 计算差集
func (s SeriesIDSet) Difference(other SeriesIDSet) SeriesIDSet {
	result := NewSeriesIDSet()
	for id := range s {
		if !other.Contains(id) {
			result.Add(id)
		}
	}
	return result
}
