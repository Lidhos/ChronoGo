package index

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// CompositeIndex 复合索引实现
type CompositeIndex struct {
	// 索引名称
	name string
	// 索引字段
	fields []string
	// 是否唯一索引
	unique bool
	// 索引数据 compositeKey -> seriesIDs
	data map[string]SeriesIDSet
	// 统计信息
	stats IndexStats
	// 互斥锁
	mu sync.RWMutex
}

// NewCompositeIndex 创建新的复合索引
func NewCompositeIndex(name string, fields []string, unique bool) *CompositeIndex {
	return &CompositeIndex{
		name:   name,
		fields: fields,
		unique: unique,
		data:   make(map[string]SeriesIDSet),
		stats: IndexStats{
			Name:           name,
			Type:           IndexTypeComposite,
			LastUpdateTime: time.Now().UnixNano(),
		},
	}
}

// Name 返回索引名称
func (idx *CompositeIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *CompositeIndex) Type() IndexType {
	return IndexTypeComposite
}

// Fields 返回索引字段
func (idx *CompositeIndex) Fields() []string {
	return idx.fields
}

// makeCompositeKey 生成复合键
func makeCompositeKey(values []string) string {
	return strings.Join(values, "\x00")
}

// parseCompositeKey 解析复合键
func parseCompositeKey(compositeKey string) []string {
	return strings.Split(compositeKey, "\x00")
}

// Insert 插入索引项
func (idx *CompositeIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为字符串切片
	values, ok := key.([]string)
	if !ok {
		return fmt.Errorf("key must be []string, got %T", key)
	}

	// 检查字段数量
	if len(values) != len(idx.fields) {
		return fmt.Errorf("expected %d values, got %d", len(idx.fields), len(values))
	}

	// 生成复合键
	compositeKey := makeCompositeKey(values)

	// 检查是否已存在（唯一索引）
	if idx.unique {
		if ids, ok := idx.data[compositeKey]; ok && ids.Size() > 0 {
			return fmt.Errorf("duplicate key %s for unique index %s", compositeKey, idx.name)
		}
	}

	// 添加到索引
	if _, ok := idx.data[compositeKey]; !ok {
		idx.data[compositeKey] = NewSeriesIDSet()
	}
	idx.data[compositeKey].Add(SeriesID(seriesID))

	// 更新统计信息
	idx.stats.ItemCount++
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Remove 删除索引项
func (idx *CompositeIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为字符串切片
	values, ok := key.([]string)
	if !ok {
		return fmt.Errorf("key must be []string, got %T", key)
	}

	// 检查字段数量
	if len(values) != len(idx.fields) {
		return fmt.Errorf("expected %d values, got %d", len(idx.fields), len(values))
	}

	// 生成复合键
	compositeKey := makeCompositeKey(values)

	// 从索引中删除
	if ids, ok := idx.data[compositeKey]; ok {
		ids.Remove(SeriesID(seriesID))
		// 如果集合为空，删除键
		if ids.Size() == 0 {
			delete(idx.data, compositeKey)
		}
		// 更新统计信息
		idx.stats.ItemCount--
		idx.stats.LastUpdateTime = time.Now().UnixNano()
	}

	return nil
}

// Update 更新索引项
func (idx *CompositeIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧的，再插入新的
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *CompositeIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 记录查询开始时间
	startTime := time.Now().UnixNano()

	// 复合索引需要特殊处理
	if condition.Field != "" {
		return nil, fmt.Errorf("composite index requires composite conditions")
	}

	var result SeriesIDSet

	// 处理复合条件
	switch condition.Operator {
	case "=", "==":
		// 精确匹配
		values, ok := condition.Value.([]string)
		if !ok {
			return nil, fmt.Errorf("value must be []string for = operator, got %T", condition.Value)
		}

		// 检查字段数量
		if len(values) != len(idx.fields) {
			return nil, fmt.Errorf("expected %d values, got %d", len(idx.fields), len(values))
		}

		// 生成复合键
		compositeKey := makeCompositeKey(values)

		// 查找键
		if ids, ok := idx.data[compositeKey]; ok {
			result = ids
		} else {
			result = NewSeriesIDSet()
		}
	case "PREFIX":
		// 前缀匹配
		values, ok := condition.Value.([]string)
		if !ok {
			return nil, fmt.Errorf("value must be []string for PREFIX operator, got %T", condition.Value)
		}

		// 检查前缀长度
		if len(values) > len(idx.fields) {
			return nil, fmt.Errorf("prefix length %d exceeds fields length %d", len(values), len(idx.fields))
		}

		// 生成前缀
		prefix := makeCompositeKey(values)

		// 查找所有匹配前缀的键
		result = NewSeriesIDSet()
		for key, ids := range idx.data {
			if strings.HasPrefix(key, prefix) {
				result = result.Union(ids)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s for composite index", condition.Operator)
	}

	// 更新统计信息
	idx.stats.QueryCount++
	if result.Size() > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)

	return result.ToSlice(), nil
}

// SearchRange 范围搜索
func (idx *CompositeIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 记录查询开始时间
	startTime := time.Now().UnixNano()

	// 将key转换为字符串切片
	startValues, ok := startKey.([]string)
	if !ok {
		return nil, fmt.Errorf("startKey must be []string, got %T", startKey)
	}
	endValues, ok := endKey.([]string)
	if !ok {
		return nil, fmt.Errorf("endKey must be []string, got %T", endKey)
	}

	// 检查字段数量
	if len(startValues) != len(idx.fields) {
		return nil, fmt.Errorf("expected %d values for startKey, got %d", len(idx.fields), len(startValues))
	}
	if len(endValues) != len(idx.fields) {
		return nil, fmt.Errorf("expected %d values for endKey, got %d", len(idx.fields), len(endValues))
	}

	// 生成复合键
	startCompositeKey := makeCompositeKey(startValues)
	endCompositeKey := makeCompositeKey(endValues)

	// 查找范围内的所有键
	result := NewSeriesIDSet()
	for key, ids := range idx.data {
		// 检查是否在范围内
		if (includeStart && key >= startCompositeKey || !includeStart && key > startCompositeKey) &&
			(includeEnd && key <= endCompositeKey || !includeEnd && key < endCompositeKey) {
			result = result.Union(ids)
		}
	}

	// 更新统计信息
	idx.stats.QueryCount++
	if result.Size() > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)

	return result.ToSlice(), nil
}

// Clear 清空索引
func (idx *CompositeIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 清空数据
	idx.data = make(map[string]SeriesIDSet)

	// 更新统计信息
	idx.stats.ItemCount = 0
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Stats 返回索引统计信息
func (idx *CompositeIndex) Stats(ctx context.Context) (IndexStats, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return IndexStats{}, ctx.Err()
	default:
	}

	// 计算索引大小
	var sizeBytes int64
	for key, ids := range idx.data {
		sizeBytes += int64(len(key))
		sizeBytes += int64(ids.Size() * 16) // 每个ID估计16字节
	}
	idx.stats.SizeBytes = sizeBytes

	return idx.stats, nil
}

// Close 关闭索引
func (idx *CompositeIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空数据
	idx.data = nil

	return nil
}

// Compact 手动触发压缩（仅LSM树索引支持）
func (idx *CompositeIndex) Compact(ctx context.Context) error {
	// 组合索引不支持压缩操作
	return nil
}

// Snapshot 创建索引快照（仅LSM树索引支持）
func (idx *CompositeIndex) Snapshot() (IndexSnapshot, error) {
	// 组合索引不支持快照
	return nil, fmt.Errorf("snapshot not supported for composite index")
}

// Flush 将内存中的数据刷新到磁盘（仅LSM树索引支持）
func (idx *CompositeIndex) Flush(ctx context.Context) error {
	// 组合索引不支持刷盘操作
	return nil
}
