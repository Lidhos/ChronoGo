package index

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// InvertedIndex 倒排索引实现
type InvertedIndex struct {
	// 索引名称
	name string
	// 索引字段
	fields []string
	// 是否唯一索引
	unique bool
	// 索引数据 field -> value -> seriesIDs
	data map[string]map[string]SeriesIDSet
	// 统计信息
	stats IndexStats
	// 互斥锁
	mu sync.RWMutex
}

// NewInvertedIndex 创建新的倒排索引
func NewInvertedIndex(name string, fields []string, unique bool) *InvertedIndex {
	return &InvertedIndex{
		name:   name,
		fields: fields,
		unique: unique,
		data:   make(map[string]map[string]SeriesIDSet),
		stats: IndexStats{
			Name:           name,
			Type:           IndexTypeInverted,
			LastUpdateTime: time.Now().UnixNano(),
		},
	}
}

// Name 返回索引名称
func (idx *InvertedIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *InvertedIndex) Type() IndexType {
	return IndexTypeInverted
}

// Fields 返回索引字段
func (idx *InvertedIndex) Fields() []string {
	return idx.fields
}

// Insert 插入索引项
func (idx *InvertedIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为字符串
	strKey, ok := key.(string)
	if !ok {
		return fmt.Errorf("key must be string, got %T", key)
	}

	// 确保字段存在
	field := idx.fields[0] // 目前只支持单字段索引
	if _, ok := idx.data[field]; !ok {
		idx.data[field] = make(map[string]SeriesIDSet)
	}

	// 检查是否已存在（唯一索引）
	if idx.unique {
		for existingKey, ids := range idx.data[field] {
			if existingKey == strKey && ids.Size() > 0 {
				return fmt.Errorf("duplicate key %s for unique index %s", strKey, idx.name)
			}
		}
	}

	// 添加到索引
	if _, ok := idx.data[field][strKey]; !ok {
		idx.data[field][strKey] = NewSeriesIDSet()
	}
	idx.data[field][strKey].Add(SeriesID(seriesID))

	// 更新统计信息
	idx.stats.ItemCount++
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Remove 删除索引项
func (idx *InvertedIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为字符串
	strKey, ok := key.(string)
	if !ok {
		return fmt.Errorf("key must be string, got %T", key)
	}

	// 确保字段存在
	field := idx.fields[0] // 目前只支持单字段索引
	if _, ok := idx.data[field]; !ok {
		return nil // 字段不存在，无需删除
	}

	// 从索引中删除
	if ids, ok := idx.data[field][strKey]; ok {
		ids.Remove(SeriesID(seriesID))
		// 如果集合为空，删除键
		if ids.Size() == 0 {
			delete(idx.data[field], strKey)
		}
		// 更新统计信息
		idx.stats.ItemCount--
		idx.stats.LastUpdateTime = time.Now().UnixNano()
	}

	return nil
}

// Update 更新索引项
func (idx *InvertedIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧的，再插入新的
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *InvertedIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
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

	// 确保字段存在
	field := condition.Field
	if _, ok := idx.data[field]; !ok {
		// 更新统计信息
		idx.stats.QueryCount++
		idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)
		return []string{}, nil // 字段不存在，返回空结果
	}

	var result SeriesIDSet

	switch condition.Operator {
	case "=", "==":
		// 精确匹配
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for = operator, got %T", condition.Value)
		}
		if ids, ok := idx.data[field][strValue]; ok {
			result = ids
		} else {
			result = NewSeriesIDSet()
		}
	case "IN":
		// IN操作符
		values, ok := condition.Value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("value must be []interface{} for IN operator, got %T", condition.Value)
		}
		result = NewSeriesIDSet()
		for _, val := range values {
			strVal, ok := val.(string)
			if !ok {
				continue
			}
			if ids, ok := idx.data[field][strVal]; ok {
				result = result.Union(ids)
			}
		}
	case "LIKE":
		// 前缀匹配
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for LIKE operator, got %T", condition.Value)
		}
		// 移除通配符
		pattern := strings.ReplaceAll(strValue, "%", "")
		result = NewSeriesIDSet()
		for key, ids := range idx.data[field] {
			if strings.Contains(key, pattern) {
				result = result.Union(ids)
			}
		}
	case "REGEX":
		// 正则表达式匹配
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for REGEX operator, got %T", condition.Value)
		}
		re, err := regexp.Compile(strValue)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %v", err)
		}
		result = NewSeriesIDSet()
		for key, ids := range idx.data[field] {
			if re.MatchString(key) {
				result = result.Union(ids)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s for inverted index", condition.Operator)
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
func (idx *InvertedIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	// 倒排索引不直接支持范围查询，需要扫描所有键
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

	// 将key转换为字符串
	strStartKey, ok := startKey.(string)
	if !ok {
		return nil, fmt.Errorf("startKey must be string, got %T", startKey)
	}
	strEndKey, ok := endKey.(string)
	if !ok {
		return nil, fmt.Errorf("endKey must be string, got %T", endKey)
	}

	// 确保字段存在
	field := idx.fields[0] // 目前只支持单字段索引
	if _, ok := idx.data[field]; !ok {
		// 更新统计信息
		idx.stats.QueryCount++
		idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)
		return []string{}, nil // 字段不存在，返回空结果
	}

	result := NewSeriesIDSet()
	for key, ids := range idx.data[field] {
		// 检查是否在范围内
		if (includeStart && key >= strStartKey || !includeStart && key > strStartKey) &&
			(includeEnd && key <= strEndKey || !includeEnd && key < strEndKey) {
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
func (idx *InvertedIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 清空数据
	idx.data = make(map[string]map[string]SeriesIDSet)

	// 更新统计信息
	idx.stats.ItemCount = 0
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Stats 返回索引统计信息
func (idx *InvertedIndex) Stats(ctx context.Context) (IndexStats, error) {
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
	for _, fieldData := range idx.data {
		sizeBytes += int64(len(fieldData)) * 64 // 估算每个键值对占用64字节
	}
	idx.stats.SizeBytes = sizeBytes

	return idx.stats, nil
}

// Close 关闭索引
func (idx *InvertedIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空数据
	idx.data = nil

	return nil
}
