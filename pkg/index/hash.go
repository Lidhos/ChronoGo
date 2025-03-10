package index

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// HashIndex 哈希索引实现
type HashIndex struct {
	// 索引名称
	name string
	// 索引字段
	fields []string
	// 是否唯一索引
	unique bool
	// 索引数据 field -> hash -> value -> seriesIDs
	data map[string]map[uint64]map[string]SeriesIDSet
	// 统计信息
	stats IndexStats
	// 互斥锁
	mu sync.RWMutex
}

// NewHashIndex 创建新的哈希索引
func NewHashIndex(name string, fields []string, unique bool) *HashIndex {
	return &HashIndex{
		name:   name,
		fields: fields,
		unique: unique,
		data:   make(map[string]map[uint64]map[string]SeriesIDSet),
		stats: IndexStats{
			Name:           name,
			Type:           IndexTypeHash,
			LastUpdateTime: time.Now().UnixNano(),
		},
	}
}

// Name 返回索引名称
func (idx *HashIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *HashIndex) Type() IndexType {
	return IndexTypeHash
}

// Fields 返回索引字段
func (idx *HashIndex) Fields() []string {
	return idx.fields
}

// hashKey 计算键的哈希值
func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// Insert 插入索引项
func (idx *HashIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
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

	// 计算哈希值
	hash := hashKey(strKey)

	// 确保字段存在
	field := idx.fields[0] // 目前只支持单字段索引
	if _, ok := idx.data[field]; !ok {
		idx.data[field] = make(map[uint64]map[string]SeriesIDSet)
	}
	if _, ok := idx.data[field][hash]; !ok {
		idx.data[field][hash] = make(map[string]SeriesIDSet)
	}

	// 检查是否已存在（唯一索引）
	if idx.unique {
		if ids, ok := idx.data[field][hash][strKey]; ok && ids.Size() > 0 {
			return fmt.Errorf("duplicate key %s for unique index %s", strKey, idx.name)
		}
	}

	// 添加到索引
	if _, ok := idx.data[field][hash][strKey]; !ok {
		idx.data[field][hash][strKey] = NewSeriesIDSet()
	}
	idx.data[field][hash][strKey].Add(SeriesID(seriesID))

	// 更新统计信息
	idx.stats.ItemCount++
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Remove 删除索引项
func (idx *HashIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
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

	// 计算哈希值
	hash := hashKey(strKey)

	// 确保字段存在
	field := idx.fields[0] // 目前只支持单字段索引
	if _, ok := idx.data[field]; !ok {
		return nil // 字段不存在，无需删除
	}
	if _, ok := idx.data[field][hash]; !ok {
		return nil // 哈希不存在，无需删除
	}

	// 从索引中删除
	if ids, ok := idx.data[field][hash][strKey]; ok {
		ids.Remove(SeriesID(seriesID))
		// 如果集合为空，删除键
		if ids.Size() == 0 {
			delete(idx.data[field][hash], strKey)
			// 如果哈希桶为空，删除哈希
			if len(idx.data[field][hash]) == 0 {
				delete(idx.data[field], hash)
			}
		}
		// 更新统计信息
		idx.stats.ItemCount--
		idx.stats.LastUpdateTime = time.Now().UnixNano()
	}

	return nil
}

// Update 更新索引项
func (idx *HashIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧的，再插入新的
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *HashIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
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

		// 计算哈希值
		hash := hashKey(strValue)

		// 查找哈希桶
		if hashBucket, ok := idx.data[field][hash]; ok {
			// 查找键
			if ids, ok := hashBucket[strValue]; ok {
				result = ids
			} else {
				result = NewSeriesIDSet()
			}
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

			// 计算哈希值
			hash := hashKey(strVal)

			// 查找哈希桶
			if hashBucket, ok := idx.data[field][hash]; ok {
				// 查找键
				if ids, ok := hashBucket[strVal]; ok {
					result = result.Union(ids)
				}
			}
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s for hash index", condition.Operator)
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
func (idx *HashIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	// 哈希索引不支持范围查询
	return nil, fmt.Errorf("hash index does not support range queries")
}

// Clear 清空索引
func (idx *HashIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 清空数据
	idx.data = make(map[string]map[uint64]map[string]SeriesIDSet)

	// 更新统计信息
	idx.stats.ItemCount = 0
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Stats 返回索引统计信息
func (idx *HashIndex) Stats(ctx context.Context) (IndexStats, error) {
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
	var bucketCount int64
	var collisionCount int64

	for _, fieldData := range idx.data {
		for _, hashBucket := range fieldData {
			bucketCount++
			if len(hashBucket) > 1 {
				collisionCount += int64(len(hashBucket) - 1)
			}
			for key, ids := range hashBucket {
				sizeBytes += int64(len(key))
				sizeBytes += int64(ids.Size() * 16) // 每个ID估计16字节
			}
		}
	}

	// 添加哈希表开销
	sizeBytes += bucketCount * 16 // 每个哈希桶估计16字节

	idx.stats.SizeBytes = sizeBytes

	return idx.stats, nil
}

// Close 关闭索引
func (idx *HashIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空数据
	idx.data = nil

	return nil
}
