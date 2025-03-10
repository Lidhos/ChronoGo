package index

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BitmapIndex 位图索引实现
type BitmapIndex struct {
	// 索引名称
	name string
	// 索引字段
	fields []string
	// 是否唯一索引
	unique bool
	// 索引数据 field -> value -> bitmap
	data map[string]map[string][]uint64
	// 时间序列ID到位图位置的映射
	seriesIDToPos map[string]int
	// 位图位置到时间序列ID的映射
	posToSeriesID []string
	// 下一个可用的位置
	nextPos int
	// 统计信息
	stats IndexStats
	// 互斥锁
	mu sync.RWMutex
}

// NewBitmapIndex 创建新的位图索引
func NewBitmapIndex(name string, fields []string, unique bool) *BitmapIndex {
	return &BitmapIndex{
		name:          name,
		fields:        fields,
		unique:        unique,
		data:          make(map[string]map[string][]uint64),
		seriesIDToPos: make(map[string]int),
		posToSeriesID: make([]string, 0),
		nextPos:       0,
		stats: IndexStats{
			Name:           name,
			Type:           IndexTypeBitmap,
			LastUpdateTime: time.Now().UnixNano(),
		},
	}
}

// Name 返回索引名称
func (idx *BitmapIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *BitmapIndex) Type() IndexType {
	return IndexTypeBitmap
}

// Fields 返回索引字段
func (idx *BitmapIndex) Fields() []string {
	return idx.fields
}

// getBitmapPos 获取时间序列ID对应的位图位置
func (idx *BitmapIndex) getBitmapPos(seriesID string) int {
	pos, ok := idx.seriesIDToPos[seriesID]
	if !ok {
		// 分配新位置
		pos = idx.nextPos
		idx.seriesIDToPos[seriesID] = pos
		idx.posToSeriesID = append(idx.posToSeriesID, seriesID)
		idx.nextPos++
	}
	return pos
}

// setBit 设置位图中的位
func setBit(bitmap []uint64, pos int) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	// 确保bitmap足够大
	for len(bitmap) <= wordIdx {
		bitmap = append(bitmap, 0)
	}
	bitmap[wordIdx] |= 1 << bitIdx
}

// clearBit 清除位图中的位
func clearBit(bitmap []uint64, pos int) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	if len(bitmap) > wordIdx {
		bitmap[wordIdx] &= ^(1 << bitIdx)
	}
}

// testBit 测试位图中的位
func testBit(bitmap []uint64, pos int) bool {
	wordIdx := pos / 64
	bitIdx := pos % 64
	if len(bitmap) <= wordIdx {
		return false
	}
	return (bitmap[wordIdx] & (1 << bitIdx)) != 0
}

// andBitmap 计算两个位图的交集
func andBitmap(a, b []uint64) []uint64 {
	result := make([]uint64, len(a))
	for i := 0; i < len(a) && i < len(b); i++ {
		result[i] = a[i] & b[i]
	}
	return result
}

// orBitmap 计算两个位图的并集
func orBitmap(a, b []uint64) []uint64 {
	result := make([]uint64, max(len(a), len(b)))
	// 复制a
	for i := 0; i < len(a); i++ {
		result[i] = a[i]
	}
	// 合并b
	for i := 0; i < len(b); i++ {
		result[i] |= b[i]
	}
	return result
}

// notBitmap 计算位图的补集
func notBitmap(a []uint64, size int) []uint64 {
	result := make([]uint64, len(a))
	for i := 0; i < len(a); i++ {
		result[i] = ^a[i]
	}
	// 处理最后一个字的多余位
	if size > 0 {
		lastWordIdx := (size - 1) / 64
		lastBitIdx := (size - 1) % 64
		if lastWordIdx < len(result) {
			// 清除超出size的位
			mask := (uint64(1) << (lastBitIdx + 1)) - 1
			result[lastWordIdx] &= mask
		}
	}
	return result
}

// countBits 计算位图中设置的位数
func countBits(bitmap []uint64) int {
	count := 0
	for _, word := range bitmap {
		// 使用位计数算法
		for word != 0 {
			count++
			word &= word - 1 // 清除最低位的1
		}
	}
	return count
}

// bitmapToSeriesIDs 将位图转换为时间序列ID列表
func (idx *BitmapIndex) bitmapToSeriesIDs(bitmap []uint64) []string {
	result := make([]string, 0)
	for i, word := range bitmap {
		if word == 0 {
			continue
		}
		for j := 0; j < 64; j++ {
			if (word & (1 << j)) != 0 {
				pos := i*64 + j
				if pos < len(idx.posToSeriesID) {
					result = append(result, idx.posToSeriesID[pos])
				}
			}
		}
	}
	return result
}

// Insert 插入索引项
func (idx *BitmapIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
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
		idx.data[field] = make(map[string][]uint64)
	}

	// 检查是否已存在（唯一索引）
	if idx.unique {
		for existingKey, bitmap := range idx.data[field] {
			if existingKey == strKey && countBits(bitmap) > 0 {
				return fmt.Errorf("duplicate key %s for unique index %s", strKey, idx.name)
			}
		}
	}

	// 获取位图位置
	pos := idx.getBitmapPos(seriesID)

	// 添加到索引
	if _, ok := idx.data[field][strKey]; !ok {
		idx.data[field][strKey] = make([]uint64, 0)
	}
	setBit(idx.data[field][strKey], pos)

	// 更新统计信息
	idx.stats.ItemCount++
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Remove 删除索引项
func (idx *BitmapIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
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

	// 获取位图位置
	pos, ok := idx.seriesIDToPos[seriesID]
	if !ok {
		return nil // 时间序列ID不存在，无需删除
	}

	// 从索引中删除
	if bitmap, ok := idx.data[field][strKey]; ok {
		clearBit(bitmap, pos)
		// 如果位图为空，删除键
		if countBits(bitmap) == 0 {
			delete(idx.data[field], strKey)
		}
		// 更新统计信息
		idx.stats.ItemCount--
		idx.stats.LastUpdateTime = time.Now().UnixNano()
	}

	return nil
}

// Update 更新索引项
func (idx *BitmapIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧的，再插入新的
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *BitmapIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
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

	var resultBitmap []uint64

	switch condition.Operator {
	case "=", "==":
		// 精确匹配
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for = operator, got %T", condition.Value)
		}
		if bitmap, ok := idx.data[field][strValue]; ok {
			resultBitmap = bitmap
		} else {
			resultBitmap = make([]uint64, 0)
		}
	case "IN":
		// IN操作符
		values, ok := condition.Value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("value must be []interface{} for IN operator, got %T", condition.Value)
		}
		resultBitmap = make([]uint64, 0)
		for _, val := range values {
			strVal, ok := val.(string)
			if !ok {
				continue
			}
			if bitmap, ok := idx.data[field][strVal]; ok {
				if len(resultBitmap) == 0 {
					resultBitmap = bitmap
				} else {
					resultBitmap = orBitmap(resultBitmap, bitmap)
				}
			}
		}
	case "!=", "<>":
		// 不等于操作符
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for != operator, got %T", condition.Value)
		}
		// 创建所有位都为1的位图
		allBitmap := make([]uint64, (idx.nextPos+63)/64)
		for i := range allBitmap {
			allBitmap[i] = ^uint64(0)
		}
		// 如果有最后一个字，处理多余的位
		if idx.nextPos > 0 {
			lastWordIdx := (idx.nextPos - 1) / 64
			lastBitIdx := (idx.nextPos - 1) % 64
			if lastWordIdx < len(allBitmap) {
				mask := (uint64(1) << (lastBitIdx + 1)) - 1
				allBitmap[lastWordIdx] &= mask
			}
		}
		// 计算补集
		if bitmap, ok := idx.data[field][strValue]; ok {
			resultBitmap = andBitmap(allBitmap, notBitmap(bitmap, idx.nextPos))
		} else {
			resultBitmap = allBitmap
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s for bitmap index", condition.Operator)
	}

	// 将位图转换为时间序列ID列表
	result := idx.bitmapToSeriesIDs(resultBitmap)

	// 更新统计信息
	idx.stats.QueryCount++
	if len(result) > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)

	return result, nil
}

// SearchRange 范围搜索
func (idx *BitmapIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	// 位图索引不直接支持范围查询，需要扫描所有键
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

	resultBitmap := make([]uint64, 0)
	for key, bitmap := range idx.data[field] {
		// 检查是否在范围内
		if (includeStart && key >= strStartKey || !includeStart && key > strStartKey) &&
			(includeEnd && key <= strEndKey || !includeEnd && key < strEndKey) {
			if len(resultBitmap) == 0 {
				resultBitmap = bitmap
			} else {
				resultBitmap = orBitmap(resultBitmap, bitmap)
			}
		}
	}

	// 将位图转换为时间序列ID列表
	result := idx.bitmapToSeriesIDs(resultBitmap)

	// 更新统计信息
	idx.stats.QueryCount++
	if len(result) > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)

	return result, nil
}

// Clear 清空索引
func (idx *BitmapIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 清空数据
	idx.data = make(map[string]map[string][]uint64)
	idx.seriesIDToPos = make(map[string]int)
	idx.posToSeriesID = make([]string, 0)
	idx.nextPos = 0

	// 更新统计信息
	idx.stats.ItemCount = 0
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Stats 返回索引统计信息
func (idx *BitmapIndex) Stats(ctx context.Context) (IndexStats, error) {
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
		for _, bitmap := range fieldData {
			sizeBytes += int64(len(bitmap) * 8) // 每个uint64占8字节
		}
	}
	sizeBytes += int64(len(idx.seriesIDToPos) * 16) // 每个映射项估计16字节
	sizeBytes += int64(len(idx.posToSeriesID) * 16) // 每个字符串指针估计16字节
	idx.stats.SizeBytes = sizeBytes

	return idx.stats, nil
}

// Close 关闭索引
func (idx *BitmapIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空数据
	idx.data = nil
	idx.seriesIDToPos = nil
	idx.posToSeriesID = nil

	return nil
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
