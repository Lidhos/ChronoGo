package index

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TimeIndexLevel 时间索引级别
type TimeIndexLevel byte

const (
	// TimeIndexLevelYear 年级别
	TimeIndexLevelYear TimeIndexLevel = 1
	// TimeIndexLevelMonth 月级别
	TimeIndexLevelMonth TimeIndexLevel = 2
	// TimeIndexLevelDay 日级别
	TimeIndexLevelDay TimeIndexLevel = 3
	// TimeIndexLevelHour 小时级别
	TimeIndexLevelHour TimeIndexLevel = 4
	// TimeIndexLevelMinute 分钟级别
	TimeIndexLevelMinute TimeIndexLevel = 5
	// TimeIndexLevelSecond 秒级别
	TimeIndexLevelSecond TimeIndexLevel = 6
)

// TimeIndexOptions 时间索引选项
type TimeIndexOptions struct {
	// 索引名称
	Name string
	// 时间字段
	TimeField string
	// 索引级别
	Level TimeIndexLevel
	// 时间单位（纳秒、微秒、毫秒、秒）
	TimeUnit string
}

// TimeIndex 时间索引实现
type TimeIndex struct {
	// 索引名称
	name string
	// 时间字段
	timeField string
	// 索引级别
	level TimeIndexLevel
	// 时间单位
	timeUnit string
	// 索引数据 level -> time -> seriesIDs
	data map[TimeIndexLevel]map[int64]SeriesIDSet
	// 统计信息
	stats IndexStats
	// 互斥锁
	mu sync.RWMutex
}

// NewTimeIndex 创建新的时间索引
func NewTimeIndex(options TimeIndexOptions) *TimeIndex {
	return &TimeIndex{
		name:      options.Name,
		timeField: options.TimeField,
		level:     options.Level,
		timeUnit:  options.TimeUnit,
		data:      make(map[TimeIndexLevel]map[int64]SeriesIDSet),
		stats: IndexStats{
			Name:           options.Name,
			Type:           IndexTypeBTree, // 时间索引基于B+树
			LastUpdateTime: time.Now().UnixNano(),
		},
	}
}

// Name 返回索引名称
func (idx *TimeIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *TimeIndex) Type() IndexType {
	return IndexTypeBTree // 时间索引基于B+树
}

// Fields 返回索引字段
func (idx *TimeIndex) Fields() []string {
	return []string{idx.timeField}
}

// truncateTime 根据索引级别截断时间
func (idx *TimeIndex) truncateTime(timestamp int64) int64 {
	// 转换为时间
	t := time.Unix(0, timestamp)

	// 根据索引级别截断时间
	switch idx.level {
	case TimeIndexLevelYear:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelMinute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelSecond:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()).UnixNano()
	default:
		return timestamp
	}
}

// Insert 插入索引项
func (idx *TimeIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为时间戳
	timestamp, ok := key.(int64)
	if !ok {
		return fmt.Errorf("key must be int64, got %T", key)
	}

	// 转换时间单位
	switch idx.timeUnit {
	case "s":
		timestamp *= 1e9 // 秒转纳秒
	case "ms":
		timestamp *= 1e6 // 毫秒转纳秒
	case "us":
		timestamp *= 1e3 // 微秒转纳秒
	}

	// 创建多级索引
	for level := TimeIndexLevelYear; level <= idx.level; level++ {
		// 确保级别存在
		if _, ok := idx.data[level]; !ok {
			idx.data[level] = make(map[int64]SeriesIDSet)
		}

		// 截断时间
		truncatedTime := idx.truncateTime(timestamp)

		// 添加到索引
		if _, ok := idx.data[level][truncatedTime]; !ok {
			idx.data[level][truncatedTime] = NewSeriesIDSet()
		}
		idx.data[level][truncatedTime].Add(SeriesID(seriesID))
	}

	// 更新统计信息
	idx.stats.ItemCount++
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Remove 删除索引项
func (idx *TimeIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为时间戳
	timestamp, ok := key.(int64)
	if !ok {
		return fmt.Errorf("key must be int64, got %T", key)
	}

	// 转换时间单位
	switch idx.timeUnit {
	case "s":
		timestamp *= 1e9 // 秒转纳秒
	case "ms":
		timestamp *= 1e6 // 毫秒转纳秒
	case "us":
		timestamp *= 1e3 // 微秒转纳秒
	}

	// 从多级索引中删除
	for level := TimeIndexLevelYear; level <= idx.level; level++ {
		// 确保级别存在
		if _, ok := idx.data[level]; !ok {
			continue
		}

		// 截断时间
		truncatedTime := idx.truncateTime(timestamp)

		// 从索引中删除
		if ids, ok := idx.data[level][truncatedTime]; ok {
			ids.Remove(SeriesID(seriesID))
			// 如果集合为空，删除键
			if ids.Size() == 0 {
				delete(idx.data[level], truncatedTime)
			}
		}
	}

	// 更新统计信息
	idx.stats.ItemCount--
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Update 更新索引项
func (idx *TimeIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧的，再插入新的
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *TimeIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
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

	// 检查字段是否匹配
	if condition.Field != idx.timeField {
		return nil, fmt.Errorf("field %s does not match time field %s", condition.Field, idx.timeField)
	}

	var result SeriesIDSet

	// 处理不同的操作符
	switch condition.Operator {
	case "=", "==":
		// 精确匹配
		timestamp, ok := condition.Value.(int64)
		if !ok {
			return nil, fmt.Errorf("value must be int64 for = operator, got %T", condition.Value)
		}

		// 转换时间单位
		switch idx.timeUnit {
		case "s":
			timestamp *= 1e9 // 秒转纳秒
		case "ms":
			timestamp *= 1e6 // 毫秒转纳秒
		case "us":
			timestamp *= 1e3 // 微秒转纳秒
		}

		// 截断时间
		truncatedTime := idx.truncateTime(timestamp)

		// 查找匹配的时间
		if _, ok := idx.data[idx.level]; ok {
			if ids, ok := idx.data[idx.level][truncatedTime]; ok {
				result = ids
			} else {
				result = NewSeriesIDSet()
			}
		} else {
			result = NewSeriesIDSet()
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s for time index", condition.Operator)
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
func (idx *TimeIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 记录查询开始时间
	queryStartTime := time.Now().UnixNano()

	// 将key转换为时间戳
	startTimestamp, ok := startKey.(int64)
	if !ok {
		return nil, fmt.Errorf("startKey must be int64, got %T", startKey)
	}
	endTimestamp, ok := endKey.(int64)
	if !ok {
		return nil, fmt.Errorf("endKey must be int64, got %T", endKey)
	}

	// 转换时间单位
	switch idx.timeUnit {
	case "s":
		startTimestamp *= 1e9 // 秒转纳秒
		endTimestamp *= 1e9
	case "ms":
		startTimestamp *= 1e6 // 毫秒转纳秒
		endTimestamp *= 1e6
	case "us":
		startTimestamp *= 1e3 // 微秒转纳秒
		endTimestamp *= 1e3
	}

	// 选择最佳索引级别
	level := idx.selectBestLevel(startTimestamp, endTimestamp)

	// 确保级别存在
	if _, ok := idx.data[level]; !ok {
		// 更新统计信息
		idx.stats.QueryCount++
		idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - queryStartTime) / (idx.stats.QueryCount + 1)
		return []string{}, nil
	}

	// 截断时间
	truncatedStartTime := idx.truncateTimeByLevel(startTimestamp, level)
	truncatedEndTime := idx.truncateTimeByLevel(endTimestamp, level)

	// 查找范围内的所有时间
	result := NewSeriesIDSet()
	for t, ids := range idx.data[level] {
		// 检查是否在范围内
		if (includeStart && t >= truncatedStartTime || !includeStart && t > truncatedStartTime) &&
			(includeEnd && t <= truncatedEndTime || !includeEnd && t < truncatedEndTime) {
			result = result.Union(ids)
		}
	}

	// 更新统计信息
	idx.stats.QueryCount++
	if result.Size() > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - queryStartTime) / (idx.stats.QueryCount + 1)

	return result.ToSlice(), nil
}

// selectBestLevel 选择最佳索引级别
func (idx *TimeIndex) selectBestLevel(startTimestamp, endTimestamp int64) TimeIndexLevel {
	// 计算时间范围
	duration := endTimestamp - startTimestamp

	// 根据时间范围选择最佳级别
	if duration >= 365*24*60*60*1e9 { // 1年以上
		return TimeIndexLevelYear
	} else if duration >= 30*24*60*60*1e9 { // 1个月以上
		return TimeIndexLevelMonth
	} else if duration >= 24*60*60*1e9 { // 1天以上
		return TimeIndexLevelDay
	} else if duration >= 60*60*1e9 { // 1小时以上
		return TimeIndexLevelHour
	} else if duration >= 60*1e9 { // 1分钟以上
		return TimeIndexLevelMinute
	} else {
		return TimeIndexLevelSecond
	}
}

// truncateTimeByLevel 根据指定级别截断时间
func (idx *TimeIndex) truncateTimeByLevel(timestamp int64, level TimeIndexLevel) int64 {
	// 转换为时间
	t := time.Unix(0, timestamp)

	// 根据索引级别截断时间
	switch level {
	case TimeIndexLevelYear:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelMinute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()).UnixNano()
	case TimeIndexLevelSecond:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()).UnixNano()
	default:
		return timestamp
	}
}

// Clear 清空索引
func (idx *TimeIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 清空数据
	idx.data = make(map[TimeIndexLevel]map[int64]SeriesIDSet)

	// 更新统计信息
	idx.stats.ItemCount = 0
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Stats 返回索引统计信息
func (idx *TimeIndex) Stats(ctx context.Context) (IndexStats, error) {
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
	var itemCount int64
	for _, levelData := range idx.data {
		for _, ids := range levelData {
			sizeBytes += int64(ids.Size() * 16) // 每个ID估计16字节
			itemCount += int64(ids.Size())
		}
	}
	idx.stats.SizeBytes = sizeBytes
	idx.stats.ItemCount = itemCount

	return idx.stats, nil
}

// Close 关闭索引
func (idx *TimeIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空数据
	idx.data = nil

	return nil
}
