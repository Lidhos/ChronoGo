package storage

import (
	"encoding/json"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/model"
)

// MemTable 表示内存表
type MemTable struct {
	data       *SkipList                     // 跳表存储
	tagIndexes map[string]map[string][]int64 // 标签索引
	size       int64                         // 当前大小
	maxSize    int64                         // 触发刷盘阈值
	lastFlush  time.Time                     // 上次刷盘时间
	mu         sync.RWMutex                  // 读写锁
}

// NewMemTable 创建新的内存表
func NewMemTable(maxSize int64) *MemTable {
	return &MemTable{
		data:       NewSkipList(),
		tagIndexes: make(map[string]map[string][]int64),
		maxSize:    maxSize,
		lastFlush:  time.Now(),
	}
}

// Put 插入时序数据点
func (mt *MemTable) Put(point *model.TimeSeriesPoint) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// 将数据点转换为BSON
	doc := point.ToBSON()

	// 序列化为二进制
	data, err := bson.Marshal(doc)
	if err != nil {
		return err
	}

	// 插入跳表
	mt.data.Insert(point.Timestamp, data)

	// 更新标签索引
	for tagKey, tagValue := range point.Tags {
		if _, exists := mt.tagIndexes[tagKey]; !exists {
			mt.tagIndexes[tagKey] = make(map[string][]int64)
		}
		mt.tagIndexes[tagKey][tagValue] = append(mt.tagIndexes[tagKey][tagValue], point.Timestamp)
	}

	// 更新大小
	mt.size += int64(len(data))

	return nil
}

// Get 获取指定时间戳的数据点
func (mt *MemTable) Get(timestamp int64) (*model.TimeSeriesPoint, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 从跳表获取数据
	data, found := mt.data.Get(timestamp)
	if !found {
		return nil, false
	}

	// 反序列化
	var doc bson.D
	if err := bson.Unmarshal(data, &doc); err != nil {
		return nil, false
	}

	// 转换为时序数据点
	point, err := model.FromBSON(doc)
	if err != nil {
		return nil, false
	}

	return point, true
}

// QueryByTimeRange 按时间范围查询
func (mt *MemTable) QueryByTimeRange(start, end int64) []*model.TimeSeriesPoint {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 从跳表获取范围数据
	dataList := mt.data.Range(start, end)

	result := make([]*model.TimeSeriesPoint, 0, len(dataList))

	// 反序列化每个数据点
	for _, data := range dataList {
		var doc bson.D
		if err := bson.Unmarshal(data, &doc); err != nil {
			continue
		}

		point, err := model.FromBSON(doc)
		if err != nil {
			continue
		}

		result = append(result, point)
	}

	return result
}

// QueryByTag 按标签查询
func (mt *MemTable) QueryByTag(tagKey, tagValue string) []*model.TimeSeriesPoint {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 检查标签索引是否存在
	if _, exists := mt.tagIndexes[tagKey]; !exists {
		return nil
	}

	timestamps, exists := mt.tagIndexes[tagKey][tagValue]
	if !exists {
		return nil
	}

	result := make([]*model.TimeSeriesPoint, 0, len(timestamps))

	// 获取每个时间戳对应的数据点
	for _, ts := range timestamps {
		data, found := mt.data.Get(ts)
		if !found {
			continue
		}

		var doc bson.D
		if err := bson.Unmarshal(data, &doc); err != nil {
			continue
		}

		point, err := model.FromBSON(doc)
		if err != nil {
			continue
		}

		result = append(result, point)
	}

	return result
}

// ShouldFlush 检查是否应该刷盘
func (mt *MemTable) ShouldFlush() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 如果大小超过阈值，或者距离上次刷盘时间超过一定阈值，则需要刷盘
	return mt.size >= mt.maxSize || time.Since(mt.lastFlush) > 10*time.Minute
}

// Size 返回内存表大小
func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// Len 返回内存表中的数据点数量
func (mt *MemTable) Len() int64 {
	return mt.data.Len()
}

// Clear 清空内存表
func (mt *MemTable) Clear() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.data.Clear()
	mt.tagIndexes = make(map[string]map[string][]int64)
	mt.size = 0
	mt.lastFlush = time.Now()
}

// ToJSON 将内存表转换为JSON格式（用于调试）
func (mt *MemTable) ToJSON() ([]byte, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 获取所有数据点
	dataList := mt.data.Range(0, 1<<63-1)

	points := make([]*model.TimeSeriesPoint, 0, len(dataList))

	// 反序列化每个数据点
	for _, data := range dataList {
		var doc bson.D
		if err := bson.Unmarshal(data, &doc); err != nil {
			continue
		}

		point, err := model.FromBSON(doc)
		if err != nil {
			continue
		}

		points = append(points, point)
	}

	return json.Marshal(points)
}

// GetAll 获取内存表中的所有数据点
func (mt *MemTable) GetAll() []*model.TimeSeriesPoint {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 获取所有数据点
	dataList := mt.data.Range(0, 1<<63-1)

	points := make([]*model.TimeSeriesPoint, 0, len(dataList))

	// 反序列化每个数据点
	for _, data := range dataList {
		var doc bson.D
		if err := bson.Unmarshal(data, &doc); err != nil {
			continue
		}

		point, err := model.FromBSON(doc)
		if err != nil {
			continue
		}

		points = append(points, point)
	}

	return points
}
