package storage

import (
	"encoding/json"
	"runtime"
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

// PutBatch 批量插入多个时序数据点
// 一次性获取锁，减少锁竞争
func (mt *MemTable) PutBatch(points []*model.TimeSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// 预分配足够大小的缓冲区，减少内存重新分配
	docBuffers := make([][]byte, len(points))

	// 并行序列化数据点
	var wg sync.WaitGroup
	var mu sync.Mutex
	var serializationErr error

	// 使用工作池限制并发数
	workerCount := runtime.NumCPU()
	if workerCount > 4 {
		workerCount = 4 // 最多4个工作线程
	}

	// 创建工作通道
	type workItem struct {
		index int
		point *model.TimeSeriesPoint
	}
	workChan := make(chan workItem, len(points))

	// 提交工作
	for i, point := range points {
		workChan <- workItem{index: i, point: point}
	}
	close(workChan)

	// 启动工作线程
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()

			for work := range workChan {
				// 使用 MarshalBSON 方法，它内部使用对象池
				data, err := work.point.MarshalBSON()
				if err != nil {
					mu.Lock()
					serializationErr = err
					mu.Unlock()
					continue
				}

				mu.Lock()
				docBuffers[work.index] = data
				mu.Unlock()
			}
		}()
	}

	// 等待所有序列化完成
	wg.Wait()

	// 检查序列化错误
	if serializationErr != nil {
		return serializationErr
	}

	// 批量插入数据
	for i, point := range points {
		// 插入跳表
		mt.data.Insert(point.Timestamp, docBuffers[i])
		mt.size++
	}

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

// Query 使用BSON过滤器查询数据点
func (mt *MemTable) Query(filter bson.D) ([]*model.TimeSeriesPoint, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// 获取所有数据点
	allPoints := mt.GetAll()

	// 如果没有过滤器，返回所有数据点
	if len(filter) == 0 {
		return allPoints, nil
	}

	// 应用过滤器
	var result []*model.TimeSeriesPoint
	for _, point := range allPoints {
		// 检查是否匹配过滤器
		if matchesFilter(point, filter) {
			result = append(result, point)
		}
	}

	return result, nil
}

// matchesFilter 检查数据点是否匹配BSON过滤器
func matchesFilter(point *model.TimeSeriesPoint, filter bson.D) bool {
	for _, elem := range filter {
		switch elem.Key {
		case "timestamp":
			// 处理时间戳过滤
			if !matchesTimestamp(point.Timestamp, elem.Value) {
				return false
			}
		case "tags":
			// 处理标签过滤
			if !matchesTags(point.Tags, elem.Value) {
				return false
			}
		default:
			// 处理字段过滤
			if !matchesField(point.Fields, elem.Key, elem.Value) {
				return false
			}
		}
	}

	return true
}

// matchesTimestamp 检查时间戳是否匹配过滤条件
func matchesTimestamp(timestamp int64, condition interface{}) bool {
	// 处理简单相等
	if ts, ok := condition.(int64); ok {
		return timestamp == ts
	}

	// 处理复杂条件 (如 $gt, $lt 等)
	if condMap, ok := condition.(bson.D); ok {
		for _, cond := range condMap {
			switch cond.Key {
			case "$gt":
				if val, ok := cond.Value.(int64); ok {
					if !(timestamp > val) {
						return false
					}
				}
			case "$gte":
				if val, ok := cond.Value.(int64); ok {
					if !(timestamp >= val) {
						return false
					}
				}
			case "$lt":
				if val, ok := cond.Value.(int64); ok {
					if !(timestamp < val) {
						return false
					}
				}
			case "$lte":
				if val, ok := cond.Value.(int64); ok {
					if !(timestamp <= val) {
						return false
					}
				}
			case "$eq":
				if val, ok := cond.Value.(int64); ok {
					if !(timestamp == val) {
						return false
					}
				}
			case "$ne":
				if val, ok := cond.Value.(int64); ok {
					if !(timestamp != val) {
						return false
					}
				}
			}
		}
		return true
	}

	// 默认不匹配
	return false
}

// matchesTags 检查标签是否匹配过滤条件
func matchesTags(tags map[string]string, condition interface{}) bool {
	// 处理标签条件
	if tagCond, ok := condition.(bson.D); ok {
		for _, cond := range tagCond {
			tagValue, exists := tags[cond.Key]
			if !exists {
				return false
			}

			// 处理简单相等
			if val, ok := cond.Value.(string); ok {
				if tagValue != val {
					return false
				}
			}
		}
		return true
	}

	// 默认不匹配
	return false
}

// matchesField 检查字段是否匹配过滤条件
func matchesField(fields map[string]interface{}, key string, condition interface{}) bool {
	fieldValue, exists := fields[key]
	if !exists {
		return false
	}

	// 处理简单相等
	if fieldValue == condition {
		return true
	}

	// 处理复杂条件 (如 $gt, $lt 等)
	if condMap, ok := condition.(bson.D); ok {
		for _, cond := range condMap {
			switch cond.Key {
			case "$gt":
				// 尝试数值比较
				if !compareGreaterThan(fieldValue, cond.Value) {
					return false
				}
			case "$gte":
				// 尝试数值比较
				if !compareGreaterThanOrEqual(fieldValue, cond.Value) {
					return false
				}
			case "$lt":
				// 尝试数值比较
				if !compareLessThan(fieldValue, cond.Value) {
					return false
				}
			case "$lte":
				// 尝试数值比较
				if !compareLessThanOrEqual(fieldValue, cond.Value) {
					return false
				}
			case "$eq":
				if fieldValue != cond.Value {
					return false
				}
			case "$ne":
				if fieldValue == cond.Value {
					return false
				}
			}
		}
		return true
	}

	// 默认不匹配
	return false
}

// compareGreaterThan 比较是否大于
func compareGreaterThan(a, b interface{}) bool {
	// 尝试转换为float64进行比较
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		return aFloat > bFloat
	}

	// 无法比较
	return false
}

// compareGreaterThanOrEqual 比较是否大于等于
func compareGreaterThanOrEqual(a, b interface{}) bool {
	// 尝试转换为float64进行比较
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		return aFloat >= bFloat
	}

	// 无法比较
	return false
}

// compareLessThan 比较是否小于
func compareLessThan(a, b interface{}) bool {
	// 尝试转换为float64进行比较
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		return aFloat < bFloat
	}

	// 无法比较
	return false
}

// compareLessThanOrEqual 比较是否小于等于
func compareLessThanOrEqual(a, b interface{}) bool {
	// 尝试转换为float64进行比较
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		return aFloat <= bFloat
	}

	// 无法比较
	return false
}

// toFloat64 尝试将值转换为float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}
