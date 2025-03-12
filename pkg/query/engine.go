package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/model"
	"ChronoGo/pkg/storage"
)

// QueryEngine 表示查询处理引擎
type QueryEngine struct {
	storage *storage.StorageEngine

	// 查询缓存
	queryCache    map[string]*QueryCacheEntry
	cacheMu       sync.RWMutex
	cacheEnabled  bool
	cacheCapacity int
	cacheTTL      time.Duration
}

// QueryCacheEntry 表示查询缓存条目
type QueryCacheEntry struct {
	Result    *QueryResult
	Timestamp time.Time
	HitCount  int
}

// NewQueryEngine 创建新的查询引擎
func NewQueryEngine(storage *storage.StorageEngine) *QueryEngine {
	return &QueryEngine{
		storage:       storage,
		queryCache:    make(map[string]*QueryCacheEntry),
		cacheEnabled:  true,
		cacheCapacity: 1000,
		cacheTTL:      5 * time.Minute,
	}
}

// SetCacheEnabled 设置是否启用缓存
func (e *QueryEngine) SetCacheEnabled(enabled bool) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.cacheEnabled = enabled
}

// SetCacheCapacity 设置缓存容量
func (e *QueryEngine) SetCacheCapacity(capacity int) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.cacheCapacity = capacity
	e.evictCacheIfNeeded()
}

// SetCacheTTL 设置缓存TTL
func (e *QueryEngine) SetCacheTTL(ttl time.Duration) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.cacheTTL = ttl
}

// ClearCache 清空缓存
func (e *QueryEngine) ClearCache() {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.queryCache = make(map[string]*QueryCacheEntry)
}

// evictCacheIfNeeded 如果缓存超过容量，则驱逐缓存
func (e *QueryEngine) evictCacheIfNeeded() {
	if len(e.queryCache) <= e.cacheCapacity {
		return
	}

	// 按照最后访问时间和命中次数排序
	type cacheItem struct {
		key       string
		timestamp time.Time
		hitCount  int
	}

	items := make([]cacheItem, 0, len(e.queryCache))
	for k, v := range e.queryCache {
		items = append(items, cacheItem{
			key:       k,
			timestamp: v.Timestamp,
			hitCount:  v.HitCount,
		})
	}

	// 按照最后访问时间排序，保留最近访问的
	sort.Slice(items, func(i, j int) bool {
		// 如果命中次数相差很大，优先保留命中次数高的
		if items[i].hitCount > items[j].hitCount*2 {
			return false
		}
		if items[j].hitCount > items[i].hitCount*2 {
			return true
		}

		// 否则按照时间排序
		return items[i].timestamp.Before(items[j].timestamp)
	})

	// 删除最旧的条目，直到缓存大小符合容量
	for i := 0; i < len(items)-e.cacheCapacity; i++ {
		delete(e.queryCache, items[i].key)
	}
}

// cleanExpiredCache 清理过期缓存
func (e *QueryEngine) getCacheKey(query *Query) string {
	// 将查询转换为字符串作为缓存键
	data, _ := json.Marshal(query)
	return string(data)
}

// Query 表示查询
type Query struct {
	Database     string
	Table        string
	TimeRange    TimeRange
	TagFilters   []TagFilter
	FieldFilters []FieldFilter
	Aggregations []Aggregation
	GroupBy      []string
	OrderBy      []OrderBy
	Limit        int
	Offset       int
}

// TimeRange 表示时间范围
type TimeRange struct {
	Start int64
	End   int64
}

// TagFilter 表示标签过滤器
type TagFilter struct {
	Key      string
	Operator string
	Value    interface{}
}

// FieldFilter 表示字段过滤器
type FieldFilter struct {
	Key      string
	Operator string
	Value    interface{}
}

// Aggregation 表示聚合操作
type Aggregation struct {
	Function string
	Field    string
	Alias    string
}

// OrderBy 表示排序
type OrderBy struct {
	Field     string
	Direction string // "asc" 或 "desc"
}

// QueryResult 表示查询结果
type QueryResult struct {
	Points []model.TimeSeriesPoint
}

// QueryParser 表示查询解析器
type QueryParser struct{}

// NewQueryParser 创建新的查询解析器
func NewQueryParser() *QueryParser {
	return &QueryParser{}
}

// ParseQuery 解析BSON查询
func (p *QueryParser) ParseQuery(db, table string, filter bson.D) (*Query, error) {
	query := &Query{
		Database: db,
		Table:    table,
	}

	// 解析过滤条件
	for _, elem := range filter {
		switch elem.Key {
		case "timestamp":
			// 解析时间范围
			if timeFilter, ok := elem.Value.(bson.D); ok {
				timeRange, err := p.parseTimeRange(timeFilter)
				if err != nil {
					return nil, err
				}
				query.TimeRange = timeRange
			}
		default:
			// 检查是否是标签过滤器
			if isTagFilter(elem.Key) {
				tagFilter, err := p.parseTagFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.TagFilters = append(query.TagFilters, tagFilter)
			} else {
				// 字段过滤器
				fieldFilter, err := p.parseFieldFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.FieldFilters = append(query.FieldFilters, fieldFilter)
			}
		}
	}

	return query, nil
}

// parseTimeRange 解析时间范围
func (p *QueryParser) parseTimeRange(filter bson.D) (TimeRange, error) {
	var start, end int64

	for _, elem := range filter {
		switch elem.Key {
		case "$gte", "$gt":
			if ts, ok := getTimestamp(elem.Value); ok {
				start = ts
			}
		case "$lte", "$lt":
			if ts, ok := getTimestamp(elem.Value); ok {
				end = ts
			}
		}
	}

	// 如果没有指定结束时间，使用当前时间
	if end == 0 {
		end = time.Now().UnixNano()
	}

	return TimeRange{Start: start, End: end}, nil
}

// parseTagFilter 解析标签过滤器
func (p *QueryParser) parseTagFilter(key string, value interface{}) (TagFilter, error) {
	// 简单实现，只支持等于操作
	return TagFilter{
		Key:      key,
		Operator: "=",
		Value:    value,
	}, nil
}

// parseFieldFilter 解析字段过滤器
func (p *QueryParser) parseFieldFilter(key string, value interface{}) (FieldFilter, error) {
	// 简单实现，只支持等于操作
	return FieldFilter{
		Key:      key,
		Operator: "=",
		Value:    value,
	}, nil
}

// isTagFilter 检查是否是标签过滤器
func isTagFilter(key string) bool {
	// 简单实现，假设以"tags."开头的是标签过滤器
	return len(key) > 5 && key[:5] == "tags."
}

// getTimestamp 获取时间戳
func getTimestamp(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case time.Time:
		return v.UnixNano(), true
	default:
		return 0, false
	}
}

// Execute 执行查询
func (e *QueryEngine) Execute(ctx context.Context, query *Query) (*QueryResult, error) {
	// 检查时间范围
	if query.TimeRange.End < query.TimeRange.Start {
		return nil, fmt.Errorf("invalid time range: end time is before start time")
	}

	// 尝试从缓存中获取结果
	if e.cacheEnabled {
		cacheKey := e.getCacheKey(query)

		e.cacheMu.RLock()
		entry, ok := e.queryCache[cacheKey]
		e.cacheMu.RUnlock()

		if ok {
			// 检查是否过期
			if time.Since(entry.Timestamp) <= e.cacheTTL {
				// 更新命中次数和时间戳
				e.cacheMu.Lock()
				entry.HitCount++
				entry.Timestamp = time.Now()
				e.cacheMu.Unlock()

				// 返回缓存结果
				return entry.Result, nil
			}
		}
	}

	// 执行时间范围查询
	points, err := e.storage.QueryByTimeRange(query.Database, query.Table, query.TimeRange.Start, query.TimeRange.End)
	if err != nil {
		return nil, fmt.Errorf("failed to query by time range: %w", err)
	}

	// 应用标签过滤器
	if len(query.TagFilters) > 0 {
		points = e.applyTagFilters(points, query.TagFilters)
	}

	// 应用字段过滤器
	if len(query.FieldFilters) > 0 {
		points = e.applyFieldFilters(points, query.FieldFilters)
	}

	// 应用分页
	if query.Offset > 0 && query.Offset < len(points) {
		points = points[query.Offset:]
	}

	if query.Limit > 0 && query.Limit < len(points) {
		points = points[:query.Limit]
	}

	// 创建结果
	result := &QueryResult{
		Points: make([]model.TimeSeriesPoint, len(points)),
	}

	for i, p := range points {
		result.Points[i] = *p
	}

	// 缓存结果
	if e.cacheEnabled {
		cacheKey := e.getCacheKey(query)

		e.cacheMu.Lock()
		e.queryCache[cacheKey] = &QueryCacheEntry{
			Result:    result,
			Timestamp: time.Now(),
			HitCount:  1,
		}
		e.evictCacheIfNeeded()
		e.cacheMu.Unlock()
	}

	return result, nil
}

// applyTagFilters 应用标签过滤器
func (e *QueryEngine) applyTagFilters(points []*model.TimeSeriesPoint, filters []TagFilter) []*model.TimeSeriesPoint {
	var result []*model.TimeSeriesPoint

	for _, point := range points {
		match := true
		for _, filter := range filters {
			// 提取实际的标签键（去掉"tags."前缀）
			tagKey := filter.Key
			if len(tagKey) > 5 && tagKey[:5] == "tags." {
				tagKey = tagKey[5:]
			}

			value, exists := point.Tags[tagKey]
			if !exists {
				match = false
				break
			}

			// 简单实现，只支持等于操作
			if filter.Operator == "=" && value != filter.Value {
				match = false
				break
			}
		}

		if match {
			result = append(result, point)
		}
	}

	return result
}

// applyFieldFilters 应用字段过滤器
func (e *QueryEngine) applyFieldFilters(points []*model.TimeSeriesPoint, filters []FieldFilter) []*model.TimeSeriesPoint {
	var result []*model.TimeSeriesPoint

	for _, point := range points {
		match := true
		for _, filter := range filters {
			value, exists := point.Fields[filter.Key]
			if !exists {
				match = false
				break
			}

			// 简单实现，只支持等于操作
			if filter.Operator == "=" && value != filter.Value {
				match = false
				break
			}
		}

		if match {
			result = append(result, point)
		}
	}

	return result
}

// ExecuteAggregation 执行聚合查询
func (e *QueryEngine) ExecuteAggregation(ctx context.Context, query *Query) (bson.A, error) {
	// 执行基本查询获取原始数据点
	result, err := e.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	// 如果没有聚合操作，直接返回
	if len(query.Aggregations) == 0 && len(query.GroupBy) == 0 {
		// 转换为BSON数组
		docs := make(bson.A, 0, len(result.Points))
		for _, point := range result.Points {
			docs = append(docs, point.ToBSON())
		}
		return docs, nil
	}

	// 按分组字段分组
	groups := make(map[string][]model.TimeSeriesPoint)

	if len(query.GroupBy) == 0 {
		// 如果没有分组字段，所有点放在一个组
		groups["_all"] = result.Points
	} else {
		// 按分组字段分组
		for _, point := range result.Points {
			// 构建分组键
			groupKey := ""
			for _, field := range query.GroupBy {
				// 检查是否是标签字段
				if strings.HasPrefix(field, "tags.") {
					tagKey := field[5:] // 去掉"tags."前缀
					if tagValue, exists := point.Tags[tagKey]; exists {
						groupKey += tagKey + ":" + tagValue + ","
					}
				} else {
					// 检查是否是普通字段
					if fieldValue, exists := point.Fields[field]; exists {
						groupKey += field + ":" + fmt.Sprintf("%v", fieldValue) + ","
					}
				}
			}

			// 添加到对应的组
			groups[groupKey] = append(groups[groupKey], point)
		}
	}

	// 对每个组执行聚合
	resultDocs := make(bson.A, 0, len(groups))

	for groupKey, groupPoints := range groups {
		// 创建结果文档
		resultDoc := bson.D{}

		// 添加分组信息
		if groupKey != "_all" {
			// 解析分组键
			groupFields := bson.D{}
			for _, part := range strings.Split(groupKey, ",") {
				if part == "" {
					continue
				}

				kv := strings.SplitN(part, ":", 2)
				if len(kv) == 2 {
					groupFields = append(groupFields, bson.E{Key: kv[0], Value: kv[1]})
				}
			}

			resultDoc = append(resultDoc, bson.E{Key: "_id", Value: groupFields})
		} else {
			resultDoc = append(resultDoc, bson.E{Key: "_id", Value: nil})
		}

		// 执行聚合操作
		for _, agg := range query.Aggregations {
			var aggFunc AggregateFunction

			switch agg.Function {
			case "avg":
				aggFunc = &AvgFunction{}
			case "sum":
				aggFunc = &SumFunction{}
			case "min":
				aggFunc = &MinFunction{}
			case "max":
				aggFunc = &MaxFunction{}
			case "count":
				aggFunc = &CountFunction{}
			default:
				return nil, fmt.Errorf("unsupported aggregation function: %s", agg.Function)
			}

			// 聚合字段值
			for _, point := range groupPoints {
				if fieldValue, exists := point.Fields[agg.Field]; exists {
					aggFunc.Add(fieldValue)
				}
			}

			// 添加聚合结果
			resultDoc = append(resultDoc, bson.E{Key: agg.Alias, Value: aggFunc.Result()})
		}

		resultDocs = append(resultDocs, resultDoc)
	}

	return resultDocs, nil
}

// TimeWindowAggregator 表示时间窗口聚合器
type TimeWindowAggregator struct {
	windowSize    time.Duration
	functions     map[string]AggregateFunction
	groupByFields []string
}

// AggregateFunction 表示聚合函数接口
type AggregateFunction interface {
	Add(value interface{})
	Result() interface{}
	Reset()
}

// AvgFunction 表示平均值聚合函数
type AvgFunction struct {
	sum   float64
	count int
}

// Add 添加值
func (f *AvgFunction) Add(value interface{}) {
	if val, ok := toFloat64(value); ok {
		f.sum += val
		f.count++
	}
}

// Result 获取结果
func (f *AvgFunction) Result() interface{} {
	if f.count == 0 {
		return 0.0
	}
	return f.sum / float64(f.count)
}

// Reset 重置
func (f *AvgFunction) Reset() {
	f.sum = 0
	f.count = 0
}

// MaxFunction 表示最大值聚合函数
type MaxFunction struct {
	max    float64
	hasVal bool
}

// Add 添加值
func (f *MaxFunction) Add(value interface{}) {
	if val, ok := toFloat64(value); ok {
		if !f.hasVal || val > f.max {
			f.max = val
			f.hasVal = true
		}
	}
}

// Result 获取结果
func (f *MaxFunction) Result() interface{} {
	if !f.hasVal {
		return nil
	}
	return f.max
}

// Reset 重置
func (f *MaxFunction) Reset() {
	f.max = 0
	f.hasVal = false
}

// MinFunction 表示最小值聚合函数
type MinFunction struct {
	min    float64
	hasVal bool
}

// Add 添加值
func (f *MinFunction) Add(value interface{}) {
	if val, ok := toFloat64(value); ok {
		if !f.hasVal || val < f.min {
			f.min = val
			f.hasVal = true
		}
	}
}

// Result 获取结果
func (f *MinFunction) Result() interface{} {
	if !f.hasVal {
		return nil
	}
	return f.min
}

// Reset 重置
func (f *MinFunction) Reset() {
	f.min = 0
	f.hasVal = false
}

// SumFunction 表示求和聚合函数
type SumFunction struct {
	sum float64
}

// Add 添加值
func (f *SumFunction) Add(value interface{}) {
	if val, ok := toFloat64(value); ok {
		f.sum += val
	}
}

// Result 获取结果
func (f *SumFunction) Result() interface{} {
	return f.sum
}

// Reset 重置
func (f *SumFunction) Reset() {
	f.sum = 0
}

// CountFunction 表示计数聚合函数
type CountFunction struct {
	count int
}

// Add 添加值
func (f *CountFunction) Add(value interface{}) {
	f.count++
}

// Result 获取结果
func (f *CountFunction) Result() interface{} {
	return f.count
}

// Reset 重置
func (f *CountFunction) Reset() {
	f.count = 0
}

// toFloat64 将值转换为float64
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	default:
		return 0, false
	}
}

// DownsampleOptions 表示降采样选项
type DownsampleOptions struct {
	TimeWindow  time.Duration // 时间窗口大小
	Aggregation string        // 聚合函数: avg, sum, min, max, count
	FillPolicy  string        // 填充策略: none, previous, linear, zero
}

// InterpolationOptions 表示插值选项
type InterpolationOptions struct {
	Method     string        // 插值方法: linear, previous, next, zero
	MaxGap     time.Duration // 最大插值间隔
	Resolution time.Duration // 插值分辨率
}

// MovingWindowOptions 表示移动窗口选项
type MovingWindowOptions struct {
	WindowSize time.Duration // 窗口大小
	Function   string        // 窗口函数: avg, sum, min, max, count
	StepSize   time.Duration // 步长
}

// Downsample 执行降采样
func (e *QueryEngine) Downsample(points []model.TimeSeriesPoint, field string, options DownsampleOptions) ([]model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, nil
	}

	// 按时间窗口分组
	windowedPoints := make(map[int64][]model.TimeSeriesPoint)
	var minTime int64 = points[0].Timestamp

	// 找到最小时间戳
	for _, point := range points {
		if point.Timestamp < minTime {
			minTime = point.Timestamp
		}
	}

	// 将点分配到时间窗口
	windowSizeNanos := options.TimeWindow.Nanoseconds()
	for _, point := range points {
		// 计算窗口开始时间
		windowStart := (point.Timestamp-minTime)/windowSizeNanos*windowSizeNanos + minTime
		windowedPoints[windowStart] = append(windowedPoints[windowStart], point)
	}

	// 对每个窗口执行聚合
	result := make([]model.TimeSeriesPoint, 0, len(windowedPoints))
	for windowStart, windowPoints := range windowedPoints {
		// 创建聚合函数
		var aggFunc AggregateFunction
		switch options.Aggregation {
		case "avg":
			aggFunc = &AvgFunction{}
		case "sum":
			aggFunc = &SumFunction{}
		case "min":
			aggFunc = &MinFunction{}
		case "max":
			aggFunc = &MaxFunction{}
		case "count":
			aggFunc = &CountFunction{}
		default:
			return nil, fmt.Errorf("unsupported aggregation function: %s", options.Aggregation)
		}

		// 聚合窗口内的点
		for _, point := range windowPoints {
			if value, exists := point.Fields[field]; exists {
				aggFunc.Add(value)
			}
		}

		// 创建聚合结果点
		resultPoint := model.TimeSeriesPoint{
			Timestamp: windowStart,
			Tags:      make(map[string]string),
			Fields:    make(map[string]interface{}),
		}

		// 复制第一个点的标签
		for k, v := range windowPoints[0].Tags {
			resultPoint.Tags[k] = v
		}

		// 设置聚合结果
		resultPoint.Fields[field] = aggFunc.Result()

		result = append(result, resultPoint)
	}

	// 按时间戳排序
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result, nil
}

// Interpolate 执行插值
func (e *QueryEngine) Interpolate(points []model.TimeSeriesPoint, field string, options InterpolationOptions) ([]model.TimeSeriesPoint, error) {
	if len(points) < 2 {
		return points, nil
	}

	// 按时间戳排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// 计算最大间隔（纳秒）
	maxGapNanos := options.MaxGap.Nanoseconds()
	resolutionNanos := options.Resolution.Nanoseconds()

	result := make([]model.TimeSeriesPoint, 0, len(points))
	result = append(result, points[0]) // 添加第一个点

	// 遍历所有点，检查是否需要插值
	for i := 1; i < len(points); i++ {
		current := points[i]
		previous := points[i-1]

		// 计算时间差
		timeDiff := current.Timestamp - previous.Timestamp

		// 如果时间差小于分辨率，直接添加当前点
		if timeDiff <= resolutionNanos {
			result = append(result, current)
			continue
		}

		// 如果时间差大于最大间隔，不进行插值
		if maxGapNanos > 0 && timeDiff > maxGapNanos {
			result = append(result, current)
			continue
		}

		// 获取前一个点和当前点的字段值
		prevValue, prevExists := previous.Fields[field]
		currValue, currExists := current.Fields[field]

		// 如果任一点没有该字段，跳过插值
		if !prevExists || !currExists {
			result = append(result, current)
			continue
		}

		// 转换为float64
		prevFloat, ok1 := toFloat64(prevValue)
		currFloat, ok2 := toFloat64(currValue)

		if !ok1 || !ok2 {
			result = append(result, current)
			continue
		}

		// 计算插值点数量
		numPoints := int(timeDiff / resolutionNanos)

		// 生成插值点
		for j := 1; j < numPoints; j++ {
			// 计算插值时间戳
			timestamp := previous.Timestamp + int64(j)*resolutionNanos

			// 创建插值点
			interpolatedPoint := model.TimeSeriesPoint{
				Timestamp: timestamp,
				Tags:      make(map[string]string),
				Fields:    make(map[string]interface{}),
			}

			// 复制标签
			for k, v := range previous.Tags {
				interpolatedPoint.Tags[k] = v
			}

			// 计算插值
			var interpolatedValue float64
			switch options.Method {
			case "linear":
				// 线性插值
				ratio := float64(j) / float64(numPoints)
				interpolatedValue = prevFloat + ratio*(currFloat-prevFloat)
			case "previous":
				// 前值插值
				interpolatedValue = prevFloat
			case "next":
				// 后值插值
				interpolatedValue = currFloat
			case "zero":
				// 零值插值
				interpolatedValue = 0
			default:
				return nil, fmt.Errorf("unsupported interpolation method: %s", options.Method)
			}

			// 设置插值结果
			interpolatedPoint.Fields[field] = interpolatedValue

			// 添加到结果
			result = append(result, interpolatedPoint)
		}

		// 添加当前点
		result = append(result, current)
	}

	return result, nil
}

// MovingWindow 执行移动窗口计算
func (e *QueryEngine) MovingWindow(points []model.TimeSeriesPoint, field string, options MovingWindowOptions) ([]model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, nil
	}

	// 按时间戳排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// 窗口大小和步长（纳秒）
	windowSizeNanos := options.WindowSize.Nanoseconds()
	stepSizeNanos := options.StepSize.Nanoseconds()
	if stepSizeNanos <= 0 {
		stepSizeNanos = windowSizeNanos
	}

	// 找到最小和最大时间戳
	minTime := points[0].Timestamp
	maxTime := points[len(points)-1].Timestamp

	// 创建结果数组
	result := make([]model.TimeSeriesPoint, 0)

	// 对每个窗口执行计算
	for windowEnd := minTime + windowSizeNanos; windowEnd <= maxTime+windowSizeNanos; windowEnd += stepSizeNanos {
		windowStart := windowEnd - windowSizeNanos

		// 创建聚合函数
		var aggFunc AggregateFunction
		switch options.Function {
		case "avg":
			aggFunc = &AvgFunction{}
		case "sum":
			aggFunc = &SumFunction{}
		case "min":
			aggFunc = &MinFunction{}
		case "max":
			aggFunc = &MaxFunction{}
		case "count":
			aggFunc = &CountFunction{}
		default:
			return nil, fmt.Errorf("unsupported window function: %s", options.Function)
		}

		// 找到窗口内的点
		var windowPoints []model.TimeSeriesPoint
		for _, point := range points {
			if point.Timestamp >= windowStart && point.Timestamp < windowEnd {
				windowPoints = append(windowPoints, point)
			}
		}

		// 如果窗口内没有点，跳过
		if len(windowPoints) == 0 {
			continue
		}

		// 聚合窗口内的点
		for _, point := range windowPoints {
			if value, exists := point.Fields[field]; exists {
				aggFunc.Add(value)
			}
		}

		// 创建结果点
		resultPoint := model.TimeSeriesPoint{
			Timestamp: windowEnd - windowSizeNanos/2, // 使用窗口中点作为时间戳
			Tags:      make(map[string]string),
			Fields:    make(map[string]interface{}),
		}

		// 复制第一个点的标签
		for k, v := range windowPoints[0].Tags {
			resultPoint.Tags[k] = v
		}

		// 设置聚合结果
		resultPoint.Fields[field+"_"+options.Function] = aggFunc.Result()

		result = append(result, resultPoint)
	}

	return result, nil
}

// EMAFunction 指数移动平均函数
type EMAFunction struct {
	alpha    float64 // 平滑因子
	lastEMA  float64
	hasValue bool
}

// NewEMAFunction 创建新的EMA函数
func NewEMAFunction(alpha float64) *EMAFunction {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.2 // 默认值
	}
	return &EMAFunction{
		alpha: alpha,
	}
}

// Add 添加值到EMA计算
func (f *EMAFunction) Add(value interface{}) {
	val, ok := toFloat64(value)
	if !ok {
		return
	}

	if !f.hasValue {
		// 第一个值直接作为EMA的初始值
		f.lastEMA = val
		f.hasValue = true
	} else {
		// EMA = alpha * current + (1 - alpha) * lastEMA
		f.lastEMA = f.alpha*val + (1-f.alpha)*f.lastEMA
	}
}

// Result 返回EMA结果
func (f *EMAFunction) Result() interface{} {
	if !f.hasValue {
		return nil
	}
	return f.lastEMA
}

// Reset 重置EMA计算
func (f *EMAFunction) Reset() {
	f.lastEMA = 0
	f.hasValue = false
}

// ExponentialMovingAverage 计算指数移动平均
func (e *QueryEngine) ExponentialMovingAverage(points []model.TimeSeriesPoint, field string, alpha float64) ([]model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, nil
	}

	// 按时间戳排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// 创建EMA函数
	emaFunc := NewEMAFunction(alpha)

	// 计算每个点的EMA
	result := make([]model.TimeSeriesPoint, 0, len(points))

	for _, point := range points {
		if value, exists := point.Fields[field]; exists {
			emaFunc.Add(value)

			// 创建结果点
			resultPoint := model.TimeSeriesPoint{
				Timestamp: point.Timestamp,
				Tags:      make(map[string]string),
				Fields:    make(map[string]interface{}),
			}

			// 复制标签
			for k, v := range point.Tags {
				resultPoint.Tags[k] = v
			}

			// 设置EMA结果
			resultPoint.Fields[field+"_ema"] = emaFunc.Result()

			result = append(result, resultPoint)
		}
	}

	return result, nil
}

// PercentileFunction 百分位数函数
type PercentileFunction struct {
	percentile float64
	values     []float64
}

// NewPercentileFunction 创建新的百分位数函数
func NewPercentileFunction(percentile float64) *PercentileFunction {
	if percentile < 0 || percentile > 100 {
		percentile = 95 // 默认值
	}
	return &PercentileFunction{
		percentile: percentile,
		values:     make([]float64, 0),
	}
}

// Add 添加值到百分位数计算
func (f *PercentileFunction) Add(value interface{}) {
	val, ok := toFloat64(value)
	if !ok {
		return
	}
	f.values = append(f.values, val)
}

// Result 返回百分位数结果
func (f *PercentileFunction) Result() interface{} {
	if len(f.values) == 0 {
		return nil
	}

	// 排序值
	sort.Float64s(f.values)

	// 计算百分位数索引
	idx := int(math.Ceil(float64(len(f.values))*f.percentile/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(f.values) {
		idx = len(f.values) - 1
	}

	return f.values[idx]
}

// Reset 重置百分位数计算
func (f *PercentileFunction) Reset() {
	f.values = f.values[:0]
}

// Percentile 计算百分位数
func (e *QueryEngine) Percentile(points []model.TimeSeriesPoint, field string, percentile float64, windowSize time.Duration) ([]model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, nil
	}

	// 按时间戳排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// 创建百分位数函数
	percentileFunc := NewPercentileFunction(percentile)

	// 计算窗口大小（纳秒）
	windowSizeNanos := windowSize.Nanoseconds()
	if windowSizeNanos <= 0 {
		// 如果未指定窗口大小，使用整个时间范围
		windowSizeNanos = points[len(points)-1].Timestamp - points[0].Timestamp + 1
	}

	// 按时间窗口分组
	windowedPoints := make(map[int64][]model.TimeSeriesPoint)
	var minTime int64 = points[0].Timestamp

	// 将点分配到时间窗口
	for _, point := range points {
		// 计算窗口开始时间
		windowStart := (point.Timestamp-minTime)/windowSizeNanos*windowSizeNanos + minTime
		windowedPoints[windowStart] = append(windowedPoints[windowStart], point)
	}

	// 对每个窗口计算百分位数
	result := make([]model.TimeSeriesPoint, 0, len(windowedPoints))

	for windowStart, windowPoints := range windowedPoints {
		// 重置百分位数函数
		percentileFunc.Reset()

		// 添加窗口内的所有值
		for _, point := range windowPoints {
			if value, exists := point.Fields[field]; exists {
				percentileFunc.Add(value)
			}
		}

		// 创建结果点
		resultPoint := model.TimeSeriesPoint{
			Timestamp: windowStart,
			Tags:      make(map[string]string),
			Fields:    make(map[string]interface{}),
		}

		// 复制第一个点的标签
		if len(windowPoints) > 0 {
			for k, v := range windowPoints[0].Tags {
				resultPoint.Tags[k] = v
			}
		}

		// 设置百分位数结果
		fieldName := fmt.Sprintf("%s_p%.0f", field, percentile)
		resultPoint.Fields[fieldName] = percentileFunc.Result()

		result = append(result, resultPoint)
	}

	// 按时间戳排序
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result, nil
}

// AnomalyDetectionOptions 异常检测选项
type AnomalyDetectionOptions struct {
	Method      string  // 检测方法: zscore, iqr, mad
	WindowSize  int     // 窗口大小
	Threshold   float64 // 阈值
	LookbackWin int     // 回溯窗口大小
}

// DetectAnomalies 检测异常值
func (e *QueryEngine) DetectAnomalies(points []model.TimeSeriesPoint, field string, options AnomalyDetectionOptions) ([]model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, nil
	}

	// 按时间戳排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// 提取字段值
	values := make([]float64, 0, len(points))
	for _, point := range points {
		if value, exists := point.Fields[field]; exists {
			val, ok := toFloat64(value)
			if ok {
				values = append(values, val)
			}
		}
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("no valid values for field: %s", field)
	}

	// 设置默认值
	if options.WindowSize <= 0 {
		options.WindowSize = 10
	}
	if options.LookbackWin <= 0 {
		options.LookbackWin = options.WindowSize
	}
	if options.Threshold <= 0 {
		options.Threshold = 3.0 // 默认Z-Score阈值
	}

	// 检测异常
	anomalies := make([]bool, len(values))

	switch options.Method {
	case "zscore":
		anomalies = detectAnomaliesByZScore(values, options.WindowSize, options.LookbackWin, options.Threshold)
	case "iqr":
		anomalies = detectAnomaliesByIQR(values, options.WindowSize, options.LookbackWin, options.Threshold)
	case "mad":
		anomalies = detectAnomaliesByMAD(values, options.WindowSize, options.LookbackWin, options.Threshold)
	default:
		// 默认使用Z-Score
		anomalies = detectAnomaliesByZScore(values, options.WindowSize, options.LookbackWin, options.Threshold)
	}

	// 创建结果
	result := make([]model.TimeSeriesPoint, 0, len(points))

	for i, point := range points {
		if i < len(anomalies) && anomalies[i] {
			// 创建异常点
			anomalyPoint := model.TimeSeriesPoint{
				Timestamp: point.Timestamp,
				Tags:      make(map[string]string),
				Fields:    make(map[string]interface{}),
			}

			// 复制标签
			for k, v := range point.Tags {
				anomalyPoint.Tags[k] = v
			}

			// 复制原始值
			if value, exists := point.Fields[field]; exists {
				anomalyPoint.Fields[field] = value
				anomalyPoint.Fields[field+"_anomaly"] = true
			}

			result = append(result, anomalyPoint)
		}
	}

	return result, nil
}

// detectAnomaliesByZScore 使用Z-Score检测异常
func detectAnomaliesByZScore(values []float64, windowSize, lookbackWin int, threshold float64) []bool {
	anomalies := make([]bool, len(values))

	for i := lookbackWin; i < len(values); i++ {
		// 计算窗口起始位置
		start := i - lookbackWin
		if start < 0 {
			start = 0
		}

		// 提取窗口数据
		windowData := values[start:i]

		// 计算均值和标准差
		mean := calculateMean(windowData)
		stdDev := calculateStdDev(windowData, mean)

		// 如果标准差接近0，跳过
		if stdDev < 1e-10 {
			continue
		}

		// 计算Z-Score
		zScore := math.Abs(values[i]-mean) / stdDev

		// 检测异常
		if zScore > threshold {
			anomalies[i] = true
		}
	}

	return anomalies
}

// detectAnomaliesByIQR 使用IQR（四分位距）检测异常
func detectAnomaliesByIQR(values []float64, windowSize, lookbackWin int, threshold float64) []bool {
	anomalies := make([]bool, len(values))

	for i := lookbackWin; i < len(values); i++ {
		// 计算窗口起始位置
		start := i - lookbackWin
		if start < 0 {
			start = 0
		}

		// 提取窗口数据
		windowData := make([]float64, i-start)
		copy(windowData, values[start:i])

		// 排序窗口数据
		sort.Float64s(windowData)

		// 计算四分位数
		q1 := calculatePercentile(windowData, 25)
		q3 := calculatePercentile(windowData, 75)
		iqr := q3 - q1

		// 计算上下界
		lowerBound := q1 - threshold*iqr
		upperBound := q3 + threshold*iqr

		// 检测异常
		if values[i] < lowerBound || values[i] > upperBound {
			anomalies[i] = true
		}
	}

	return anomalies
}

// detectAnomaliesByMAD 使用MAD（中位数绝对偏差）检测异常
func detectAnomaliesByMAD(values []float64, windowSize, lookbackWin int, threshold float64) []bool {
	anomalies := make([]bool, len(values))

	for i := lookbackWin; i < len(values); i++ {
		// 计算窗口起始位置
		start := i - lookbackWin
		if start < 0 {
			start = 0
		}

		// 提取窗口数据
		windowData := make([]float64, i-start)
		copy(windowData, values[start:i])

		// 排序窗口数据
		sort.Float64s(windowData)

		// 计算中位数
		median := calculatePercentile(windowData, 50)

		// 计算偏差
		deviations := make([]float64, len(windowData))
		for j, v := range windowData {
			deviations[j] = math.Abs(v - median)
		}

		// 排序偏差
		sort.Float64s(deviations)

		// 计算MAD
		mad := calculatePercentile(deviations, 50)

		// 如果MAD接近0，跳过
		if mad < 1e-10 {
			continue
		}

		// 计算异常分数
		score := math.Abs(values[i]-median) / mad

		// 检测异常
		if score > threshold {
			anomalies[i] = true
		}
	}

	return anomalies
}

// calculateMean 计算均值
func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}

	return sum / float64(len(values))
}

// calculateStdDev 计算标准差
func calculateStdDev(values []float64, mean float64) float64 {
	if len(values) <= 1 {
		return 0
	}

	sumSquaredDiff := 0.0
	for _, v := range values {
		diff := v - mean
		sumSquaredDiff += diff * diff
	}

	return math.Sqrt(sumSquaredDiff / float64(len(values)-1))
}

// calculatePercentile 计算百分位数
func calculatePercentile(sortedValues []float64, percentile float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}

	// 计算索引
	idx := percentile / 100.0 * float64(len(sortedValues)-1)
	idxLow := int(math.Floor(idx))
	idxHigh := int(math.Ceil(idx))

	if idxLow == idxHigh {
		return sortedValues[idxLow]
	}

	// 线性插值
	weight := idx - float64(idxLow)
	return sortedValues[idxLow]*(1-weight) + sortedValues[idxHigh]*weight
}

// ForecastOptions 预测选项
type ForecastOptions struct {
	Method       string        // 预测方法: linear, average, ses (简单指数平滑)
	History      int           // 用于预测的历史点数
	Horizon      int           // 预测未来点数
	Interval     time.Duration // 预测点的时间间隔
	Alpha        float64       // 指数平滑系数 (用于SES)
	SeasonLength int           // 季节性长度 (用于季节性分解)
}

// Forecast 预测时间序列未来值
func (e *QueryEngine) Forecast(points []model.TimeSeriesPoint, field string, options ForecastOptions) ([]model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, nil
	}

	// 按时间戳排序
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// 提取字段值
	values := make([]float64, 0, len(points))
	timestamps := make([]int64, 0, len(points))

	for _, point := range points {
		if value, exists := point.Fields[field]; exists {
			val, ok := toFloat64(value)
			if ok {
				values = append(values, val)
				timestamps = append(timestamps, point.Timestamp)
			}
		}
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("no valid values for field: %s", field)
	}

	// 设置默认值
	if options.History <= 0 {
		options.History = len(values)
	} else if options.History > len(values) {
		options.History = len(values)
	}

	if options.Horizon <= 0 {
		options.Horizon = 10
	}

	if options.Interval <= 0 {
		// 如果未指定间隔，使用历史数据的平均间隔
		if len(timestamps) > 1 {
			totalDuration := timestamps[len(timestamps)-1] - timestamps[0]
			options.Interval = time.Duration(totalDuration / int64(len(timestamps)-1))
		} else {
			options.Interval = time.Hour // 默认1小时
		}
	}

	if options.Alpha <= 0 || options.Alpha > 1 {
		options.Alpha = 0.2 // 默认指数平滑系数
	}

	// 使用最近的历史数据
	historyStart := len(values) - options.History
	if historyStart < 0 {
		historyStart = 0
	}

	historyValues := values[historyStart:]
	historyTimestamps := timestamps[historyStart:]

	// 预测未来值
	var forecastValues []float64

	switch options.Method {
	case "linear":
		forecastValues = forecastLinear(historyValues, historyTimestamps, options.Horizon)
	case "average":
		forecastValues = forecastAverage(historyValues, options.Horizon)
	case "ses":
		forecastValues = forecastSES(historyValues, options.Horizon, options.Alpha)
	default:
		// 默认使用线性预测
		forecastValues = forecastLinear(historyValues, historyTimestamps, options.Horizon)
	}

	// 创建预测结果
	result := make([]model.TimeSeriesPoint, options.Horizon)
	lastTimestamp := timestamps[len(timestamps)-1]
	interval := options.Interval.Nanoseconds()

	for i := 0; i < options.Horizon; i++ {
		// 计算预测点的时间戳
		forecastTimestamp := lastTimestamp + int64(i+1)*interval

		// 创建预测点
		forecastPoint := model.TimeSeriesPoint{
			Timestamp: forecastTimestamp,
			Tags:      make(map[string]string),
			Fields:    make(map[string]interface{}),
		}

		// 复制最后一个点的标签
		for k, v := range points[len(points)-1].Tags {
			forecastPoint.Tags[k] = v
		}

		// 添加预测标记
		forecastPoint.Tags["_forecast"] = "true"

		// 设置预测值
		forecastPoint.Fields[field] = forecastValues[i]

		result[i] = forecastPoint
	}

	return result, nil
}

// forecastLinear 线性预测
func forecastLinear(values []float64, timestamps []int64, horizon int) []float64 {
	result := make([]float64, horizon)

	if len(values) < 2 {
		// 如果历史数据不足，使用最后一个值
		lastValue := values[len(values)-1]
		for i := 0; i < horizon; i++ {
			result[i] = lastValue
		}
		return result
	}

	// 计算线性回归参数
	n := float64(len(values))
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0

	// 使用时间戳作为X轴
	x := make([]float64, len(timestamps))
	for i, ts := range timestamps {
		x[i] = float64(ts - timestamps[0]) // 相对于第一个时间戳的偏移
	}

	for i := 0; i < len(values); i++ {
		sumX += x[i]
		sumY += values[i]
		sumXY += x[i] * values[i]
		sumX2 += x[i] * x[i]
	}

	// 计算斜率和截距
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	intercept := (sumY - slope*sumX) / n

	// 预测未来值
	lastX := x[len(x)-1]
	interval := (lastX - x[0]) / float64(len(x)-1) // 平均间隔

	for i := 0; i < horizon; i++ {
		forecastX := lastX + interval*float64(i+1)
		result[i] = slope*forecastX + intercept
	}

	return result
}

// forecastAverage 平均值预测
func forecastAverage(values []float64, horizon int) []float64 {
	result := make([]float64, horizon)

	// 计算平均值
	avg := 0.0
	for _, v := range values {
		avg += v
	}
	avg /= float64(len(values))

	// 所有预测点使用相同的平均值
	for i := 0; i < horizon; i++ {
		result[i] = avg
	}

	return result
}

// forecastSES 简单指数平滑预测
func forecastSES(values []float64, horizon int, alpha float64) []float64 {
	result := make([]float64, horizon)

	if len(values) == 0 {
		return result
	}

	// 计算最后一个平滑值
	smoothed := values[0]
	for i := 1; i < len(values); i++ {
		smoothed = alpha*values[i] + (1-alpha)*smoothed
	}

	// 所有预测点使用相同的平滑值
	for i := 0; i < horizon; i++ {
		result[i] = smoothed
	}

	return result
}

// ExecuteTimeWindowAggregation 执行时间窗口聚合
func (e *QueryEngine) ExecuteTimeWindowAggregation(ctx context.Context, request map[string]interface{}) ([]interface{}, error) {
	// 解析请求参数
	dbName, ok := request["database"].(string)
	if !ok {
		return nil, fmt.Errorf("missing database name")
	}

	collName, ok := request["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("missing collection name")
	}

	timeField, ok := request["timeField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing time field")
	}

	windowSizeStr, ok := request["windowSize"].(string)
	if !ok {
		return nil, fmt.Errorf("missing window size")
	}

	// 解析窗口大小
	windowSize, err := parseWindowSize(windowSizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid window size: %w", err)
	}

	// 解析时间范围
	startTime, ok := request["startTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing start time")
	}

	endTime, ok := request["endTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing end time")
	}

	// 创建查询
	query := &Query{
		Database: dbName,
		Table:    collName,
		TimeRange: TimeRange{
			Start: startTime,
			End:   endTime,
		},
	}

	// 添加过滤条件
	if filter, ok := request["filter"].(bson.D); ok {
		parser := NewQueryParser()
		for _, elem := range filter {
			if isTagFilter(elem.Key) {
				tagFilter, err := parser.parseTagFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.TagFilters = append(query.TagFilters, tagFilter)
			} else if elem.Key != timeField {
				fieldFilter, err := parser.parseFieldFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.FieldFilters = append(query.FieldFilters, fieldFilter)
			}
		}
	}

	// 执行查询
	result, err := e.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	// 解析聚合函数
	aggregations := make(map[string]string)
	if aggs, ok := request["aggregations"].(bson.D); ok {
		for _, agg := range aggs {
			if function, ok := agg.Value.(string); ok {
				aggregations[agg.Key] = function
			}
		}
	}

	// 如果没有聚合函数，使用默认的平均值
	if len(aggregations) == 0 {
		aggregations["value"] = "avg"
	}

	// 创建时间窗口聚合器
	aggregator := &TimeWindowAggregator{
		windowSize: windowSize,
		functions:  make(map[string]AggregateFunction),
	}

	// 添加聚合函数
	for field, function := range aggregations {
		switch function {
		case "avg":
			aggregator.functions[field] = &AvgFunction{}
		case "max":
			aggregator.functions[field] = &MaxFunction{}
		case "min":
			aggregator.functions[field] = &MinFunction{}
		case "sum":
			aggregator.functions[field] = &SumFunction{}
		case "count":
			aggregator.functions[field] = &CountFunction{}
		default:
			return nil, fmt.Errorf("unsupported aggregation function: %s", function)
		}
	}

	// 按时间窗口分组
	windows := make(map[int64][]model.TimeSeriesPoint)
	for _, point := range result.Points {
		// 计算窗口开始时间
		windowStart := (point.Timestamp / int64(windowSize)) * int64(windowSize)
		windows[windowStart] = append(windows[windowStart], point)
	}

	// 聚合每个窗口的数据
	results := make([]interface{}, 0, len(windows))
	for windowStart, points := range windows {
		// 重置聚合函数
		for _, fn := range aggregator.functions {
			fn.Reset()
		}

		// 添加数据点
		for _, point := range points {
			for field, fn := range aggregator.functions {
				if value, ok := point.Fields[field]; ok {
					fn.Add(value)
				}
			}
		}

		// 创建结果
		resultDoc := bson.D{
			{Key: "timestamp", Value: time.Unix(0, windowStart)},
		}

		// 添加聚合结果
		for field, fn := range aggregator.functions {
			resultDoc = append(resultDoc, bson.E{Key: field, Value: fn.Result()})
		}

		results = append(results, resultDoc)
	}

	// 按时间排序
	sort.Slice(results, func(i, j int) bool {
		docI := results[i].(bson.D)
		docJ := results[j].(bson.D)

		var timeI, timeJ time.Time
		for _, elem := range docI {
			if elem.Key == "timestamp" {
				if t, ok := elem.Value.(time.Time); ok {
					timeI = t
				}
			}
		}

		for _, elem := range docJ {
			if elem.Key == "timestamp" {
				if t, ok := elem.Value.(time.Time); ok {
					timeJ = t
				}
			}
		}

		return timeI.Before(timeJ)
	})

	return results, nil
}

// ExecuteDownsample 执行降采样
func (e *QueryEngine) ExecuteDownsample(ctx context.Context, request map[string]interface{}) (int, error) {
	// 解析请求参数
	dbName, ok := request["database"].(string)
	if !ok {
		return 0, fmt.Errorf("missing database name")
	}

	sourceCollName, ok := request["sourceCollection"].(string)
	if !ok {
		return 0, fmt.Errorf("missing source collection name")
	}

	targetCollName, ok := request["targetCollection"].(string)
	if !ok {
		return 0, fmt.Errorf("missing target collection name")
	}

	timeField, ok := request["timeField"].(string)
	if !ok {
		return 0, fmt.Errorf("missing time field")
	}

	intervalStr, ok := request["interval"].(string)
	if !ok {
		return 0, fmt.Errorf("missing interval")
	}

	// 解析时间间隔
	interval, err := parseWindowSize(intervalStr)
	if err != nil {
		return 0, fmt.Errorf("invalid interval: %w", err)
	}

	// 解析时间范围
	startTime, ok := request["startTime"].(int64)
	if !ok {
		return 0, fmt.Errorf("missing start time")
	}

	endTime, ok := request["endTime"].(int64)
	if !ok {
		return 0, fmt.Errorf("missing end time")
	}

	// 创建查询
	query := &Query{
		Database: dbName,
		Table:    sourceCollName,
		TimeRange: TimeRange{
			Start: startTime,
			End:   endTime,
		},
	}

	// 添加过滤条件
	if filter, ok := request["filter"].(bson.D); ok {
		parser := NewQueryParser()
		for _, elem := range filter {
			if isTagFilter(elem.Key) {
				tagFilter, err := parser.parseTagFilter(elem.Key, elem.Value)
				if err != nil {
					return 0, err
				}
				query.TagFilters = append(query.TagFilters, tagFilter)
			} else if elem.Key != timeField {
				fieldFilter, err := parser.parseFieldFilter(elem.Key, elem.Value)
				if err != nil {
					return 0, err
				}
				query.FieldFilters = append(query.FieldFilters, fieldFilter)
			}
		}
	}

	// 执行查询
	result, err := e.Execute(ctx, query)
	if err != nil {
		return 0, err
	}

	// 解析聚合函数
	aggregations := make(map[string]string)
	if aggs, ok := request["aggregations"].(bson.D); ok {
		for _, agg := range aggs {
			if function, ok := agg.Value.(string); ok {
				aggregations[agg.Key] = function
			}
		}
	}

	// 如果没有聚合函数，使用默认的平均值
	if len(aggregations) == 0 {
		aggregations["value"] = "avg"
	}

	// 创建降采样选项
	options := DownsampleOptions{
		TimeWindow:  interval,
		Aggregation: "avg", // 默认使用平均值
		FillPolicy:  "none",
	}

	// 按时间窗口分组
	windows := make(map[int64][]model.TimeSeriesPoint)
	for _, point := range result.Points {
		// 计算窗口开始时间
		windowStart := (point.Timestamp / int64(options.TimeWindow)) * int64(options.TimeWindow)
		windows[windowStart] = append(windows[windowStart], point)
	}

	// 聚合每个窗口的数据
	downsampledPoints := make([]model.TimeSeriesPoint, 0, len(windows))
	for windowStart, points := range windows {
		// 创建新的数据点
		newPoint := model.TimeSeriesPoint{
			Timestamp: windowStart,
			Tags:      make(map[string]string),
			Fields:    make(map[string]interface{}),
		}

		// 复制标签（使用第一个点的标签）
		if len(points) > 0 {
			for k, v := range points[0].Tags {
				newPoint.Tags[k] = v
			}
		}

		// 聚合字段
		for field, function := range aggregations {
			var fn AggregateFunction

			switch function {
			case "avg":
				fn = &AvgFunction{}
			case "max":
				fn = &MaxFunction{}
			case "min":
				fn = &MinFunction{}
			case "sum":
				fn = &SumFunction{}
			case "count":
				fn = &CountFunction{}
			default:
				return 0, fmt.Errorf("unsupported aggregation function: %s", function)
			}

			// 添加数据点
			for _, point := range points {
				if value, ok := point.Fields[field]; ok {
					fn.Add(value)
				}
			}

			// 设置聚合结果
			newPoint.Fields[field] = fn.Result()
		}

		downsampledPoints = append(downsampledPoints, newPoint)
	}

	// 将降采样后的数据写入目标集合
	// 这里应该调用存储引擎的方法，但为了简化，我们直接返回点数
	// 在实际实现中，应该将downsampledPoints写入targetCollName集合
	_ = targetCollName
	return len(downsampledPoints), nil
}

// ExecuteInterpolation 执行插值
func (e *QueryEngine) ExecuteInterpolation(ctx context.Context, request map[string]interface{}) ([]interface{}, error) {
	// 解析请求参数
	dbName, ok := request["database"].(string)
	if !ok {
		return nil, fmt.Errorf("missing database name")
	}

	collName, ok := request["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("missing collection name")
	}

	timeField, ok := request["timeField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing time field")
	}

	valueField, ok := request["valueField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing value field")
	}

	method, ok := request["method"].(string)
	if !ok || method == "" {
		method = "linear" // 默认使用线性插值
	}

	var intervalDuration time.Duration
	if intervalStr, ok := request["interval"].(string); ok && intervalStr != "" {
		// 解析时间间隔
		interval, err := parseWindowSize(intervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid interval: %w", err)
		}
		intervalDuration = interval
	} else {
		// 默认使用1分钟
		intervalDuration = time.Minute
	}

	// 解析时间范围
	startTime, ok := request["startTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing start time")
	}

	endTime, ok := request["endTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing end time")
	}

	// 创建查询
	query := &Query{
		Database: dbName,
		Table:    collName,
		TimeRange: TimeRange{
			Start: startTime,
			End:   endTime,
		},
	}

	// 添加过滤条件
	if filter, ok := request["filter"].(bson.D); ok {
		parser := NewQueryParser()
		for _, elem := range filter {
			if isTagFilter(elem.Key) {
				tagFilter, err := parser.parseTagFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.TagFilters = append(query.TagFilters, tagFilter)
			} else if elem.Key != timeField && elem.Key != valueField {
				fieldFilter, err := parser.parseFieldFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.FieldFilters = append(query.FieldFilters, fieldFilter)
			}
		}
	}

	// 执行查询
	result, err := e.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	// 创建插值选项
	options := InterpolationOptions{
		Method:     method,
		MaxGap:     10 * intervalDuration, // 最大插值间隔为10个间隔
		Resolution: intervalDuration,
	}

	// 执行插值
	interpolatedPoints, err := e.Interpolate(result.Points, valueField, options)
	if err != nil {
		return nil, err
	}

	// 转换为BSON文档
	results := make([]interface{}, 0, len(interpolatedPoints))
	for _, point := range interpolatedPoints {
		doc := bson.D{
			{Key: "timestamp", Value: time.Unix(0, point.Timestamp)},
		}

		// 添加标签
		tags := bson.D{}
		for k, v := range point.Tags {
			tags = append(tags, bson.E{Key: k, Value: v})
		}
		doc = append(doc, bson.E{Key: "tags", Value: tags})

		// 添加字段
		for k, v := range point.Fields {
			doc = append(doc, bson.E{Key: k, Value: v})
		}

		results = append(results, doc)
	}

	return results, nil
}

// ExecuteMovingWindow 执行移动窗口
func (e *QueryEngine) ExecuteMovingWindow(ctx context.Context, request map[string]interface{}) ([]interface{}, error) {
	// 解析请求参数
	dbName, ok := request["database"].(string)
	if !ok {
		return nil, fmt.Errorf("missing database name")
	}

	collName, ok := request["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("missing collection name")
	}

	timeField, ok := request["timeField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing time field")
	}

	valueField, ok := request["valueField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing value field")
	}

	windowSize, ok := request["windowSize"].(int)
	if !ok || windowSize <= 0 {
		return nil, fmt.Errorf("missing or invalid window size")
	}

	function, ok := request["function"].(string)
	if !ok || function == "" {
		function = "avg" // 默认使用平均值
	}

	// 解析时间范围
	startTime, ok := request["startTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing start time")
	}

	endTime, ok := request["endTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing end time")
	}

	// 创建查询
	query := &Query{
		Database: dbName,
		Table:    collName,
		TimeRange: TimeRange{
			Start: startTime,
			End:   endTime,
		},
	}

	// 添加过滤条件
	if filter, ok := request["filter"].(bson.D); ok {
		parser := NewQueryParser()
		for _, elem := range filter {
			if isTagFilter(elem.Key) {
				tagFilter, err := parser.parseTagFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.TagFilters = append(query.TagFilters, tagFilter)
			} else if elem.Key != timeField && elem.Key != valueField {
				fieldFilter, err := parser.parseFieldFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.FieldFilters = append(query.FieldFilters, fieldFilter)
			}
		}
	}

	// 执行查询
	result, err := e.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	// 创建移动窗口选项
	options := MovingWindowOptions{
		WindowSize: time.Duration(windowSize) * time.Minute, // 窗口大小（分钟）
		Function:   function,
		StepSize:   time.Minute, // 步长（1分钟）
	}

	// 执行移动窗口
	windowedPoints, err := e.MovingWindow(result.Points, valueField, options)
	if err != nil {
		return nil, err
	}

	// 转换为BSON文档
	results := make([]interface{}, 0, len(windowedPoints))
	for _, point := range windowedPoints {
		doc := bson.D{
			{Key: "timestamp", Value: time.Unix(0, point.Timestamp)},
		}

		// 添加标签
		tags := bson.D{}
		for k, v := range point.Tags {
			tags = append(tags, bson.E{Key: k, Value: v})
		}
		doc = append(doc, bson.E{Key: "tags", Value: tags})

		// 添加字段
		for k, v := range point.Fields {
			doc = append(doc, bson.E{Key: k, Value: v})
		}

		results = append(results, doc)
	}

	return results, nil
}

// ExecuteAnomalyDetection 执行异常检测
func (e *QueryEngine) ExecuteAnomalyDetection(ctx context.Context, request map[string]interface{}) ([]interface{}, error) {
	// 解析请求参数
	dbName, ok := request["database"].(string)
	if !ok {
		return nil, fmt.Errorf("missing database name")
	}

	collName, ok := request["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("missing collection name")
	}

	timeField, ok := request["timeField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing time field")
	}

	valueField, ok := request["valueField"].(string)
	if !ok {
		return nil, fmt.Errorf("missing value field")
	}

	method, ok := request["method"].(string)
	if !ok || method == "" {
		method = "zscore" // 默认使用Z-Score
	}

	threshold, ok := request["threshold"].(float64)
	if !ok || threshold <= 0 {
		threshold = 3.0 // 默认阈值
	}

	// 解析时间范围
	startTime, ok := request["startTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing start time")
	}

	endTime, ok := request["endTime"].(int64)
	if !ok {
		return nil, fmt.Errorf("missing end time")
	}

	// 创建查询
	query := &Query{
		Database: dbName,
		Table:    collName,
		TimeRange: TimeRange{
			Start: startTime,
			End:   endTime,
		},
	}

	// 添加过滤条件
	if filter, ok := request["filter"].(bson.D); ok {
		parser := NewQueryParser()
		for _, elem := range filter {
			if isTagFilter(elem.Key) {
				tagFilter, err := parser.parseTagFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.TagFilters = append(query.TagFilters, tagFilter)
			} else if elem.Key != timeField && elem.Key != valueField {
				fieldFilter, err := parser.parseFieldFilter(elem.Key, elem.Value)
				if err != nil {
					return nil, err
				}
				query.FieldFilters = append(query.FieldFilters, fieldFilter)
			}
		}
	}

	// 执行查询
	result, err := e.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	// 创建异常检测选项
	options := AnomalyDetectionOptions{
		Method:      method,
		WindowSize:  24, // 窗口大小（24个点）
		Threshold:   threshold,
		LookbackWin: 72, // 回溯窗口大小（72个点）
	}

	// 执行异常检测
	anomalyPoints, err := e.DetectAnomalies(result.Points, valueField, options)
	if err != nil {
		return nil, err
	}

	// 转换为BSON文档
	results := make([]interface{}, 0, len(anomalyPoints))
	for _, point := range anomalyPoints {
		doc := bson.D{
			{Key: "timestamp", Value: time.Unix(0, point.Timestamp)},
		}

		// 添加标签
		tags := bson.D{}
		for k, v := range point.Tags {
			tags = append(tags, bson.E{Key: k, Value: v})
		}
		doc = append(doc, bson.E{Key: "tags", Value: tags})

		// 添加字段
		for k, v := range point.Fields {
			doc = append(doc, bson.E{Key: k, Value: v})
		}

		results = append(results, doc)
	}

	return results, nil
}

// parseWindowSize 解析窗口大小字符串
func parseWindowSize(windowSize string) (time.Duration, error) {
	// 支持的单位：s, m, h, d, w
	// 例如：1s, 5m, 2h, 1d, 1w

	if len(windowSize) < 2 {
		return 0, fmt.Errorf("invalid window size format")
	}

	// 提取数字部分
	var numStr string
	var unit string

	for i, c := range windowSize {
		if c >= '0' && c <= '9' {
			numStr += string(c)
		} else {
			unit = windowSize[i:]
			break
		}
	}

	if numStr == "" || unit == "" {
		return 0, fmt.Errorf("invalid window size format")
	}

	// 解析数字
	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid window size number: %w", err)
	}

	// 解析单位
	var duration time.Duration
	switch unit {
	case "s":
		duration = time.Duration(num) * time.Second
	case "m":
		duration = time.Duration(num) * time.Minute
	case "h":
		duration = time.Duration(num) * time.Hour
	case "d":
		duration = time.Duration(num) * 24 * time.Hour
	case "w":
		duration = time.Duration(num) * 7 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("unsupported window size unit: %s", unit)
	}

	return duration, nil
}
