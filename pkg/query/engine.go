package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/model"
	"ChronoGo/pkg/storage"
)

// QueryEngine 表示查询处理引擎
type QueryEngine struct {
	storage *storage.StorageEngine
}

// NewQueryEngine 创建新的查询引擎
func NewQueryEngine(storage *storage.StorageEngine) *QueryEngine {
	return &QueryEngine{
		storage: storage,
	}
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

	// 应用聚合操作
	if len(query.Aggregations) > 0 {
		// TODO: 实现聚合操作
	}

	// 应用分组
	if len(query.GroupBy) > 0 {
		// TODO: 实现分组操作
	}

	// 应用排序
	if len(query.OrderBy) > 0 {
		// TODO: 实现排序操作
	}

	// 应用分页
	if query.Limit > 0 {
		// TODO: 实现分页操作
	}

	// 转换结果
	result := &QueryResult{}
	for _, p := range points {
		result.Points = append(result.Points, *p)
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
