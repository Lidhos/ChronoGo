package model

import (
	"sync"
	"sync/atomic"
)

// TimeSeriesPointPool 是TimeSeriesPoint对象池
type TimeSeriesPointPool struct {
	pool      sync.Pool
	gets      uint64 // 获取计数
	puts      uint64 // 放回计数
	news      uint64 // 新建计数
	prewarmed uint64 // 预热计数
}

// NewTimeSeriesPointPool 创建一个新的TimeSeriesPoint对象池
func NewTimeSeriesPointPool() *TimeSeriesPointPool {
	return &TimeSeriesPointPool{
		pool: sync.Pool{
			New: func() interface{} {
				point := &TimeSeriesPoint{
					Tags:   make(map[string]string),
					Fields: make(map[string]interface{}),
				}
				return point
			},
		},
	}
}

// Get 从池中获取一个TimeSeriesPoint对象
// 返回的对象已经初始化，但需要设置具体的字段值
func (p *TimeSeriesPointPool) Get() *TimeSeriesPoint {
	atomic.AddUint64(&p.gets, 1)
	obj := p.pool.Get()
	if _, ok := obj.(*TimeSeriesPoint); !ok {
		// 如果获取到的不是TimeSeriesPoint，说明是新创建的
		atomic.AddUint64(&p.news, 1)
	}
	return obj.(*TimeSeriesPoint)
}

// Put 将TimeSeriesPoint对象放回池中
// 调用者必须确保放回池中的对象不再被引用
func (p *TimeSeriesPointPool) Put(point *TimeSeriesPoint) {
	if point == nil {
		return
	}

	atomic.AddUint64(&p.puts, 1)

	// 清空对象，避免内存泄漏
	point.Timestamp = 0

	// 清空Tags
	for k := range point.Tags {
		delete(point.Tags, k)
	}

	// 清空Fields
	for k := range point.Fields {
		delete(point.Fields, k)
	}

	// 放回池中
	p.pool.Put(point)
}

// CreatePoint 创建一个新的TimeSeriesPoint对象
// 这是一个便捷方法，用于从池中获取对象并设置初始值
func (p *TimeSeriesPointPool) CreatePoint(timestamp int64, tags map[string]string, fields map[string]interface{}) *TimeSeriesPoint {
	point := p.Get()
	point.Timestamp = timestamp

	// 复制tags
	for k, v := range tags {
		point.Tags[k] = v
	}

	// 复制fields
	for k, v := range fields {
		point.Fields[k] = v
	}

	return point
}

// Prewarm 预热对象池
// 创建指定数量的对象并放入池中，减少运行时的分配开销
func (p *TimeSeriesPointPool) Prewarm(count int) {
	points := make([]*TimeSeriesPoint, 0, count)

	// 创建对象
	for i := 0; i < count; i++ {
		points = append(points, p.Get())
	}

	// 放回池中
	for _, point := range points {
		p.Put(point)
	}

	atomic.AddUint64(&p.prewarmed, uint64(count))
}

// Stats 返回对象池的统计信息
func (p *TimeSeriesPointPool) Stats() map[string]interface{} {
	return map[string]interface{}{
		"gets":      atomic.LoadUint64(&p.gets),
		"puts":      atomic.LoadUint64(&p.puts),
		"news":      atomic.LoadUint64(&p.news),
		"prewarmed": atomic.LoadUint64(&p.prewarmed),
		"hit_rate":  p.HitRate(),
	}
}

// HitRate 返回对象池的命中率
func (p *TimeSeriesPointPool) HitRate() float64 {
	gets := atomic.LoadUint64(&p.gets)
	news := atomic.LoadUint64(&p.news)

	if gets == 0 {
		return 0
	}

	return float64(gets-news) / float64(gets)
}
