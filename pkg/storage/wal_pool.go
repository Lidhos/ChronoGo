package storage

import (
	"sync"
	"sync/atomic"
)

// WALEntryPool 是WALEntry对象池
type WALEntryPool struct {
	pool      sync.Pool
	gets      uint64 // 获取计数
	puts      uint64 // 放回计数
	news      uint64 // 新建计数
	prewarmed uint64 // 预热计数
}

// NewWALEntryPool 创建一个新的WALEntry对象池
func NewWALEntryPool() *WALEntryPool {
	return &WALEntryPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &WALEntry{}
			},
		},
	}
}

// Get 从池中获取一个WALEntry对象
// 返回的对象已经初始化，但需要设置具体的字段值
func (p *WALEntryPool) Get() *WALEntry {
	atomic.AddUint64(&p.gets, 1)
	obj := p.pool.Get()
	if _, ok := obj.(*WALEntry); !ok {
		// 如果获取到的不是WALEntry，说明是新创建的
		atomic.AddUint64(&p.news, 1)
	}
	return obj.(*WALEntry)
}

// GetInsertEntry 从池中获取一个插入类型的WALEntry对象
func (p *WALEntryPool) GetInsertEntry() *WALEntry {
	entry := p.Get()
	entry.Type = walOpInsert
	return entry
}

// GetDeleteEntry 从池中获取一个删除类型的WALEntry对象
func (p *WALEntryPool) GetDeleteEntry() *WALEntry {
	entry := p.Get()
	entry.Type = walOpDelete
	return entry
}

// Put 将WALEntry对象放回池中
// 调用者必须确保放回池中的对象不再被引用
func (p *WALEntryPool) Put(entry *WALEntry) {
	if entry == nil {
		return
	}

	atomic.AddUint64(&p.puts, 1)

	// 清空对象，避免内存泄漏
	entry.Type = 0
	entry.Database = ""
	entry.Table = ""
	entry.Timestamp = 0

	// 清空Data切片，但保留底层数组以便重用
	if entry.Data != nil {
		entry.Data = entry.Data[:0]
	}

	// 放回池中
	p.pool.Put(entry)
}

// CreateInsertEntry 使用对象池创建插入类型的WAL条目
func (p *WALEntryPool) CreateInsertEntry(db, table string, timestamp int64, data []byte) *WALEntry {
	entry := p.Get()
	entry.Type = walOpInsert
	entry.Database = db
	entry.Table = table
	entry.Timestamp = timestamp

	// 复制数据
	if cap(entry.Data) < len(data) {
		entry.Data = make([]byte, len(data))
	} else {
		entry.Data = entry.Data[:len(data)]
	}
	copy(entry.Data, data)

	return entry
}

// CreateDeleteEntry 使用对象池创建删除类型的WAL条目
func (p *WALEntryPool) CreateDeleteEntry(db, table string, timestamp int64) *WALEntry {
	entry := p.Get()
	entry.Type = walOpDelete
	entry.Database = db
	entry.Table = table
	entry.Timestamp = timestamp
	entry.Data = entry.Data[:0] // 删除操作不需要数据
	return entry
}

// Prewarm 预热对象池
// 创建指定数量的对象并放入池中，减少运行时的分配开销
func (p *WALEntryPool) Prewarm(count int) {
	entries := make([]*WALEntry, 0, count)

	// 创建对象
	for i := 0; i < count; i++ {
		entries = append(entries, p.Get())
	}

	// 放回池中
	for _, entry := range entries {
		p.Put(entry)
	}

	atomic.AddUint64(&p.prewarmed, uint64(count))
}

// Stats 返回对象池的统计信息
func (p *WALEntryPool) Stats() map[string]interface{} {
	return map[string]interface{}{
		"gets":      atomic.LoadUint64(&p.gets),
		"puts":      atomic.LoadUint64(&p.puts),
		"news":      atomic.LoadUint64(&p.news),
		"prewarmed": atomic.LoadUint64(&p.prewarmed),
		"hit_rate":  p.HitRate(),
	}
}

// HitRate 返回对象池的命中率
func (p *WALEntryPool) HitRate() float64 {
	gets := atomic.LoadUint64(&p.gets)
	news := atomic.LoadUint64(&p.news)

	if gets == 0 {
		return 0
	}

	return float64(gets-news) / float64(gets)
}
