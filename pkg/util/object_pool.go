package util

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// ObjectPoolStats 表示对象池统计信息
type ObjectPoolStats struct {
	Gets      uint64 // 获取计数
	Puts      uint64 // 放回计数
	News      uint64 // 新建计数
	Prewarmed uint64 // 预热计数
}

// GenericPool 通用对象池接口
type GenericPool interface {
	// Get 从池中获取对象
	Get() interface{}
	// Put 将对象放回池中
	Put(obj interface{})
	// Stats 获取统计信息
	Stats() ObjectPoolStats
	// Prewarm 预热对象池
	Prewarm(count int)
}

// ObjectPool 通用对象池实现
type ObjectPool struct {
	pool      sync.Pool
	gets      uint64
	puts      uint64
	news      uint64
	prewarmed uint64
	newFunc   func() interface{}
	resetFunc func(interface{})
}

// NewObjectPool 创建新的对象池
func NewObjectPool(newFunc func() interface{}, resetFunc func(interface{})) *ObjectPool {
	pool := &ObjectPool{
		newFunc:   newFunc,
		resetFunc: resetFunc,
	}

	pool.pool.New = func() interface{} {
		atomic.AddUint64(&pool.news, 1)
		return newFunc()
	}

	return pool
}

// Get 从池中获取对象
func (p *ObjectPool) Get() interface{} {
	atomic.AddUint64(&p.gets, 1)
	return p.pool.Get()
}

// Put 将对象放回池中
func (p *ObjectPool) Put(obj interface{}) {
	if obj == nil {
		return
	}

	atomic.AddUint64(&p.puts, 1)

	// 重置对象
	if p.resetFunc != nil {
		p.resetFunc(obj)
	}

	p.pool.Put(obj)
}

// Stats 获取统计信息
func (p *ObjectPool) Stats() ObjectPoolStats {
	return ObjectPoolStats{
		Gets:      atomic.LoadUint64(&p.gets),
		Puts:      atomic.LoadUint64(&p.puts),
		News:      atomic.LoadUint64(&p.news),
		Prewarmed: atomic.LoadUint64(&p.prewarmed),
	}
}

// Prewarm 预热对象池
func (p *ObjectPool) Prewarm(count int) {
	objects := make([]interface{}, count)

	// 创建对象
	for i := 0; i < count; i++ {
		objects[i] = p.newFunc()
		atomic.AddUint64(&p.prewarmed, 1)
	}

	// 放回池中
	for i := 0; i < count; i++ {
		p.pool.Put(objects[i])
	}
}

// BufferPool 字节缓冲区池
type BufferPool struct {
	*ObjectPool
	defaultSize int
}

// NewBufferPool 创建新的字节缓冲区池
func NewBufferPool(defaultSize int) *BufferPool {
	if defaultSize <= 0 {
		defaultSize = 4096 // 默认4KB
	}

	return &BufferPool{
		ObjectPool: NewObjectPool(
			func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, defaultSize))
			},
			func(obj interface{}) {
				buf := obj.(*bytes.Buffer)
				buf.Reset()
			},
		),
		defaultSize: defaultSize,
	}
}

// Get 获取缓冲区
func (p *BufferPool) Get() *bytes.Buffer {
	return p.ObjectPool.Get().(*bytes.Buffer)
}

// BytesPool 字节切片池
type BytesPool struct {
	*ObjectPool
	size int
}

// NewBytesPool 创建新的字节切片池
func NewBytesPool(size int) *BytesPool {
	if size <= 0 {
		size = 4096 // 默认4KB
	}

	return &BytesPool{
		ObjectPool: NewObjectPool(
			func() interface{} {
				return make([]byte, size)
			},
			func(obj interface{}) {
				// 字节切片不需要重置，GC会处理
			},
		),
		size: size,
	}
}

// Get 获取字节切片
func (p *BytesPool) Get() []byte {
	return p.ObjectPool.Get().([]byte)
}

// ObjectPools 对象池管理器
type ObjectPools struct {
	// 各种对象池
	bufferPool      *BufferPool // 缓冲区池
	smallBytesPool  *BytesPool  // 小字节切片池 (4KB)
	mediumBytesPool *BytesPool  // 中字节切片池 (16KB)
	largeBytesPool  *BytesPool  // 大字节切片池 (64KB)
	// 可以添加更多对象池
}

// NewObjectPools 创建新的对象池管理器
func NewObjectPools() *ObjectPools {
	return &ObjectPools{
		bufferPool:      NewBufferPool(4096),     // 4KB
		smallBytesPool:  NewBytesPool(4096),      // 4KB
		mediumBytesPool: NewBytesPool(16 * 1024), // 16KB
		largeBytesPool:  NewBytesPool(64 * 1024), // 64KB
	}
}

// GetBuffer 获取缓冲区
func (p *ObjectPools) GetBuffer() *bytes.Buffer {
	return p.bufferPool.Get()
}

// PutBuffer 放回缓冲区
func (p *ObjectPools) PutBuffer(buf *bytes.Buffer) {
	p.bufferPool.Put(buf)
}

// GetBytes 根据大小获取合适的字节切片
func (p *ObjectPools) GetBytes(size int) []byte {
	if size <= 4096 {
		return p.smallBytesPool.Get()
	} else if size <= 16*1024 {
		return p.mediumBytesPool.Get()
	} else {
		return p.largeBytesPool.Get()
	}
}

// PutBytes 放回字节切片
func (p *ObjectPools) PutBytes(b []byte, size int) {
	if b == nil {
		return
	}

	if size <= 4096 {
		p.smallBytesPool.Put(b)
	} else if size <= 16*1024 {
		p.mediumBytesPool.Put(b)
	} else {
		p.largeBytesPool.Put(b)
	}
}

// GetStats 获取所有对象池的统计信息
func (p *ObjectPools) GetStats() map[string]ObjectPoolStats {
	return map[string]ObjectPoolStats{
		"buffer":      p.bufferPool.Stats(),
		"smallBytes":  p.smallBytesPool.Stats(),
		"mediumBytes": p.mediumBytesPool.Stats(),
		"largeBytes":  p.largeBytesPool.Stats(),
	}
}

// PrewarmAll 预热所有对象池
func (p *ObjectPools) PrewarmAll(count int) {
	p.bufferPool.Prewarm(count)
	p.smallBytesPool.Prewarm(count)
	p.mediumBytesPool.Prewarm(count)
	p.largeBytesPool.Prewarm(count)
}
