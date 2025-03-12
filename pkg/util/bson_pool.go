package util

import (
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

// BSONBufferPool 是 BSON 缓冲区的对象池
type BSONBufferPool struct {
	pool sync.Pool
}

// NewBSONBufferPool 创建一个新的 BSON 缓冲区对象池
func NewBSONBufferPool(initialSize int) *BSONBufferPool {
	return &BSONBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				buffer := make([]byte, 0, initialSize)
				return &buffer
			},
		},
	}
}

// Get 从池中获取一个缓冲区
func (p *BSONBufferPool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

// Put 将缓冲区放回池中
func (p *BSONBufferPool) Put(buffer *[]byte) {
	// 清空缓冲区但保留容量
	*buffer = (*buffer)[:0]
	p.pool.Put(buffer)
}

// BSONDocPool 是 BSON 文档的对象池
type BSONDocPool struct {
	pool sync.Pool
}

// NewBSONDocPool 创建一个新的 BSON 文档对象池
func NewBSONDocPool() *BSONDocPool {
	return &BSONDocPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &bson.D{}
			},
		},
	}
}

// Get 从池中获取一个 BSON 文档
func (p *BSONDocPool) Get() *bson.D {
	return p.pool.Get().(*bson.D)
}

// Put 将 BSON 文档放回池中
func (p *BSONDocPool) Put(doc *bson.D) {
	// 清空文档
	*doc = (*doc)[:0]
	p.pool.Put(doc)
}

// 全局 BSON 对象池
var (
	GlobalBSONBufferPool = NewBSONBufferPool(4096) // 初始大小 4KB
	GlobalBSONDocPool    = NewBSONDocPool()
)

// MarshalWithPool 使用对象池进行 BSON 序列化
func MarshalWithPool(v interface{}) ([]byte, error) {
	// 获取缓冲区
	buffer := GlobalBSONBufferPool.Get()
	defer GlobalBSONBufferPool.Put(buffer)

	// 使用标准 Marshal 函数
	data, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}

	// 复制到缓冲区
	*buffer = append(*buffer, data...)

	// 创建结果副本
	result := make([]byte, len(*buffer))
	copy(result, *buffer)

	return result, nil
}

// UnmarshalWithPool 使用对象池进行 BSON 反序列化
func UnmarshalWithPool(data []byte, v interface{}) error {
	return bson.Unmarshal(data, v)
}

// GetBSONDoc 从池中获取一个 BSON 文档
func GetBSONDoc() *bson.D {
	return GlobalBSONDocPool.Get()
}

// PutBSONDoc 将 BSON 文档放回池中
func PutBSONDoc(doc *bson.D) {
	GlobalBSONDocPool.Put(doc)
}
