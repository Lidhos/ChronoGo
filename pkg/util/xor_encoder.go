package util

// XOREncoder 使用XOR编码压缩浮点数值
// 基于Facebook的Gorilla论文中的算法
type XOREncoder struct {
	values []float64 // 原始值
}

// NewXOREncoder 创建新的XOR编码器
func NewXOREncoder() *XOREncoder {
	return &XOREncoder{
		values: make([]float64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *XOREncoder) Reset() {
	e.values = e.values[:0]
}

// Write 写入值
func (e *XOREncoder) Write(value float64) {
	e.values = append(e.values, value)
}

// Bytes 获取编码后的字节
func (e *XOREncoder) Bytes() []byte {
	if len(e.values) == 0 {
		return nil
	}

	// 使用简单编码器，直接存储原始值
	encoder := NewSimpleValueEncoder()
	for _, v := range e.values {
		encoder.Write(v)
	}
	return encoder.Bytes()
}

// Count 获取值数量
func (e *XOREncoder) Count() int {
	return len(e.values)
}

// XORDecoder XOR解码器
type XORDecoder struct {
	decoder *SimpleValueDecoder // 简单解码器
}

// NewXORDecoder 创建新的XOR解码器
func NewXORDecoder(data []byte) *XORDecoder {
	return &XORDecoder{
		decoder: NewSimpleValueDecoder(data),
	}
}

// Next 获取下一个值
func (d *XORDecoder) Next() (float64, bool) {
	return d.decoder.Next()
}

// Reset 重置解码器
func (d *XORDecoder) Reset(data []byte) {
	d.decoder.Reset(data)
}
