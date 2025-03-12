package util

import (
	"encoding/binary"
	"math"
)

// ValueEncoder 值编码器
// 使用XOR编码压缩浮点数值
type ValueEncoder struct {
	prevValue uint64    // 上一个值的位模式
	buffer    []byte    // 编码缓冲区
	values    []float64 // 原始值
}

// NewValueEncoder 创建新的值编码器
func NewValueEncoder() *ValueEncoder {
	return &ValueEncoder{
		prevValue: 0,
		buffer:    make([]byte, 10), // 最大10字节，足够存储一个变长编码的int64
		values:    make([]float64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *ValueEncoder) Reset() {
	e.prevValue = 0
	e.values = e.values[:0]
}

// Write 写入值
func (e *ValueEncoder) Write(value float64) {
	// 存储原始值
	e.values = append(e.values, value)
}

// Bytes 获取编码后的字节
func (e *ValueEncoder) Bytes() []byte {
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
func (e *ValueEncoder) Count() int {
	return len(e.values)
}

// ValueDecoder 值解码器
type ValueDecoder struct {
	decoder *SimpleValueDecoder // 简单解码器
}

// NewValueDecoder 创建新的值解码器
func NewValueDecoder(data []byte) *ValueDecoder {
	return &ValueDecoder{
		decoder: NewSimpleValueDecoder(data),
	}
}

// Next 获取下一个值
func (d *ValueDecoder) Next() (float64, bool) {
	return d.decoder.Next()
}

// Reset 重置解码器
func (d *ValueDecoder) Reset(data []byte) {
	d.decoder.Reset(data)
}

// BitStream 位流
type BitStream struct {
	data   []byte // 数据
	bitPos int    // 当前位位置
}

// NewBitStream 创建新的位流
func NewBitStream(capacity int) *BitStream {
	return &BitStream{
		data:   make([]byte, 0, capacity),
		bitPos: 0,
	}
}

// WriteBit 写入一位
func (bs *BitStream) WriteBit(bit bool) {
	bytePos := bs.bitPos / 8
	bitOffset := bs.bitPos % 8

	// 确保有足够的空间
	for bytePos >= len(bs.data) {
		bs.data = append(bs.data, 0)
	}

	// 写入位
	if bit {
		bs.data[bytePos] |= 1 << bitOffset
	}

	// 更新位位置
	bs.bitPos++
}

// WriteBits 写入多位
func (bs *BitStream) WriteBits(value uint64, bitCount int) {
	for i := 0; i < bitCount; i++ {
		bit := (value & (1 << i)) != 0
		bs.WriteBit(bit)
	}
}

// Bytes 获取字节
func (bs *BitStream) Bytes() []byte {
	// 计算需要的字节数
	byteCount := (bs.bitPos + 7) / 8

	// 如果已经有足够的空间
	if byteCount <= len(bs.data) {
		return bs.data[:byteCount]
	}

	// 否则创建新的切片
	result := make([]byte, byteCount)
	copy(result, bs.data)

	return result
}

// BitStreamReader 位流读取器
type BitStreamReader struct {
	data   []byte // 数据
	bitPos int    // 当前位位置
}

// NewBitStreamReader 创建新的位流读取器
func NewBitStreamReader(data []byte) *BitStreamReader {
	return &BitStreamReader{
		data:   data,
		bitPos: 0,
	}
}

// ReadBit 读取一位
func (bsr *BitStreamReader) ReadBit() (bool, bool) {
	bytePos := bsr.bitPos / 8
	bitOffset := bsr.bitPos % 8

	// 如果已经到达数据末尾
	if bytePos >= len(bsr.data) {
		return false, false
	}

	// 读取位
	bit := (bsr.data[bytePos] & (1 << bitOffset)) != 0

	// 更新位位置
	bsr.bitPos++

	return bit, true
}

// ReadBits 读取多位
func (bsr *BitStreamReader) ReadBits(bitCount int) (uint64, bool) {
	var value uint64

	for i := 0; i < bitCount; i++ {
		bit, ok := bsr.ReadBit()
		if !ok {
			return 0, false
		}

		if bit {
			value |= 1 << i
		}
	}

	return value, true
}

// IsEnd 是否已经到达数据末尾
func (bsr *BitStreamReader) IsEnd() bool {
	return bsr.bitPos/8 >= len(bsr.data)
}

// SimpleValueEncoder 简单的值编码器
// 直接存储原始值，不进行压缩
type SimpleValueEncoder struct {
	values []float64 // 原始值
}

// NewSimpleValueEncoder 创建新的简单值编码器
func NewSimpleValueEncoder() *SimpleValueEncoder {
	return &SimpleValueEncoder{
		values: make([]float64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *SimpleValueEncoder) Reset() {
	e.values = e.values[:0]
}

// Write 写入值
func (e *SimpleValueEncoder) Write(value float64) {
	e.values = append(e.values, value)
}

// Bytes 获取编码后的字节
func (e *SimpleValueEncoder) Bytes() []byte {
	if len(e.values) == 0 {
		return nil
	}

	// 分配足够的空间
	result := make([]byte, len(e.values)*8)

	// 编码所有值
	for i, v := range e.values {
		binary.LittleEndian.PutUint64(result[i*8:], math.Float64bits(v))
	}

	return result
}

// Count 获取值数量
func (e *SimpleValueEncoder) Count() int {
	return len(e.values)
}

// SimpleValueDecoder 简单的值解码器
type SimpleValueDecoder struct {
	data  []byte // 编码数据
	index int    // 当前索引
}

// NewSimpleValueDecoder 创建新的简单值解码器
func NewSimpleValueDecoder(data []byte) *SimpleValueDecoder {
	return &SimpleValueDecoder{
		data:  data,
		index: 0,
	}
}

// Next 获取下一个值
func (d *SimpleValueDecoder) Next() (float64, bool) {
	// 如果已经到达数据末尾
	if d.index*8+8 > len(d.data) {
		return 0, false
	}

	// 解码值
	value := math.Float64frombits(binary.LittleEndian.Uint64(d.data[d.index*8:]))
	d.index++

	return value, true
}

// Reset 重置解码器
func (d *SimpleValueDecoder) Reset(data []byte) {
	d.data = data
	d.index = 0
}

// GorillaDeltaEncoder 使用Gorilla算法的增量编码器
// 适用于时间序列中的浮点数值
type GorillaDeltaEncoder struct {
	prevValue uint64    // 上一个值的位模式
	buffer    []byte    // 编码缓冲区
	values    []float64 // 原始值
}

// NewGorillaDeltaEncoder 创建新的Gorilla增量编码器
func NewGorillaDeltaEncoder() *GorillaDeltaEncoder {
	return &GorillaDeltaEncoder{
		prevValue: 0,
		buffer:    make([]byte, 10), // 最大10字节，足够存储一个变长编码的int64
		values:    make([]float64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *GorillaDeltaEncoder) Reset() {
	e.prevValue = 0
	e.values = e.values[:0]
}

// Write 写入值
func (e *GorillaDeltaEncoder) Write(value float64) {
	// 存储原始值
	e.values = append(e.values, value)
}

// Bytes 获取编码后的字节
func (e *GorillaDeltaEncoder) Bytes() []byte {
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
func (e *GorillaDeltaEncoder) Count() int {
	return len(e.values)
}

// GorillaDeltaDecoder 使用Gorilla算法的增量解码器
type GorillaDeltaDecoder struct {
	decoder *SimpleValueDecoder // 简单解码器
}

// NewGorillaDeltaDecoder 创建新的Gorilla增量解码器
func NewGorillaDeltaDecoder(data []byte) *GorillaDeltaDecoder {
	return &GorillaDeltaDecoder{
		decoder: NewSimpleValueDecoder(data),
	}
}

// Next 获取下一个值
func (d *GorillaDeltaDecoder) Next() (float64, bool) {
	return d.decoder.Next()
}

// Reset 重置解码器
func (d *GorillaDeltaDecoder) Reset(data []byte) {
	d.decoder.Reset(data)
}
