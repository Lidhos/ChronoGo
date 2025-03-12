package util

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// TimestampEncoder 时间戳编码器
// 使用增量编码和变长编码压缩时间戳
type TimestampEncoder struct {
	prevTimestamp int64   // 上一个时间戳
	buffer        []byte  // 编码缓冲区
	deltas        []int64 // 增量值
}

// NewTimestampEncoder 创建新的时间戳编码器
func NewTimestampEncoder() *TimestampEncoder {
	return &TimestampEncoder{
		prevTimestamp: 0,
		buffer:        make([]byte, 10), // 最大10字节，足够存储一个变长编码的int64
		deltas:        make([]int64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *TimestampEncoder) Reset() {
	e.prevTimestamp = 0
	e.deltas = e.deltas[:0]
}

// Write 写入时间戳
func (e *TimestampEncoder) Write(timestamp int64) {
	// 计算增量
	delta := timestamp - e.prevTimestamp

	// 存储增量
	e.deltas = append(e.deltas, delta)

	// 更新上一个时间戳
	e.prevTimestamp = timestamp
}

// Bytes 获取编码后的字节
func (e *TimestampEncoder) Bytes() []byte {
	if len(e.deltas) == 0 {
		return nil
	}

	// 计算所需的字节数
	size := binary.PutVarint(e.buffer, e.deltas[0])
	totalSize := size

	for i := 1; i < len(e.deltas); i++ {
		size = binary.PutVarint(e.buffer, e.deltas[i])
		totalSize += size
	}

	// 分配足够的空间
	result := make([]byte, totalSize)

	// 编码第一个增量
	size = binary.PutVarint(result, e.deltas[0])
	offset := size

	// 编码剩余的增量
	for i := 1; i < len(e.deltas); i++ {
		size = binary.PutVarint(result[offset:], e.deltas[i])
		offset += size
	}

	return result
}

// Count 获取时间戳数量
func (e *TimestampEncoder) Count() int {
	return len(e.deltas)
}

// TimestampDecoder 时间戳解码器
type TimestampDecoder struct {
	data          []byte // 编码数据
	offset        int    // 当前偏移量
	prevTimestamp int64  // 上一个时间戳
}

// NewTimestampDecoder 创建新的时间戳解码器
func NewTimestampDecoder(data []byte) *TimestampDecoder {
	return &TimestampDecoder{
		data:          data,
		offset:        0,
		prevTimestamp: 0,
	}
}

// Next 获取下一个时间戳
func (d *TimestampDecoder) Next() (int64, bool) {
	if d.offset >= len(d.data) {
		return 0, false
	}

	// 解码增量
	delta, n := binary.Varint(d.data[d.offset:])
	if n <= 0 {
		return 0, false
	}

	// 更新偏移量
	d.offset += n

	// 计算时间戳
	d.prevTimestamp += delta

	return d.prevTimestamp, true
}

// Reset 重置解码器
func (d *TimestampDecoder) Reset(data []byte) {
	d.data = data
	d.offset = 0
	d.prevTimestamp = 0
}

// DeltaOfDeltaTimestampEncoder 二阶增量时间戳编码器
// 使用二阶增量编码和变长编码压缩时间戳
// 对于等间隔的时间序列效果更好
type DeltaOfDeltaTimestampEncoder struct {
	prevTimestamp int64   // 上一个时间戳
	prevDelta     int64   // 上一个增量
	buffer        []byte  // 编码缓冲区
	deltas        []int64 // 二阶增量值
	timestamps    []int64 // 原始时间戳
}

// NewDeltaOfDeltaTimestampEncoder 创建新的二阶增量时间戳编码器
func NewDeltaOfDeltaTimestampEncoder() *DeltaOfDeltaTimestampEncoder {
	return &DeltaOfDeltaTimestampEncoder{
		prevTimestamp: 0,
		prevDelta:     0,
		buffer:        make([]byte, 10), // 最大10字节，足够存储一个变长编码的int64
		deltas:        make([]int64, 0, 1024),
		timestamps:    make([]int64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *DeltaOfDeltaTimestampEncoder) Reset() {
	e.prevTimestamp = 0
	e.prevDelta = 0
	e.deltas = e.deltas[:0]
	e.timestamps = e.timestamps[:0]
}

// Write 写入时间戳
func (e *DeltaOfDeltaTimestampEncoder) Write(timestamp int64) {
	// 存储原始时间戳
	e.timestamps = append(e.timestamps, timestamp)

	// 如果是第一个时间戳
	if len(e.timestamps) == 1 {
		e.prevTimestamp = timestamp
		return
	}

	// 如果是第二个时间戳
	if len(e.timestamps) == 2 {
		e.prevDelta = timestamp - e.prevTimestamp
		e.prevTimestamp = timestamp
		e.deltas = append(e.deltas, e.prevDelta)
		return
	}

	// 计算一阶增量
	delta := timestamp - e.prevTimestamp

	// 计算二阶增量
	deltaOfDelta := delta - e.prevDelta

	// 存储二阶增量
	e.deltas = append(e.deltas, deltaOfDelta)

	// 更新状态
	e.prevDelta = delta
	e.prevTimestamp = timestamp
}

// Bytes 获取编码后的字节
func (e *DeltaOfDeltaTimestampEncoder) Bytes() []byte {
	if len(e.timestamps) == 0 {
		return nil
	}

	// 如果只有一个时间戳
	if len(e.timestamps) == 1 {
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, uint64(e.timestamps[0]))
		return result
	}

	// 如果只有两个时间戳
	if len(e.timestamps) == 2 {
		result := make([]byte, 16)
		binary.LittleEndian.PutUint64(result, uint64(e.timestamps[0]))
		binary.LittleEndian.PutUint64(result[8:], uint64(e.timestamps[1]))
		return result
	}

	// 计算所需的字节数
	totalSize := 16 // 前两个时间戳

	for _, delta := range e.deltas[1:] {
		size := binary.PutVarint(e.buffer, delta)
		totalSize += size
	}

	// 分配足够的空间
	result := make([]byte, totalSize)

	// 编码前两个时间戳
	binary.LittleEndian.PutUint64(result, uint64(e.timestamps[0]))
	binary.LittleEndian.PutUint64(result[8:], uint64(e.timestamps[1]))
	offset := 16

	// 编码剩余的二阶增量
	for _, delta := range e.deltas[1:] {
		size := binary.PutVarint(result[offset:], delta)
		offset += size
	}

	return result
}

// Count 获取时间戳数量
func (e *DeltaOfDeltaTimestampEncoder) Count() int {
	return len(e.timestamps)
}

// DeltaOfDeltaTimestampDecoder 二阶增量时间戳解码器
type DeltaOfDeltaTimestampDecoder struct {
	data          []byte // 编码数据
	offset        int    // 当前偏移量
	prevTimestamp int64  // 上一个时间戳
	prevDelta     int64  // 上一个增量
	index         int    // 当前索引
}

// NewDeltaOfDeltaTimestampDecoder 创建新的二阶增量时间戳解码器
func NewDeltaOfDeltaTimestampDecoder(data []byte) *DeltaOfDeltaTimestampDecoder {
	return &DeltaOfDeltaTimestampDecoder{
		data:   data,
		offset: 0,
		index:  0,
	}
}

// Next 获取下一个时间戳
func (d *DeltaOfDeltaTimestampDecoder) Next() (int64, bool) {
	// 如果数据不足
	if len(d.data) < 8 {
		return 0, false
	}

	// 如果是第一个时间戳
	if d.index == 0 {
		d.prevTimestamp = int64(binary.LittleEndian.Uint64(d.data))
		d.offset = 8
		d.index++
		return d.prevTimestamp, true
	}

	// 如果是第二个时间戳
	if d.index == 1 {
		if len(d.data) < 16 {
			return 0, false
		}

		timestamp := int64(binary.LittleEndian.Uint64(d.data[8:]))
		d.prevDelta = timestamp - d.prevTimestamp
		d.prevTimestamp = timestamp
		d.offset = 16
		d.index++
		return timestamp, true
	}

	// 如果已经到达数据末尾
	if d.offset >= len(d.data) {
		return 0, false
	}

	// 解码二阶增量
	deltaOfDelta, n := binary.Varint(d.data[d.offset:])
	if n <= 0 {
		return 0, false
	}

	// 更新偏移量
	d.offset += n

	// 计算一阶增量
	delta := d.prevDelta + deltaOfDelta

	// 计算时间戳
	timestamp := d.prevTimestamp + delta

	// 更新状态
	d.prevDelta = delta
	d.prevTimestamp = timestamp
	d.index++

	return timestamp, true
}

// Reset 重置解码器
func (d *DeltaOfDeltaTimestampDecoder) Reset(data []byte) {
	d.data = data
	d.offset = 0
	d.prevTimestamp = 0
	d.prevDelta = 0
	d.index = 0
}

// XORTimestampEncoder XOR时间戳编码器
// 使用XOR编码压缩时间戳，对于接近的时间戳效果更好
type XORTimestampEncoder struct {
	prevTimestamp uint64  // 上一个时间戳
	buffer        []byte  // 编码缓冲区
	timestamps    []int64 // 原始时间戳
}

// NewXORTimestampEncoder 创建新的XOR时间戳编码器
func NewXORTimestampEncoder() *XORTimestampEncoder {
	return &XORTimestampEncoder{
		prevTimestamp: 0,
		buffer:        make([]byte, 10), // 最大10字节，足够存储一个变长编码的int64
		timestamps:    make([]int64, 0, 1024),
	}
}

// Reset 重置编码器
func (e *XORTimestampEncoder) Reset() {
	e.prevTimestamp = 0
	e.timestamps = e.timestamps[:0]
}

// Write 写入时间戳
func (e *XORTimestampEncoder) Write(timestamp int64) {
	// 存储原始时间戳
	e.timestamps = append(e.timestamps, timestamp)
}

// Bytes 获取编码后的字节
func (e *XORTimestampEncoder) Bytes() []byte {
	if len(e.timestamps) == 0 {
		return nil
	}

	// 如果只有一个时间戳
	if len(e.timestamps) == 1 {
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, uint64(e.timestamps[0]))
		return result
	}

	// 计算所需的字节数
	totalSize := 8 // 第一个时间戳

	for i := 1; i < len(e.timestamps); i++ {
		// 计算XOR值
		xor := uint64(e.timestamps[i]) ^ uint64(e.timestamps[i-1])

		// 计算前导零和尾随零的数量
		leadingZeros := uint8(0)
		trailingZeros := uint8(0)

		if xor != 0 {
			leadingZeros = uint8(bits.LeadingZeros64(xor))
			trailingZeros = uint8(bits.TrailingZeros64(xor))
		}

		// 计算有效位数
		significantBits := 64 - leadingZeros - trailingZeros

		// 控制字节 (1字节) + 有效位 (最多8字节)
		totalSize += 1 + int(math.Ceil(float64(significantBits)/8.0))
	}

	// 分配足够的空间
	result := make([]byte, totalSize)

	// 编码第一个时间戳
	binary.LittleEndian.PutUint64(result, uint64(e.timestamps[0]))
	offset := 8

	// 编码剩余的时间戳
	for i := 1; i < len(e.timestamps); i++ {
		// 计算XOR值
		xor := uint64(e.timestamps[i]) ^ uint64(e.timestamps[i-1])

		// 计算前导零和尾随零的数量
		leadingZeros := uint8(0)
		trailingZeros := uint8(0)

		if xor != 0 {
			leadingZeros = uint8(bits.LeadingZeros64(xor))
			trailingZeros = uint8(bits.TrailingZeros64(xor))
		}

		// 计算有效位数
		significantBits := 64 - leadingZeros - trailingZeros

		// 编码控制字节
		// 前4位：前导零的数量 / 4
		// 后4位：有效位的数量 - 1
		controlByte := (leadingZeros / 4) << 4
		controlByte |= (significantBits - 1) & 0x0F
		result[offset] = controlByte
		offset++

		// 编码有效位
		if significantBits > 0 {
			// 右移尾随零
			significantXor := xor >> trailingZeros

			// 计算需要的字节数
			byteCount := int(math.Ceil(float64(significantBits) / 8.0))

			// 编码有效位
			for j := 0; j < byteCount; j++ {
				result[offset+j] = byte(significantXor >> (j * 8))
			}

			offset += byteCount
		}
	}

	return result[:offset]
}

// Count 获取时间戳数量
func (e *XORTimestampEncoder) Count() int {
	return len(e.timestamps)
}

// XORTimestampDecoder XOR时间戳解码器
type XORTimestampDecoder struct {
	data          []byte // 编码数据
	offset        int    // 当前偏移量
	prevTimestamp uint64 // 上一个时间戳
	index         int    // 当前索引
}

// NewXORTimestampDecoder 创建新的XOR时间戳解码器
func NewXORTimestampDecoder(data []byte) *XORTimestampDecoder {
	return &XORTimestampDecoder{
		data:   data,
		offset: 0,
		index:  0,
	}
}

// Next 获取下一个时间戳
func (d *XORTimestampDecoder) Next() (int64, bool) {
	// 如果数据不足
	if len(d.data) < 8 {
		return 0, false
	}

	// 如果是第一个时间戳
	if d.index == 0 {
		d.prevTimestamp = binary.LittleEndian.Uint64(d.data)
		d.offset = 8
		d.index++
		return int64(d.prevTimestamp), true
	}

	// 如果已经到达数据末尾
	if d.offset >= len(d.data) {
		return 0, false
	}

	// 解码控制字节
	controlByte := d.data[d.offset]
	d.offset++

	// 解析前导零和有效位数
	leadingZeros := uint8(controlByte>>4) * 4
	significantBits := (controlByte & 0x0F) + 1

	// 如果有效位数为0，表示时间戳与前一个相同
	if significantBits == 0 {
		d.index++
		return int64(d.prevTimestamp), true
	}

	// 计算需要的字节数
	byteCount := int(math.Ceil(float64(significantBits) / 8.0))

	// 如果数据不足
	if d.offset+byteCount > len(d.data) {
		return 0, false
	}

	// 解码有效位
	var significantXor uint64
	for i := 0; i < byteCount; i++ {
		significantXor |= uint64(d.data[d.offset+i]) << (i * 8)
	}
	d.offset += byteCount

	// 计算尾随零的数量
	trailingZeros := 64 - leadingZeros - significantBits

	// 计算完整的XOR值
	xor := significantXor << trailingZeros

	// 计算时间戳
	timestamp := d.prevTimestamp ^ xor

	// 更新状态
	d.prevTimestamp = timestamp
	d.index++

	return int64(timestamp), true
}

// Reset 重置解码器
func (d *XORTimestampDecoder) Reset(data []byte) {
	d.data = data
	d.offset = 0
	d.prevTimestamp = 0
	d.index = 0
}
