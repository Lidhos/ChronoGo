package storage

import (
	"fmt"
	"math"
	mathbits "math/bits"
)

// GorillaCompressor 实现Gorilla压缩算法
// 参考论文: Gorilla: A Fast, Scalable, In-Memory Time Series Database
type GorillaCompressor struct {
	// 时间戳压缩状态
	prevTimestamp int64
	prevDelta     int64

	// 值压缩状态
	prevValue     uint64
	leadingZeros  uint8
	trailingZeros uint8
}

// NewGorillaCompressor 创建新的Gorilla压缩器
func NewGorillaCompressor() *GorillaCompressor {
	return &GorillaCompressor{}
}

// CompressTimestamps 使用Gorilla算法压缩时间戳
// 算法:
// 1. 存储第一个时间戳
// 2. 对于后续时间戳，计算与前一个时间戳的差值(delta)
// 3. 计算delta与前一个delta的差值(delta-of-delta)
// 4. 根据delta-of-delta的值使用不同的编码方式:
//   - 如果delta-of-delta为0，使用1位"0"表示
//   - 如果delta-of-delta在[-127,128]范围内，使用2位"10"加9位值表示
//   - 否则，使用2位"11"加定长值表示
func (c *GorillaCompressor) CompressTimestamps(timestamps []int64) ([]byte, error) {
	if len(timestamps) == 0 {
		return nil, fmt.Errorf("empty timestamps")
	}

	// 创建位缓冲区
	bitBuf := NewBitBuffer()

	// 写入第一个时间戳（完整64位）
	firstTs := timestamps[0]
	if err := bitBuf.WriteBits(uint64(firstTs), 64); err != nil {
		return nil, err
	}

	if len(timestamps) == 1 {
		return bitBuf.Bytes(), nil
	}

	// 计算第一个delta
	c.prevTimestamp = firstTs
	c.prevDelta = timestamps[1] - firstTs

	// 写入第一个delta（定长14位，足够表示秒级精度的常见间隔）
	if err := bitBuf.WriteBits(uint64(c.prevDelta), 14); err != nil {
		return nil, err
	}

	// 对剩余时间戳进行delta-of-delta编码
	for i := 2; i < len(timestamps); i++ {
		ts := timestamps[i]
		delta := ts - c.prevTimestamp
		deltaOfDelta := delta - c.prevDelta

		// 根据delta-of-delta的值使用不同的编码
		if deltaOfDelta == 0 {
			// 如果delta-of-delta为0，写入单个0位
			if err := bitBuf.WriteBit(false); err != nil {
				return nil, err
			}
		} else if deltaOfDelta >= -63 && deltaOfDelta <= 64 {
			// 如果delta-of-delta在[-63,64]范围内，使用2位"10"加7位值表示
			if err := bitBuf.WriteBit(true); err != nil {
				return nil, err
			}
			if err := bitBuf.WriteBit(false); err != nil {
				return nil, err
			}

			// 写入7位值（偏移量+63，使其变为无符号值）
			if err := bitBuf.WriteBits(uint64(deltaOfDelta+63), 7); err != nil {
				return nil, err
			}
		} else {
			// 否则，使用2位"11"加定长值表示
			if err := bitBuf.WriteBit(true); err != nil {
				return nil, err
			}
			if err := bitBuf.WriteBit(true); err != nil {
				return nil, err
			}

			// 写入定长值（使用zigzag编码处理有符号整数）
			zigzag := encodeZigZag(deltaOfDelta)
			if err := bitBuf.WriteBits(zigzag, 64); err != nil {
				return nil, err
			}
		}

		c.prevTimestamp = ts
		c.prevDelta = delta
	}

	return bitBuf.Bytes(), nil
}

// DecompressTimestamps 解压缩Gorilla算法压缩的时间戳
func (c *GorillaCompressor) DecompressTimestamps(data []byte, count int) ([]int64, error) {
	if len(data) == 0 || count <= 0 {
		return nil, fmt.Errorf("invalid data or count")
	}

	// 创建位缓冲区
	bitBuf := NewBitBufferFromBytes(data)

	// 读取第一个时间戳
	firstTsBits, err := bitBuf.ReadBits(64)
	if err != nil {
		return nil, err
	}
	firstTs := int64(firstTsBits)

	result := make([]int64, count)
	result[0] = firstTs

	if count == 1 {
		return result, nil
	}

	// 读取第一个delta
	deltaBits, err := bitBuf.ReadBits(14)
	if err != nil {
		return nil, err
	}
	delta := int64(deltaBits)

	// 计算第二个时间戳
	result[1] = firstTs + delta

	// 初始化状态
	c.prevTimestamp = firstTs
	c.prevDelta = delta

	// 解码剩余时间戳
	for i := 2; i < count; i++ {
		// 读取控制位
		bit, err := bitBuf.ReadBit()
		if err != nil {
			return nil, err
		}

		var deltaOfDelta int64
		if !bit {
			// 如果控制位为0，delta-of-delta为0
			deltaOfDelta = 0
		} else {
			// 读取第二个控制位
			bit2, err := bitBuf.ReadBit()
			if err != nil {
				return nil, err
			}

			if !bit2 {
				// 如果控制位为10，读取7位值
				bits, err := bitBuf.ReadBits(7)
				if err != nil {
					return nil, err
				}
				// 将无符号值转换回有符号值（减去偏移量63）
				deltaOfDelta = int64(bits) - 63
			} else {
				// 如果控制位为11，读取定长值
				bits, err := bitBuf.ReadBits(64)
				if err != nil {
					return nil, err
				}
				// 解码zigzag编码
				deltaOfDelta = decodeZigZag(bits)
			}
		}

		// 计算当前delta和时间戳
		delta = c.prevDelta + deltaOfDelta
		ts := c.prevTimestamp + delta

		result[i] = ts
		c.prevTimestamp = ts
		c.prevDelta = delta
	}

	return result, nil
}

// CompressFloats 使用Gorilla算法压缩浮点数
// 算法:
// 1. 存储第一个值的完整64位
// 2. 对于后续值:
//   - 计算与前一个值的XOR
//   - 如果XOR为0（值相同），使用单个0位表示
//   - 否则，计算前导零和尾随零的数量
//   - 如果前导零和尾随零的模式与前一个相同，使用"10"前缀加有效位
//   - 否则，使用"11"前缀，然后是5位前导零数量，6位有效位长度，然后是有效位
func (c *GorillaCompressor) CompressFloats(values []float64) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("empty values")
	}

	// 创建位缓冲区
	bitBuf := NewBitBuffer()

	// 写入第一个值（完整64位）
	firstVal := math.Float64bits(values[0])
	if err := bitBuf.WriteBits(firstVal, 64); err != nil {
		return nil, err
	}

	if len(values) == 1 {
		return bitBuf.Bytes(), nil
	}

	// 初始化状态
	c.prevValue = firstVal

	// 对剩余值进行XOR编码
	for i := 1; i < len(values); i++ {
		val := math.Float64bits(values[i])
		xor := val ^ c.prevValue

		if xor == 0 {
			// 如果值相同，写入单个0位
			if err := bitBuf.WriteBit(false); err != nil {
				return nil, err
			}
		} else {
			// 值不同，写入1位
			if err := bitBuf.WriteBit(true); err != nil {
				return nil, err
			}

			// 计算前导零和尾随零
			leadingZeros := uint8(mathbits.LeadingZeros64(xor))
			trailingZeros := uint8(mathbits.TrailingZeros64(xor))

			// 计算有效位长度
			sigBits := 64 - int(leadingZeros) - int(trailingZeros)

			// 检查前导零和尾随零的模式是否与前一个相同
			if leadingZeros >= c.leadingZeros && trailingZeros >= c.trailingZeros {
				// 模式相同或更好，使用"10"前缀
				if err := bitBuf.WriteBit(true); err != nil {
					return nil, err
				}
				if err := bitBuf.WriteBit(false); err != nil {
					return nil, err
				}

				// 写入有效位
				significantBits := 64 - int(c.leadingZeros) - int(c.trailingZeros)
				significantXor := (xor >> c.trailingZeros) & ((1 << significantBits) - 1)
				if err := bitBuf.WriteBits(significantXor, uint8(significantBits)); err != nil {
					return nil, err
				}
			} else {
				// 模式不同，使用"11"前缀
				if err := bitBuf.WriteBit(true); err != nil {
					return nil, err
				}
				if err := bitBuf.WriteBit(true); err != nil {
					return nil, err
				}

				// 写入前导零数量（5位，最多32个前导零）
				if err := bitBuf.WriteBits(uint64(leadingZeros), 5); err != nil {
					return nil, err
				}

				// 写入有效位长度（6位，最多63个有效位）
				if err := bitBuf.WriteBits(uint64(sigBits), 6); err != nil {
					return nil, err
				}

				// 写入有效位
				significantXor := (xor >> trailingZeros) & ((1 << sigBits) - 1)
				if err := bitBuf.WriteBits(significantXor, uint8(sigBits)); err != nil {
					return nil, err
				}

				// 更新状态
				c.leadingZeros = leadingZeros
				c.trailingZeros = trailingZeros
			}
		}

		c.prevValue = val
	}

	return bitBuf.Bytes(), nil
}

// DecompressFloats 解压缩Gorilla算法压缩的浮点数
func (c *GorillaCompressor) DecompressFloats(data []byte, count int) ([]float64, error) {
	if len(data) == 0 || count <= 0 {
		return nil, fmt.Errorf("invalid data or count")
	}

	// 创建位缓冲区
	bitBuf := NewBitBufferFromBytes(data)

	// 读取第一个值
	firstValBits, err := bitBuf.ReadBits(64)
	if err != nil {
		return nil, err
	}

	result := make([]float64, count)
	result[0] = math.Float64frombits(firstValBits)

	if count == 1 {
		return result, nil
	}

	// 初始化状态
	c.prevValue = firstValBits
	c.leadingZeros = 0
	c.trailingZeros = 0

	// 解码剩余值
	for i := 1; i < count; i++ {
		// 读取控制位
		bit, err := bitBuf.ReadBit()
		if err != nil {
			return nil, err
		}

		if !bit {
			// 如果控制位为0，值与前一个相同
			result[i] = math.Float64frombits(c.prevValue)
		} else {
			// 读取第二个控制位
			bit2, err := bitBuf.ReadBit()
			if err != nil {
				return nil, err
			}

			var xor uint64
			if !bit2 {
				// 如果控制位为10，使用前一个模式
				significantBits := 64 - int(c.leadingZeros) - int(c.trailingZeros)
				bits, err := bitBuf.ReadBits(uint8(significantBits))
				if err != nil {
					return nil, err
				}
				xor = bits << c.trailingZeros
			} else {
				// 如果控制位为11，读取新模式
				leadingZerosBits, err := bitBuf.ReadBits(5)
				if err != nil {
					return nil, err
				}
				c.leadingZeros = uint8(leadingZerosBits)

				significantBitsBits, err := bitBuf.ReadBits(6)
				if err != nil {
					return nil, err
				}
				significantBits := uint8(significantBitsBits)

				// 计算尾随零
				c.trailingZeros = 64 - c.leadingZeros - significantBits

				// 读取有效位
				bits, err := bitBuf.ReadBits(significantBits)
				if err != nil {
					return nil, err
				}
				xor = bits << c.trailingZeros
			}

			// 计算当前值
			val := c.prevValue ^ xor
			result[i] = math.Float64frombits(val)
			c.prevValue = val
		}
	}

	return result, nil
}

// BitBuffer 位缓冲区，用于按位写入和读取
type BitBuffer struct {
	data     []byte
	bitPos   uint8
	bytePos  int
	readMode bool
}

// NewBitBuffer 创建新的位缓冲区
func NewBitBuffer() *BitBuffer {
	return &BitBuffer{
		data:    make([]byte, 8), // 初始分配8字节
		bitPos:  0,
		bytePos: 0,
	}
}

// NewBitBufferFromBytes 从字节数组创建位缓冲区
func NewBitBufferFromBytes(data []byte) *BitBuffer {
	return &BitBuffer{
		data:     data,
		bitPos:   0,
		bytePos:  0,
		readMode: true,
	}
}

// WriteBit 写入单个位
func (b *BitBuffer) WriteBit(bit bool) error {
	if b.readMode {
		return fmt.Errorf("buffer is in read mode")
	}

	// 确保有足够的空间
	if b.bytePos >= len(b.data) {
		b.data = append(b.data, 0)
	}

	// 写入位
	if bit {
		b.data[b.bytePos] |= 1 << (7 - b.bitPos)
	}

	// 更新位置
	b.bitPos++
	if b.bitPos == 8 {
		b.bitPos = 0
		b.bytePos++
	}

	return nil
}

// WriteBits 写入多个位
func (b *BitBuffer) WriteBits(value uint64, count uint8) error {
	if count > 64 {
		return fmt.Errorf("count exceeds 64")
	}

	// 从最高位开始写入
	for i := int(count) - 1; i >= 0; i-- {
		bit := (value & (1 << i)) != 0
		if err := b.WriteBit(bit); err != nil {
			return err
		}
	}

	return nil
}

// ReadBit 读取单个位
func (b *BitBuffer) ReadBit() (bool, error) {
	if b.bytePos >= len(b.data) {
		return false, fmt.Errorf("end of buffer")
	}

	// 读取位
	bit := (b.data[b.bytePos] & (1 << (7 - b.bitPos))) != 0

	// 更新位置
	b.bitPos++
	if b.bitPos == 8 {
		b.bitPos = 0
		b.bytePos++
	}

	return bit, nil
}

// ReadBits 读取多个位
func (b *BitBuffer) ReadBits(count uint8) (uint64, error) {
	if count > 64 {
		return 0, fmt.Errorf("count exceeds 64")
	}

	var result uint64
	for i := uint8(0); i < count; i++ {
		bit, err := b.ReadBit()
		if err != nil {
			return 0, err
		}
		if bit {
			result |= 1 << (count - 1 - i)
		}
	}

	return result, nil
}

// Bytes 返回缓冲区中的字节
func (b *BitBuffer) Bytes() []byte {
	if b.bitPos == 0 {
		return b.data[:b.bytePos]
	}
	return b.data[:b.bytePos+1]
}

// encodeZigZag 使用ZigZag编码将有符号整数转换为无符号整数
// 这样可以使小的负数和小的正数都映射到小的无符号数
func encodeZigZag(v int64) uint64 {
	return uint64((v << 1) ^ (v >> 63))
}

// decodeZigZag 解码ZigZag编码的整数
func decodeZigZag(v uint64) int64 {
	return int64((v >> 1) ^ -(v & 1))
}
