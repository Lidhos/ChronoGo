package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	mathbits "math/bits"
)

// 列类型
const (
	ColumnTypeTimestamp byte = 1 // 时间戳列
	ColumnTypeInteger   byte = 2 // 整数列
	ColumnTypeFloat     byte = 3 // 浮点数列
	ColumnTypeString    byte = 4 // 字符串列
	ColumnTypeBoolean   byte = 5 // 布尔列
)

// 压缩类型
const (
	CompressionNone       byte = 0 // 无压缩
	CompressionDelta      byte = 1 // Delta编码
	CompressionDeltaDelta byte = 2 // Delta-of-Delta编码
	CompressionXOR        byte = 3 // XOR编码
	CompressionDictionary byte = 4 // 字典编码
	CompressionRLE        byte = 5 // 游程编码
	CompressionGorilla    byte = 6 // Gorilla压缩
)

// Column 表示列存储
type Column struct {
	Type           byte   // 列类型
	Name           string // 列名
	Compression    byte   // 压缩类型
	Data           []byte // 压缩后的数据
	Count          int    // 值的数量
	Min            []byte // 最小值
	Max            []byte // 最大值
	DictionaryData []byte // 字典数据（用于字典压缩）
}

// NewColumn 创建新的列
func NewColumn(name string, colType byte) *Column {
	return &Column{
		Type: colType,
		Name: name,
	}
}

// ColumnBlock 表示列块
type ColumnBlock struct {
	Timestamp *Column            // 时间戳列
	Tags      map[string]*Column // 标签列
	Fields    map[string]*Column // 字段列
	Count     int                // 行数
}

// NewColumnBlock 创建新的列块
func NewColumnBlock() *ColumnBlock {
	return &ColumnBlock{
		Tags:   make(map[string]*Column),
		Fields: make(map[string]*Column),
	}
}

// TimestampCompressor 时间戳压缩器
type TimestampCompressor struct {
	prevTimestamp int64
	prevDelta     int64
}

// NewTimestampCompressor 创建新的时间戳压缩器
func NewTimestampCompressor() *TimestampCompressor {
	return &TimestampCompressor{}
}

// CompressDeltaDelta 使用Delta-of-Delta编码压缩时间戳
func (c *TimestampCompressor) CompressDeltaDelta(timestamps []int64) ([]byte, error) {
	if len(timestamps) == 0 {
		return nil, fmt.Errorf("empty timestamps")
	}

	// 计算需要的字节数
	// 第一个时间戳(8字节) + 每个delta-of-delta值(变长编码)
	buf := bytes.NewBuffer(make([]byte, 0, 8+len(timestamps)*4))

	// 写入第一个时间戳
	firstTs := timestamps[0]
	if err := binary.Write(buf, binary.LittleEndian, firstTs); err != nil {
		return nil, err
	}

	if len(timestamps) == 1 {
		return buf.Bytes(), nil
	}

	// 计算第一个delta
	prevTs := firstTs
	prevDelta := timestamps[1] - timestamps[0]

	// 写入第一个delta
	if err := binary.Write(buf, binary.LittleEndian, prevDelta); err != nil {
		return nil, err
	}

	// 对剩余时间戳进行delta-of-delta编码
	for i := 2; i < len(timestamps); i++ {
		ts := timestamps[i]
		delta := ts - prevTs
		deltaOfDelta := delta - prevDelta

		// 使用变长编码写入delta-of-delta
		if err := writeVarInt(buf, deltaOfDelta); err != nil {
			return nil, err
		}

		prevTs = ts
		prevDelta = delta
	}

	return buf.Bytes(), nil
}

// DecompressDeltaDelta 解压缩Delta-of-Delta编码的时间戳
func (c *TimestampCompressor) DecompressDeltaDelta(data []byte, count int) ([]int64, error) {
	if len(data) < 16 || count < 2 {
		return nil, fmt.Errorf("invalid data or count")
	}

	buf := bytes.NewReader(data)
	result := make([]int64, count)

	// 读取第一个时间戳
	var firstTs int64
	if err := binary.Read(buf, binary.LittleEndian, &firstTs); err != nil {
		return nil, err
	}
	result[0] = firstTs

	// 读取第一个delta
	var firstDelta int64
	if err := binary.Read(buf, binary.LittleEndian, &firstDelta); err != nil {
		return nil, err
	}
	result[1] = firstTs + firstDelta

	// 读取剩余的delta-of-delta
	prevTs := result[1]
	prevDelta := firstDelta

	for i := 2; i < count; i++ {
		deltaOfDelta, err := readVarInt(buf)
		if err != nil {
			return nil, err
		}

		delta := prevDelta + deltaOfDelta
		ts := prevTs + delta

		result[i] = ts
		prevTs = ts
		prevDelta = delta
	}

	return result, nil
}

// FloatCompressor 浮点数压缩器
type FloatCompressor struct {
	prevValue uint64
}

// NewFloatCompressor 创建新的浮点数压缩器
func NewFloatCompressor() *FloatCompressor {
	return &FloatCompressor{}
}

// CompressXOR 使用XOR编码压缩浮点数
func (c *FloatCompressor) CompressXOR(values []float64) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("empty values")
	}

	// 计算需要的字节数
	// 第一个值(8字节) + 每个XOR值(变长编码)
	buf := bytes.NewBuffer(make([]byte, 0, 8+len(values)*4))

	// 写入第一个值
	firstVal := values[0]
	if err := binary.Write(buf, binary.LittleEndian, firstVal); err != nil {
		return nil, err
	}

	if len(values) == 1 {
		return buf.Bytes(), nil
	}

	// 对剩余值进行XOR编码
	prevBits := math.Float64bits(firstVal)

	for i := 1; i < len(values); i++ {
		bits := math.Float64bits(values[i])
		xor := bits ^ prevBits

		// 使用前导零压缩
		if xor == 0 {
			// 写入一个0表示值相同
			if err := buf.WriteByte(0); err != nil {
				return nil, err
			}
		} else {
			// 计算前导零和尾随零
			leadingZeros := uint8(mathbits.LeadingZeros64(xor))
			trailingZeros := uint8(mathbits.TrailingZeros64(xor))

			// 控制字节: 1位标志 + 7位前导零数量
			controlByte := byte(0x80 | (leadingZeros & 0x7F))
			if err := buf.WriteByte(controlByte); err != nil {
				return nil, err
			}

			// 计算有效位数量
			significantBits := 64 - int(leadingZeros) - int(trailingZeros)

			// 写入尾随零数量
			if err := buf.WriteByte(trailingZeros); err != nil {
				return nil, err
			}

			// 写入有效位
			significantBytes := (significantBits + 7) / 8
			significantXor := (xor >> trailingZeros) << (8 - (significantBits % 8))

			for j := 0; j < significantBytes; j++ {
				shift := (significantBytes - j - 1) * 8
				if err := buf.WriteByte(byte(significantXor >> shift)); err != nil {
					return nil, err
				}
			}
		}

		prevBits = bits
	}

	return buf.Bytes(), nil
}

// DecompressXOR 解压缩XOR编码的浮点数
func (c *FloatCompressor) DecompressXOR(data []byte, count int) ([]float64, error) {
	if len(data) < 8 || count < 1 {
		return nil, fmt.Errorf("invalid data or count")
	}

	buf := bytes.NewReader(data)
	result := make([]float64, count)

	// 读取第一个值
	var firstVal float64
	if err := binary.Read(buf, binary.LittleEndian, &firstVal); err != nil {
		return nil, err
	}
	result[0] = firstVal

	if count == 1 {
		return result, nil
	}

	// 解压缩剩余值
	prevBits := math.Float64bits(firstVal)

	for i := 1; i < count; i++ {
		// 读取控制字节
		controlByte, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}

		if controlByte == 0 {
			// 值相同
			result[i] = math.Float64frombits(prevBits)
		} else {
			// 解析前导零数量
			leadingZeros := uint8(controlByte & 0x7F)

			// 读取尾随零数量
			trailingZeros, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}

			// 计算有效位数量
			significantBits := 64 - int(leadingZeros) - int(trailingZeros)
			significantBytes := (significantBits + 7) / 8

			// 读取有效位
			xorBytes := make([]byte, significantBytes)
			if _, err := buf.Read(xorBytes); err != nil {
				return nil, err
			}

			// 重建XOR值
			var xor uint64
			for j := 0; j < significantBytes; j++ {
				xor = (xor << 8) | uint64(xorBytes[j])
			}

			// 调整位置
			xor = xor >> (8 - (significantBits % 8))
			xor = xor << trailingZeros

			// 应用XOR
			bits := prevBits ^ xor
			result[i] = math.Float64frombits(bits)
			prevBits = bits
		}
	}

	return result, nil
}

// StringCompressor 字符串压缩器
type StringCompressor struct{}

// NewStringCompressor 创建新的字符串压缩器
func NewStringCompressor() *StringCompressor {
	return &StringCompressor{}
}

// CompressDictionary 使用字典编码压缩字符串
func (c *StringCompressor) CompressDictionary(values []string) ([]byte, []byte, error) {
	if len(values) == 0 {
		return nil, nil, fmt.Errorf("empty values")
	}

	// 构建字典
	dict := make(map[string]uint16)
	var dictEntries []string

	for _, v := range values {
		if _, exists := dict[v]; !exists {
			dict[v] = uint16(len(dictEntries))
			dictEntries = append(dictEntries, v)
		}
	}

	// 序列化字典
	dictBuf := bytes.NewBuffer(nil)

	// 写入字典大小
	if err := binary.Write(dictBuf, binary.LittleEndian, uint16(len(dictEntries))); err != nil {
		return nil, nil, err
	}

	// 写入字典条目
	for _, entry := range dictEntries {
		// 写入字符串长度
		if err := binary.Write(dictBuf, binary.LittleEndian, uint16(len(entry))); err != nil {
			return nil, nil, err
		}
		// 写入字符串内容
		if _, err := dictBuf.Write([]byte(entry)); err != nil {
			return nil, nil, err
		}
	}

	// 压缩数据
	dataBuf := bytes.NewBuffer(nil)

	// 写入值数量
	if err := binary.Write(dataBuf, binary.LittleEndian, uint32(len(values))); err != nil {
		return nil, nil, err
	}

	// 写入字典索引
	for _, v := range values {
		idx := dict[v]
		if err := binary.Write(dataBuf, binary.LittleEndian, idx); err != nil {
			return nil, nil, err
		}
	}

	return dictBuf.Bytes(), dataBuf.Bytes(), nil
}

// DecompressDictionary 解压缩字典编码的字符串
func (c *StringCompressor) DecompressDictionary(dictData, indexData []byte) ([]string, error) {
	if len(dictData) < 2 || len(indexData) < 4 {
		return nil, fmt.Errorf("invalid data")
	}

	// 读取字典
	dictBuf := bytes.NewReader(dictData)

	// 读取字典大小
	var dictSize uint16
	if err := binary.Read(dictBuf, binary.LittleEndian, &dictSize); err != nil {
		return nil, err
	}

	// 读取字典条目
	dict := make([]string, dictSize)
	for i := 0; i < int(dictSize); i++ {
		// 读取字符串长度
		var strLen uint16
		if err := binary.Read(dictBuf, binary.LittleEndian, &strLen); err != nil {
			return nil, err
		}

		// 读取字符串内容
		strBytes := make([]byte, strLen)
		if _, err := dictBuf.Read(strBytes); err != nil {
			return nil, err
		}

		dict[i] = string(strBytes)
	}

	// 读取压缩数据
	dataBuf := bytes.NewReader(indexData)

	// 读取值数量
	var count uint32
	if err := binary.Read(dataBuf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	// 读取字典索引
	result := make([]string, count)
	for i := 0; i < int(count); i++ {
		var idx uint16
		if err := binary.Read(dataBuf, binary.LittleEndian, &idx); err != nil {
			return nil, err
		}

		if int(idx) >= len(dict) {
			return nil, fmt.Errorf("invalid dictionary index: %d", idx)
		}

		result[i] = dict[idx]
	}

	return result, nil
}

// IntegerCompressor 整数压缩器
type IntegerCompressor struct{}

// NewIntegerCompressor 创建新的整数压缩器
func NewIntegerCompressor() *IntegerCompressor {
	return &IntegerCompressor{}
}

// CompressZigZag 使用ZigZag编码压缩整数
func (c *IntegerCompressor) CompressZigZag(values []int64) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("empty values")
	}

	buf := bytes.NewBuffer(nil)

	// 写入值数量
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(values))); err != nil {
		return nil, err
	}

	// 对每个值进行ZigZag编码
	for _, v := range values {
		// ZigZag编码: (v << 1) ^ (v >> 63)
		encoded := uint64((v << 1) ^ (v >> 63))

		// 使用变长编码写入
		if err := writeVarUint(buf, encoded); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecompressZigZag 解压缩ZigZag编码的整数
func (c *IntegerCompressor) DecompressZigZag(data []byte) ([]int64, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid data")
	}

	buf := bytes.NewReader(data)

	// 读取值数量
	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	result := make([]int64, count)

	// 读取每个值
	for i := 0; i < int(count); i++ {
		encoded, err := readVarUint(buf)
		if err != nil {
			return nil, err
		}

		// ZigZag解码: (encoded >> 1) ^ -(encoded & 1)
		result[i] = int64((encoded >> 1) ^ -(encoded & 1))
	}

	return result, nil
}

// 变长整数编码/解码

// writeVarInt 写入变长有符号整数
func writeVarInt(buf *bytes.Buffer, v int64) error {
	// 转换为ZigZag编码
	encoded := uint64((v << 1) ^ (v >> 63))
	return writeVarUint(buf, encoded)
}

// readVarInt 读取变长有符号整数
func readVarInt(buf *bytes.Reader) (int64, error) {
	encoded, err := readVarUint(buf)
	if err != nil {
		return 0, err
	}
	// ZigZag解码
	return int64((encoded >> 1) ^ -(encoded & 1)), nil
}

// writeVarUint 写入变长无符号整数
func writeVarUint(buf *bytes.Buffer, v uint64) error {
	for v >= 0x80 {
		if err := buf.WriteByte(byte(v) | 0x80); err != nil {
			return err
		}
		v >>= 7
	}
	return buf.WriteByte(byte(v))
}

// readVarUint 读取变长无符号整数
func readVarUint(buf *bytes.Reader) (uint64, error) {
	var result uint64
	var shift uint

	for {
		b, err := buf.ReadByte()
		if err != nil {
			return 0, err
		}

		result |= uint64(b&0x7F) << shift
		if b < 0x80 {
			break
		}
		shift += 7
	}

	return result, nil
}

// AddValue 添加值到列
func (c *Column) AddValue(value interface{}) error {
	// 根据列类型处理不同类型的值
	switch c.Type {
	case ColumnTypeTimestamp:
		ts, ok := value.(int64)
		if !ok {
			return fmt.Errorf("value is not int64 for timestamp column")
		}
		// 简单实现：直接将值序列化并追加到数据中
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(ts))
		c.Data = append(c.Data, buf...)
	case ColumnTypeInteger:
		var i int64
		switch v := value.(type) {
		case int64:
			i = v
		case int:
			i = int64(v)
		case int32:
			i = int64(v)
		default:
			return fmt.Errorf("value cannot be converted to int64 for integer column")
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(i))
		c.Data = append(c.Data, buf...)
	case ColumnTypeFloat:
		var f float64
		switch v := value.(type) {
		case float64:
			f = v
		case float32:
			f = float64(v)
		default:
			return fmt.Errorf("value cannot be converted to float64 for float column")
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(f))
		c.Data = append(c.Data, buf...)
	case ColumnTypeString:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("value is not string for string column")
		}
		// 写入字符串长度
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(s)))
		c.Data = append(c.Data, lenBuf...)
		// 写入字符串内容
		c.Data = append(c.Data, []byte(s)...)
	case ColumnTypeBoolean:
		b, ok := value.(bool)
		if !ok {
			return fmt.Errorf("value is not bool for boolean column")
		}
		if b {
			c.Data = append(c.Data, 1)
		} else {
			c.Data = append(c.Data, 0)
		}
	default:
		return fmt.Errorf("unsupported column type: %d", c.Type)
	}

	c.Count++
	return nil
}

// GetValue 获取指定索引的值
func (c *Column) GetValue(index int) (interface{}, error) {
	if index < 0 || index >= c.Count {
		return nil, fmt.Errorf("index out of range: %d", index)
	}

	switch c.Type {
	case ColumnTypeTimestamp:
		offset := index * 8
		if offset+8 > len(c.Data) {
			return nil, fmt.Errorf("data corrupted for timestamp column")
		}
		return int64(binary.LittleEndian.Uint64(c.Data[offset : offset+8])), nil
	case ColumnTypeInteger:
		offset := index * 8
		if offset+8 > len(c.Data) {
			return nil, fmt.Errorf("data corrupted for integer column")
		}
		return int64(binary.LittleEndian.Uint64(c.Data[offset : offset+8])), nil
	case ColumnTypeFloat:
		offset := index * 8
		if offset+8 > len(c.Data) {
			return nil, fmt.Errorf("data corrupted for float column")
		}
		bits := binary.LittleEndian.Uint64(c.Data[offset : offset+8])
		return math.Float64frombits(bits), nil
	case ColumnTypeString:
		// 找到字符串的起始位置
		var offset int
		for i := 0; i < index; i++ {
			if offset+4 > len(c.Data) {
				return nil, fmt.Errorf("data corrupted for string column")
			}
			strLen := int(binary.LittleEndian.Uint32(c.Data[offset : offset+4]))
			offset += 4 + strLen
		}
		if offset+4 > len(c.Data) {
			return nil, fmt.Errorf("data corrupted for string column")
		}
		strLen := int(binary.LittleEndian.Uint32(c.Data[offset : offset+4]))
		offset += 4
		if offset+strLen > len(c.Data) {
			return nil, fmt.Errorf("data corrupted for string column")
		}
		return string(c.Data[offset : offset+strLen]), nil
	case ColumnTypeBoolean:
		offset := index
		if offset >= len(c.Data) {
			return nil, fmt.Errorf("data corrupted for boolean column")
		}
		return c.Data[offset] != 0, nil
	default:
		return nil, fmt.Errorf("unsupported column type: %d", c.Type)
	}
}

// Serialize 序列化列数据
func (c *Column) Serialize() ([]byte, error) {
	// 创建缓冲区
	buf := bytes.NewBuffer(nil)

	// 写入列类型
	if err := buf.WriteByte(c.Type); err != nil {
		return nil, err
	}

	// 写入列名长度和列名
	nameLen := uint16(len(c.Name))
	if err := binary.Write(buf, binary.LittleEndian, nameLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(c.Name)); err != nil {
		return nil, err
	}

	// 根据列类型选择合适的压缩算法
	var compressedData []byte
	var err error
	var compressionType byte

	switch c.Type {
	case ColumnTypeTimestamp:
		// 提取时间戳数据
		timestamps := make([]int64, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return nil, err
			}
			timestamps[i] = val.(int64)
		}

		// 使用Gorilla压缩
		compressor := NewGorillaCompressor()
		compressedData, err = compressor.CompressTimestamps(timestamps)
		if err != nil {
			return nil, err
		}
		compressionType = CompressionGorilla
	case ColumnTypeFloat:
		// 提取浮点数据
		floats := make([]float64, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return nil, err
			}
			floats[i] = val.(float64)
		}

		// 使用Gorilla压缩
		compressor := NewGorillaCompressor()
		compressedData, err = compressor.CompressFloats(floats)
		if err != nil {
			return nil, err
		}
		compressionType = CompressionGorilla
	case ColumnTypeInteger:
		// 提取整数数据
		integers := make([]int64, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return nil, err
			}
			integers[i] = val.(int64)
		}

		// 使用ZigZag编码
		compressor := NewIntegerCompressor()
		compressedData, err = compressor.CompressZigZag(integers)
		if err != nil {
			return nil, err
		}
		compressionType = CompressionDelta
	case ColumnTypeString:
		// 提取字符串数据
		strings := make([]string, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return nil, err
			}
			strings[i] = val.(string)
		}

		// 使用字典编码
		compressor := NewStringCompressor()
		dictData, indexData, err := compressor.CompressDictionary(strings)
		if err != nil {
			return nil, err
		}

		// 合并字典数据和索引数据
		dictLen := uint32(len(dictData))
		indexLen := uint32(len(indexData))

		tempBuf := bytes.NewBuffer(nil)
		if err := binary.Write(tempBuf, binary.LittleEndian, dictLen); err != nil {
			return nil, err
		}
		if err := binary.Write(tempBuf, binary.LittleEndian, indexLen); err != nil {
			return nil, err
		}
		if _, err := tempBuf.Write(dictData); err != nil {
			return nil, err
		}
		if _, err := tempBuf.Write(indexData); err != nil {
			return nil, err
		}

		compressedData = tempBuf.Bytes()
		compressionType = CompressionDictionary
	case ColumnTypeBoolean:
		// 布尔值使用位压缩
		compressedData = make([]byte, (c.Count+7)/8)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return nil, err
			}
			if val.(bool) {
				compressedData[i/8] |= 1 << (i % 8)
			}
		}
		compressionType = CompressionNone
	default:
		return nil, fmt.Errorf("unsupported column type: %d", c.Type)
	}

	// 写入压缩类型
	if err := buf.WriteByte(compressionType); err != nil {
		return nil, err
	}

	// 写入值数量
	if err := binary.Write(buf, binary.LittleEndian, uint32(c.Count)); err != nil {
		return nil, err
	}

	// 写入压缩数据长度
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(compressedData))); err != nil {
		return nil, err
	}

	// 写入压缩数据
	if _, err := buf.Write(compressedData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize 反序列化列数据
func (c *Column) Deserialize(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}

	buf := bytes.NewReader(data)

	// 读取列类型
	colType, err := buf.ReadByte()
	if err != nil {
		return err
	}
	c.Type = colType

	// 读取列名长度和列名
	var nameLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := buf.Read(nameBytes); err != nil {
		return err
	}
	c.Name = string(nameBytes)

	// 读取压缩类型
	compression, err := buf.ReadByte()
	if err != nil {
		return err
	}
	c.Compression = compression

	// 读取值数量
	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return err
	}
	c.Count = int(count)

	// 读取压缩数据长度
	var compressedLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
		return err
	}

	// 读取压缩数据
	compressedData := make([]byte, compressedLen)
	if _, err := buf.Read(compressedData); err != nil {
		return err
	}

	// 根据列类型和压缩类型解压数据
	switch c.Type {
	case ColumnTypeTimestamp:
		if compression == CompressionGorilla {
			// 使用Gorilla解压
			compressor := NewGorillaCompressor()
			timestamps, err := compressor.DecompressTimestamps(compressedData, c.Count)
			if err != nil {
				return err
			}

			// 重建数据
			c.Data = make([]byte, c.Count*8)
			for i, ts := range timestamps {
				binary.LittleEndian.PutUint64(c.Data[i*8:], uint64(ts))
			}
		} else if compression == CompressionDeltaDelta {
			// 使用Delta-of-Delta解压
			compressor := NewTimestampCompressor()
			timestamps, err := compressor.DecompressDeltaDelta(compressedData, c.Count)
			if err != nil {
				return err
			}

			// 重建数据
			c.Data = make([]byte, c.Count*8)
			for i, ts := range timestamps {
				binary.LittleEndian.PutUint64(c.Data[i*8:], uint64(ts))
			}
		} else {
			return fmt.Errorf("unsupported compression type for timestamp: %d", compression)
		}
	case ColumnTypeFloat:
		if compression == CompressionGorilla {
			// 使用Gorilla解压
			compressor := NewGorillaCompressor()
			floats, err := compressor.DecompressFloats(compressedData, c.Count)
			if err != nil {
				return err
			}

			// 重建数据
			c.Data = make([]byte, c.Count*8)
			for i, f := range floats {
				binary.LittleEndian.PutUint64(c.Data[i*8:], math.Float64bits(f))
			}
		} else if compression == CompressionXOR {
			// 使用XOR解压
			compressor := NewFloatCompressor()
			floats, err := compressor.DecompressXOR(compressedData, c.Count)
			if err != nil {
				return err
			}

			// 重建数据
			c.Data = make([]byte, c.Count*8)
			for i, f := range floats {
				binary.LittleEndian.PutUint64(c.Data[i*8:], math.Float64bits(f))
			}
		} else {
			return fmt.Errorf("unsupported compression type for float: %d", compression)
		}
	case ColumnTypeInteger:
		if compression == CompressionDelta {
			// 使用ZigZag解压
			compressor := NewIntegerCompressor()
			integers, err := compressor.DecompressZigZag(compressedData)
			if err != nil {
				return err
			}

			// 重建数据
			c.Data = make([]byte, c.Count*8)
			for i, n := range integers {
				binary.LittleEndian.PutUint64(c.Data[i*8:], uint64(n))
			}
		} else {
			return fmt.Errorf("unsupported compression type for integer: %d", compression)
		}
	case ColumnTypeString:
		if compression == CompressionDictionary {
			// 解析字典数据和索引数据
			dictBuf := bytes.NewReader(compressedData)

			var dictLen, indexLen uint32
			if err := binary.Read(dictBuf, binary.LittleEndian, &dictLen); err != nil {
				return err
			}
			if err := binary.Read(dictBuf, binary.LittleEndian, &indexLen); err != nil {
				return err
			}

			dictData := make([]byte, dictLen)
			if _, err := dictBuf.Read(dictData); err != nil {
				return err
			}

			indexData := make([]byte, indexLen)
			if _, err := dictBuf.Read(indexData); err != nil {
				return err
			}

			// 使用字典解压
			compressor := NewStringCompressor()
			strings, err := compressor.DecompressDictionary(dictData, indexData)
			if err != nil {
				return err
			}

			// 重建数据
			var totalLen int
			for _, s := range strings {
				totalLen += 4 + len(s) // 4字节长度 + 字符串内容
			}

			c.Data = make([]byte, totalLen)
			offset := 0
			for _, s := range strings {
				binary.LittleEndian.PutUint32(c.Data[offset:], uint32(len(s)))
				offset += 4
				copy(c.Data[offset:], s)
				offset += len(s)
			}
		} else {
			return fmt.Errorf("unsupported compression type for string: %d", compression)
		}
	case ColumnTypeBoolean:
		// 布尔值使用位压缩
		c.Data = make([]byte, c.Count)
		for i := 0; i < c.Count; i++ {
			if compressedData[i/8]&(1<<(i%8)) != 0 {
				c.Data[i] = 1
			} else {
				c.Data[i] = 0
			}
		}
	default:
		return fmt.Errorf("unsupported column type: %d", c.Type)
	}

	return nil
}
