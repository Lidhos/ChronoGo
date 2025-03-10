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
	CompressionAuto       byte = 7 // 自动选择最佳压缩方法
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

// BatchAddValues 批量添加值到列中
func (c *Column) BatchAddValues(values []interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// 预分配内存
	newCount := c.Count + len(values)

	// 根据列类型处理不同的数据
	switch c.Type {
	case ColumnTypeTimestamp:
		// 时间戳列
		timestamps := make([]int64, len(values))
		for i, v := range values {
			ts, ok := v.(int64)
			if !ok {
				return fmt.Errorf("value at index %d is not a timestamp", i)
			}
			timestamps[i] = ts
		}

		// 如果是第一批数据，直接存储
		if c.Count == 0 {
			c.Data = make([]byte, len(timestamps)*8)
			for i, ts := range timestamps {
				binary.LittleEndian.PutUint64(c.Data[i*8:], uint64(ts))
			}
		} else {
			// 否则，扩展现有数据
			newData := make([]byte, newCount*8)
			copy(newData, c.Data)
			for i, ts := range timestamps {
				binary.LittleEndian.PutUint64(newData[(c.Count+i)*8:], uint64(ts))
			}
			c.Data = newData
		}

		// 更新最小值和最大值
		if c.Count == 0 || timestamps[0] < int64(binary.LittleEndian.Uint64(c.Min)) {
			c.Min = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.Min, uint64(timestamps[0]))
		}
		if c.Count == 0 || timestamps[len(timestamps)-1] > int64(binary.LittleEndian.Uint64(c.Max)) {
			c.Max = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.Max, uint64(timestamps[len(timestamps)-1]))
		}

	case ColumnTypeFloat:
		// 浮点数列
		floats := make([]float64, len(values))
		for i, v := range values {
			f, ok := v.(float64)
			if !ok {
				return fmt.Errorf("value at index %d is not a float", i)
			}
			floats[i] = f
		}

		// 如果是第一批数据，直接存储
		if c.Count == 0 {
			c.Data = make([]byte, len(floats)*8)
			for i, f := range floats {
				binary.LittleEndian.PutUint64(c.Data[i*8:], math.Float64bits(f))
			}
		} else {
			// 否则，扩展现有数据
			newData := make([]byte, newCount*8)
			copy(newData, c.Data)
			for i, f := range floats {
				binary.LittleEndian.PutUint64(newData[(c.Count+i)*8:], math.Float64bits(f))
			}
			c.Data = newData
		}

		// 更新最小值和最大值
		minFloat := floats[0]
		maxFloat := floats[0]
		for _, f := range floats {
			if f < minFloat {
				minFloat = f
			}
			if f > maxFloat {
				maxFloat = f
			}
		}

		if c.Count == 0 || minFloat < math.Float64frombits(binary.LittleEndian.Uint64(c.Min)) {
			c.Min = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.Min, math.Float64bits(minFloat))
		}
		if c.Count == 0 || maxFloat > math.Float64frombits(binary.LittleEndian.Uint64(c.Max)) {
			c.Max = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.Max, math.Float64bits(maxFloat))
		}

	case ColumnTypeInteger:
		// 整数列
		integers := make([]int64, len(values))
		for i, v := range values {
			n, ok := v.(int64)
			if !ok {
				return fmt.Errorf("value at index %d is not an integer", i)
			}
			integers[i] = n
		}

		// 如果是第一批数据，直接存储
		if c.Count == 0 {
			c.Data = make([]byte, len(integers)*8)
			for i, n := range integers {
				binary.LittleEndian.PutUint64(c.Data[i*8:], uint64(n))
			}
		} else {
			// 否则，扩展现有数据
			newData := make([]byte, newCount*8)
			copy(newData, c.Data)
			for i, n := range integers {
				binary.LittleEndian.PutUint64(newData[(c.Count+i)*8:], uint64(n))
			}
			c.Data = newData
		}

		// 更新最小值和最大值
		minInt := integers[0]
		maxInt := integers[0]
		for _, n := range integers {
			if n < minInt {
				minInt = n
			}
			if n > maxInt {
				maxInt = n
			}
		}

		if c.Count == 0 || minInt < int64(binary.LittleEndian.Uint64(c.Min)) {
			c.Min = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.Min, uint64(minInt))
		}
		if c.Count == 0 || maxInt > int64(binary.LittleEndian.Uint64(c.Max)) {
			c.Max = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.Max, uint64(maxInt))
		}

	case ColumnTypeString:
		// 字符串列 - 使用偏移量表示
		offsets := make([]uint32, newCount+1)
		if c.Count > 0 {
			// 复制现有偏移量
			for i := 0; i <= c.Count; i++ {
				offsets[i] = binary.LittleEndian.Uint32(c.Data[i*4:])
			}
		} else {
			offsets[0] = 0
		}

		// 计算新的字符串数据大小
		totalLen := uint32(0)
		if c.Count > 0 {
			totalLen = binary.LittleEndian.Uint32(c.Data[c.Count*4:])
		}

		for _, v := range values {
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("value is not a string")
			}
			totalLen += uint32(len(s))
			offsets[c.Count+1] = totalLen
			c.Count++
		}

		// 创建新的数据缓冲区
		newData := make([]byte, int(uint32(newCount+1)*4+totalLen))

		// 写入偏移量
		for i := 0; i <= newCount; i++ {
			binary.LittleEndian.PutUint32(newData[i*4:], offsets[i])
		}

		// 复制现有字符串数据
		if c.Count > 0 {
			oldDataStart := (c.Count + 1) * 4
			oldDataLen := binary.LittleEndian.Uint32(c.Data[c.Count*4:])
			copy(newData[(newCount+1)*4:], c.Data[oldDataStart:oldDataStart+int(oldDataLen)])
		}

		// 写入新的字符串数据
		dataPos := (newCount + 1) * 4
		for i, v := range values {
			s := v.(string)
			pos := dataPos + int(offsets[c.Count-len(values)+i])
			copy(newData[pos:], s)
		}

		c.Data = newData
		c.Count = newCount
		return nil

	case ColumnTypeBoolean:
		// 布尔值列 - 使用位压缩
		if c.Count == 0 {
			c.Data = make([]byte, (len(values)+7)/8)
		} else {
			// 扩展数据缓冲区
			newSize := (newCount + 7) / 8
			if len(c.Data) < newSize {
				newData := make([]byte, newSize)
				copy(newData, c.Data)
				c.Data = newData
			}
		}

		// 写入新的布尔值
		for i, v := range values {
			b, ok := v.(bool)
			if !ok {
				return fmt.Errorf("value at index %d is not a boolean", i)
			}

			if b {
				bytePos := (c.Count + i) / 8
				bitPos := (c.Count + i) % 8
				c.Data[bytePos] |= 1 << bitPos
			}
		}
	}

	c.Count = newCount
	return nil
}

// BatchGetValues 批量获取列中的值
func (c *Column) BatchGetValues(startIndex, count int) ([]interface{}, error) {
	if startIndex < 0 || startIndex+count > c.Count {
		return nil, fmt.Errorf("index out of range")
	}

	result := make([]interface{}, count)

	switch c.Type {
	case ColumnTypeTimestamp, ColumnTypeInteger:
		// 时间戳和整数列
		for i := 0; i < count; i++ {
			idx := startIndex + i
			offset := idx * 8
			value := int64(binary.LittleEndian.Uint64(c.Data[offset:]))
			result[i] = value
		}

	case ColumnTypeFloat:
		// 浮点数列
		for i := 0; i < count; i++ {
			idx := startIndex + i
			offset := idx * 8
			bits := binary.LittleEndian.Uint64(c.Data[offset:])
			result[i] = math.Float64frombits(bits)
		}

	case ColumnTypeString:
		// 字符串列
		for i := 0; i < count; i++ {
			idx := startIndex + i
			startOffset := binary.LittleEndian.Uint32(c.Data[idx*4:])
			endOffset := binary.LittleEndian.Uint32(c.Data[(idx+1)*4:])
			dataStart := (c.Count + 1) * 4

			strBytes := c.Data[dataStart+int(startOffset) : dataStart+int(endOffset)]
			result[i] = string(strBytes)
		}

	case ColumnTypeBoolean:
		// 布尔值列
		for i := 0; i < count; i++ {
			idx := startIndex + i
			bytePos := idx / 8
			bitPos := idx % 8
			result[i] = (c.Data[bytePos] & (1 << bitPos)) != 0
		}
	}

	return result, nil
}

// CompressColumnAuto 自适应压缩，根据数据特性选择最佳压缩方法
func (c *Column) CompressColumnAuto() error {
	// 如果列为空，不需要压缩
	if c.Count == 0 {
		return nil
	}

	// 根据列类型选择合适的压缩算法
	switch c.Type {
	case ColumnTypeTimestamp:
		return c.compressTimestampAuto()
	case ColumnTypeFloat:
		return c.compressFloatAuto()
	case ColumnTypeInteger:
		return c.compressIntegerAuto()
	case ColumnTypeString:
		return c.compressStringAuto()
	case ColumnTypeBoolean:
		// 布尔值已经使用位压缩，不需要额外压缩
		c.Compression = CompressionNone
		return nil
	default:
		return fmt.Errorf("unsupported column type: %d", c.Type)
	}
}

// compressTimestampAuto 自适应压缩时间戳列
func (c *Column) compressTimestampAuto() error {
	// 提取时间戳数据
	timestamps := make([]int64, c.Count)
	for i := 0; i < c.Count; i++ {
		val, err := c.GetValue(i)
		if err != nil {
			return err
		}
		timestamps[i] = val.(int64)
	}

	// 检查时间戳是否有规律的间隔
	isRegular := true
	if len(timestamps) > 2 {
		delta := timestamps[1] - timestamps[0]
		for i := 2; i < len(timestamps); i++ {
			if timestamps[i]-timestamps[i-1] != delta {
				isRegular = false
				break
			}
		}
	}

	// 尝试不同的压缩方法并选择最佳的
	var bestData []byte
	var bestCompression byte
	var minSize int = len(timestamps) * 8 // 原始大小

	// 1. 尝试Delta-of-Delta编码
	compressor1 := NewTimestampCompressor()
	deltaDeltaData, err := compressor1.CompressDeltaDelta(timestamps)
	if err == nil && len(deltaDeltaData) < minSize {
		bestData = deltaDeltaData
		bestCompression = CompressionDeltaDelta
		minSize = len(deltaDeltaData)
	}

	// 2. 尝试Gorilla压缩
	compressor2 := NewGorillaCompressor()
	gorillaData, err := compressor2.CompressTimestamps(timestamps)
	if err == nil && len(gorillaData) < minSize {
		bestData = gorillaData
		bestCompression = CompressionGorilla
		minSize = len(gorillaData)
	}

	// 如果时间戳有规律间隔，Delta编码可能更好
	if isRegular {
		// 使用简单的Delta编码
		deltaData := make([]byte, 8+len(timestamps)*4) // 第一个值(8字节) + 每个delta值(4字节)
		binary.LittleEndian.PutUint64(deltaData[:8], uint64(timestamps[0]))

		delta := timestamps[1] - timestamps[0]
		binary.LittleEndian.PutUint32(deltaData[8:12], uint32(delta))

		if len(deltaData) < minSize {
			bestData = deltaData
			bestCompression = CompressionDelta
			minSize = len(deltaData)
		}
	}

	// 更新列的压缩数据和压缩类型
	if bestData != nil {
		c.Data = bestData
		c.Compression = bestCompression
	}

	return nil
}

// compressFloatAuto 自适应压缩浮点数列
func (c *Column) compressFloatAuto() error {
	// 提取浮点数据
	floats := make([]float64, c.Count)
	for i := 0; i < c.Count; i++ {
		val, err := c.GetValue(i)
		if err != nil {
			return err
		}
		floats[i] = val.(float64)
	}

	// 检查数据特性
	// 1. 计算数据的变化率
	changeRates := make([]float64, len(floats)-1)
	for i := 1; i < len(floats); i++ {
		changeRates[i-1] = math.Abs(floats[i] - floats[i-1])
	}

	// 计算平均变化率
	avgChangeRate := 0.0
	for _, rate := range changeRates {
		avgChangeRate += rate
	}
	if len(changeRates) > 0 {
		avgChangeRate /= float64(len(changeRates))
	}

	// 2. 检查是否有大量重复值
	valueCount := make(map[uint64]int)
	for _, f := range floats {
		bits := math.Float64bits(f)
		valueCount[bits]++
	}

	// 计算唯一值的比例
	uniqueRatio := float64(len(valueCount)) / float64(len(floats))

	// 尝试不同的压缩方法并选择最佳的
	var bestData []byte
	var bestCompression byte
	var minSize int = len(floats) * 8 // 原始大小

	// 1. 尝试XOR编码 - 适合变化平缓的数据
	if avgChangeRate < 1.0 {
		compressor1 := NewFloatCompressor()
		xorData, err := compressor1.CompressXOR(floats)
		if err == nil && len(xorData) < minSize {
			bestData = xorData
			bestCompression = CompressionXOR
			minSize = len(xorData)
		}
	}

	// 2. 尝试Gorilla压缩 - 通常对时序数据效果好
	compressor2 := NewGorillaCompressor()
	gorillaData, err := compressor2.CompressFloats(floats)
	if err == nil && len(gorillaData) < minSize {
		bestData = gorillaData
		bestCompression = CompressionGorilla
		minSize = len(gorillaData)
	}

	// 3. 如果有大量重复值，考虑使用字典编码
	if uniqueRatio < 0.1 { // 如果唯一值少于10%
		// 创建字典
		dict := make(map[uint64]int)
		dictValues := make([]uint64, 0, len(valueCount))

		for bits := range valueCount {
			dict[bits] = len(dictValues)
			dictValues = append(dictValues, bits)
		}

		// 编码数据
		dictData := make([]byte, len(dictValues)*8)
		for i, bits := range dictValues {
			binary.LittleEndian.PutUint64(dictData[i*8:], bits)
		}

		// 创建索引
		indexData := make([]byte, len(floats)*2) // 假设索引需要2字节
		for i, f := range floats {
			bits := math.Float64bits(f)
			idx := dict[bits]
			binary.LittleEndian.PutUint16(indexData[i*2:], uint16(idx))
		}

		// 合并字典和索引
		dictLen := uint32(len(dictData))
		indexLen := uint32(len(indexData))

		dictCompressedData := make([]byte, 8+len(dictData)+len(indexData))
		binary.LittleEndian.PutUint32(dictCompressedData[0:], dictLen)
		binary.LittleEndian.PutUint32(dictCompressedData[4:], indexLen)
		copy(dictCompressedData[8:], dictData)
		copy(dictCompressedData[8+len(dictData):], indexData)

		if len(dictCompressedData) < minSize {
			bestData = indexData
			c.DictionaryData = dictData
			bestCompression = CompressionDictionary
			minSize = len(dictCompressedData)
		}
	}

	// 更新列的压缩数据和压缩类型
	if bestData != nil {
		c.Data = bestData
		c.Compression = bestCompression
	}

	return nil
}

// compressIntegerAuto 自适应压缩整数列
func (c *Column) compressIntegerAuto() error {
	// 提取整数数据
	integers := make([]int64, c.Count)
	for i := 0; i < c.Count; i++ {
		val, err := c.GetValue(i)
		if err != nil {
			return err
		}
		integers[i] = val.(int64)
	}

	// 检查数据特性
	// 1. 计算数据范围
	minVal := integers[0]
	maxVal := integers[0]
	for _, n := range integers {
		if n < minVal {
			minVal = n
		}
		if n > maxVal {
			maxVal = n
		}
	}
	dataRange := maxVal - minVal

	// 2. 检查是否有大量重复值
	valueCount := make(map[int64]int)
	for _, n := range integers {
		valueCount[n]++
	}

	// 计算唯一值的比例
	uniqueRatio := float64(len(valueCount)) / float64(len(integers))

	// 尝试不同的压缩方法并选择最佳的
	var bestData []byte
	var bestCompression byte
	var minSize int = len(integers) * 8 // 原始大小

	// 1. 尝试ZigZag编码 - 适合有符号整数
	compressor1 := NewIntegerCompressor()
	zigzagData, err := compressor1.CompressZigZag(integers)
	if err == nil && len(zigzagData) < minSize {
		bestData = zigzagData
		bestCompression = CompressionDelta
		minSize = len(zigzagData)
	}

	// 2. 如果数据范围小，考虑使用Delta编码
	if dataRange < 1000 && len(integers) > 1 {
		// 使用简单的Delta编码
		deltaData := make([]byte, 8+len(integers)*4) // 第一个值(8字节) + 每个delta值(4字节)
		binary.LittleEndian.PutUint64(deltaData[:8], uint64(integers[0]))

		for i := 1; i < len(integers); i++ {
			delta := integers[i] - integers[i-1]
			binary.LittleEndian.PutUint32(deltaData[8+(i-1)*4:], uint32(delta))
		}

		if len(deltaData) < minSize {
			bestData = deltaData
			bestCompression = CompressionDelta
			minSize = len(deltaData)
		}
	}

	// 3. 如果有大量重复值，考虑使用字典编码
	if uniqueRatio < 0.1 { // 如果唯一值少于10%
		// 创建字典
		dict := make(map[int64]int)
		dictValues := make([]int64, 0, len(valueCount))

		for n := range valueCount {
			dict[n] = len(dictValues)
			dictValues = append(dictValues, n)
		}

		// 编码数据
		dictData := make([]byte, len(dictValues)*8)
		for i, n := range dictValues {
			binary.LittleEndian.PutUint64(dictData[i*8:], uint64(n))
		}

		// 创建索引
		indexData := make([]byte, len(integers)*2) // 假设索引需要2字节
		for i, n := range integers {
			idx := dict[n]
			binary.LittleEndian.PutUint16(indexData[i*2:], uint16(idx))
		}

		// 合并字典和索引
		dictLen := uint32(len(dictData))
		indexLen := uint32(len(indexData))

		dictCompressedData := make([]byte, 8+len(dictData)+len(indexData))
		binary.LittleEndian.PutUint32(dictCompressedData[0:], dictLen)
		binary.LittleEndian.PutUint32(dictCompressedData[4:], indexLen)
		copy(dictCompressedData[8:], dictData)
		copy(dictCompressedData[8+len(dictData):], indexData)

		if len(dictCompressedData) < minSize {
			bestData = indexData
			c.DictionaryData = dictData
			bestCompression = CompressionDictionary
			minSize = len(dictCompressedData)
		}
	}

	// 更新列的压缩数据和压缩类型
	if bestData != nil {
		c.Data = bestData
		c.Compression = bestCompression
	}

	return nil
}

// compressStringAuto 自适应压缩字符串列
func (c *Column) compressStringAuto() error {
	// 提取字符串数据
	strings := make([]string, c.Count)
	for i := 0; i < c.Count; i++ {
		val, err := c.GetValue(i)
		if err != nil {
			return err
		}
		strings[i] = val.(string)
	}

	// 检查数据特性
	// 1. 计算平均字符串长度
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}
	avgLen := float64(totalLen) / float64(len(strings))

	// 2. 检查是否有大量重复值
	valueCount := make(map[string]int)
	for _, s := range strings {
		valueCount[s]++
	}

	// 计算唯一值的比例
	uniqueRatio := float64(len(valueCount)) / float64(len(strings))

	// 3. 检查前缀重复情况
	prefixCount := make(map[string]int)
	if avgLen > 5 { // 只对较长的字符串检查前缀
		for _, s := range strings {
			if len(s) >= 3 {
				prefix := s[:3]
				prefixCount[prefix]++
			}
		}
	}

	// 找到最常见的前缀
	var commonPrefix string
	var maxCount int
	for prefix, count := range prefixCount {
		if count > maxCount {
			maxCount = count
			commonPrefix = prefix
		}
	}

	// 计算前缀重复率
	prefixRatio := float64(maxCount) / float64(len(strings))

	// 尝试不同的压缩方法并选择最佳的
	var bestData []byte
	var bestDictData []byte
	var bestCompression byte
	var minSize int = totalLen + len(strings)*4 // 原始大小（假设每个字符串需要4字节的偏移量）

	// 1. 尝试字典编码 - 适合有大量重复值的情况
	if uniqueRatio < 0.5 { // 如果唯一值少于50%
		compressor := NewStringCompressor()
		dictData, indexData, err := compressor.CompressDictionary(strings)
		if err == nil {
			totalSize := len(dictData) + len(indexData)
			if totalSize < minSize {
				bestData = indexData
				bestDictData = dictData
				bestCompression = CompressionDictionary
				minSize = totalSize
			}
		}
	}

	// 2. 如果有共同前缀，考虑前缀压缩
	if prefixRatio > 0.5 && len(commonPrefix) > 0 {
		// 简单的前缀压缩实现
		prefixData := []byte(commonPrefix)
		prefixLen := uint8(len(prefixData))

		// 创建压缩数据
		compressedData := make([]byte, 1+len(prefixData)+totalLen)
		compressedData[0] = prefixLen
		copy(compressedData[1:], prefixData)

		// 写入每个字符串，如果以公共前缀开头则省略前缀
		offset := 1 + len(prefixData)
		for _, s := range strings {
			if len(s) >= len(commonPrefix) && s[:len(commonPrefix)] == commonPrefix {
				// 写入不带前缀的部分
				suffix := s[len(commonPrefix):]
				copy(compressedData[offset:], suffix)
				offset += len(suffix)
			} else {
				// 写入完整字符串
				copy(compressedData[offset:], s)
				offset += len(s)
			}
			// 写入分隔符
			compressedData[offset] = 0
			offset++
		}

		// 截断到实际大小
		compressedData = compressedData[:offset]

		if len(compressedData) < minSize {
			bestData = compressedData
			bestCompression = CompressionRLE // 使用RLE类型表示前缀压缩
			minSize = len(compressedData)
		}
	}

	// 更新列的压缩数据和压缩类型
	if bestData != nil {
		c.Data = bestData
		c.Compression = bestCompression
		if bestCompression == CompressionDictionary {
			c.DictionaryData = bestDictData
		}
	}

	return nil
}

// CompressColumn 根据列类型选择最佳压缩算法压缩列数据
func (c *Column) CompressColumn() error {
	// 如果列为空，不需要压缩
	if c.Count == 0 {
		return nil
	}

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
				return err
			}
			timestamps[i] = val.(int64)
		}

		// 使用Gorilla压缩
		compressor := NewGorillaCompressor()
		compressedData, err = compressor.CompressTimestamps(timestamps)
		if err != nil {
			return err
		}
		compressionType = CompressionGorilla

	case ColumnTypeFloat:
		// 提取浮点数据
		floats := make([]float64, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return err
			}
			floats[i] = val.(float64)
		}

		// 使用XOR压缩
		compressor := NewFloatCompressor()
		compressedData, err = compressor.CompressXOR(floats)
		if err != nil {
			return err
		}
		compressionType = CompressionXOR

	case ColumnTypeInteger:
		// 提取整数数据
		integers := make([]int64, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return err
			}
			integers[i] = val.(int64)
		}

		// 使用ZigZag编码
		compressor := NewIntegerCompressor()
		compressedData, err = compressor.CompressZigZag(integers)
		if err != nil {
			return err
		}
		compressionType = CompressionDelta

	case ColumnTypeString:
		// 提取字符串数据
		strings := make([]string, c.Count)
		for i := 0; i < c.Count; i++ {
			val, err := c.GetValue(i)
			if err != nil {
				return err
			}
			strings[i] = val.(string)
		}

		// 使用字典编码
		compressor := NewStringCompressor()
		dictData, indexData, err := compressor.CompressDictionary(strings)
		if err != nil {
			return err
		}

		// 保存字典数据
		c.DictionaryData = dictData
		compressedData = indexData
		compressionType = CompressionDictionary

	case ColumnTypeBoolean:
		// 布尔值已经使用位压缩，不需要额外压缩
		compressedData = c.Data
		compressionType = CompressionNone
	}

	// 更新列的压缩数据和压缩类型
	c.Data = compressedData
	c.Compression = compressionType

	return nil
}

// DecompressColumn 解压缩列数据
func (c *Column) DecompressColumn() error {
	// 如果列为空或未压缩，不需要解压缩
	if c.Count == 0 || c.Compression == CompressionNone {
		return nil
	}

	var decompressedData []byte

	switch c.Type {
	case ColumnTypeTimestamp:
		if c.Compression == CompressionGorilla {
			// 使用Gorilla解压缩
			compressor := NewGorillaCompressor()
			timestamps, err := compressor.DecompressTimestamps(c.Data, c.Count)
			if err != nil {
				return err
			}

			// 将解压缩的时间戳转换为字节
			decompressedData = make([]byte, c.Count*8)
			for i, ts := range timestamps {
				binary.LittleEndian.PutUint64(decompressedData[i*8:], uint64(ts))
			}
		} else if c.Compression == CompressionDeltaDelta {
			// 使用Delta-of-Delta解压缩
			compressor := NewTimestampCompressor()
			timestamps, err := compressor.DecompressDeltaDelta(c.Data, c.Count)
			if err != nil {
				return err
			}

			// 将解压缩的时间戳转换为字节
			decompressedData = make([]byte, c.Count*8)
			for i, ts := range timestamps {
				binary.LittleEndian.PutUint64(decompressedData[i*8:], uint64(ts))
			}
		}

	case ColumnTypeFloat:
		if c.Compression == CompressionXOR {
			// 使用XOR解压缩
			compressor := NewFloatCompressor()
			floats, err := compressor.DecompressXOR(c.Data, c.Count)
			if err != nil {
				return err
			}

			// 将解压缩的浮点数转换为字节
			decompressedData = make([]byte, c.Count*8)
			for i, f := range floats {
				binary.LittleEndian.PutUint64(decompressedData[i*8:], math.Float64bits(f))
			}
		}

	case ColumnTypeInteger:
		if c.Compression == CompressionDelta {
			// 使用ZigZag解压缩
			compressor := NewIntegerCompressor()
			integers, err := compressor.DecompressZigZag(c.Data)
			if err != nil {
				return err
			}

			// 将解压缩的整数转换为字节
			decompressedData = make([]byte, c.Count*8)
			for i, n := range integers {
				binary.LittleEndian.PutUint64(decompressedData[i*8:], uint64(n))
			}
		}

	case ColumnTypeString:
		if c.Compression == CompressionDictionary {
			// 使用字典解压缩
			compressor := NewStringCompressor()
			strings, err := compressor.DecompressDictionary(c.DictionaryData, c.Data)
			if err != nil {
				return err
			}

			// 将解压缩的字符串转换为偏移量格式
			totalLen := 0
			for _, s := range strings {
				totalLen += len(s)
			}

			decompressedData = make([]byte, (c.Count+1)*4+totalLen)
			offset := uint32(0)

			// 写入偏移量
			for i, s := range strings {
				binary.LittleEndian.PutUint32(decompressedData[i*4:], offset)
				offset += uint32(len(s))
			}
			binary.LittleEndian.PutUint32(decompressedData[c.Count*4:], offset)

			// 写入字符串数据
			dataStart := (c.Count + 1) * 4
			for i, s := range strings {
				startOffset := binary.LittleEndian.Uint32(decompressedData[i*4:])
				copy(decompressedData[dataStart+int(startOffset):], s)
			}
		}
	}

	// 更新列的数据和压缩类型
	if decompressedData != nil {
		c.Data = decompressedData
		c.Compression = CompressionNone
	}

	return nil
}
