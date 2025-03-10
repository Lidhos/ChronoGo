package util

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
)

// Buffer 是一个用于序列化和反序列化的缓冲区
type Buffer struct {
	data   []byte
	offset int
}

// NewBuffer 创建一个新的缓冲区
func NewBuffer(data []byte) *Buffer {
	if data == nil {
		data = make([]byte, 0, 1024)
	}
	return &Buffer{
		data:   data,
		offset: 0,
	}
}

// Write 写入字节
func (b *Buffer) Write(p []byte) (n int, err error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

// Bytes 返回缓冲区中的字节
func (b *Buffer) Bytes() []byte {
	return b.data
}

// WriteUint8 写入uint8
func (b *Buffer) WriteUint8(v uint8) {
	b.data = append(b.data, v)
}

// WriteUint16 写入uint16
func (b *Buffer) WriteUint16(v uint16) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, v)
	b.data = append(b.data, buf...)
}

// WriteUint32 写入uint32
func (b *Buffer) WriteUint32(v uint32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	b.data = append(b.data, buf...)
}

// WriteUint64 写入uint64
func (b *Buffer) WriteUint64(v uint64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	b.data = append(b.data, buf...)
}

// WriteInt8 写入int8
func (b *Buffer) WriteInt8(v int8) {
	b.data = append(b.data, byte(v))
}

// WriteInt16 写入int16
func (b *Buffer) WriteInt16(v int16) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(v))
	b.data = append(b.data, buf...)
}

// WriteInt32 写入int32
func (b *Buffer) WriteInt32(v int32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	b.data = append(b.data, buf...)
}

// WriteInt64 写入int64
func (b *Buffer) WriteInt64(v int64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	b.data = append(b.data, buf...)
}

// WriteFloat32 写入float32
func (b *Buffer) WriteFloat32(v float32) {
	bits := binary.LittleEndian.Uint32([]byte{0, 0, 0, 0})
	binary.LittleEndian.PutUint32([]byte{0, 0, 0, 0}, bits)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, bits)
	b.data = append(b.data, buf...)
}

// WriteFloat64 写入float64
func (b *Buffer) WriteFloat64(v float64) {
	bits := binary.LittleEndian.Uint64([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.LittleEndian.PutUint64([]byte{0, 0, 0, 0, 0, 0, 0, 0}, bits)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, bits)
	b.data = append(b.data, buf...)
}

// WriteString 写入字符串
func (b *Buffer) WriteString(v string) {
	// 写入字符串长度
	b.WriteUint32(uint32(len(v)))
	// 写入字符串内容
	b.data = append(b.data, []byte(v)...)
}

// WriteBool 写入布尔值
func (b *Buffer) WriteBool(v bool) {
	if v {
		b.data = append(b.data, 1)
	} else {
		b.data = append(b.data, 0)
	}
}

// ReadUint8 读取uint8
func (b *Buffer) ReadUint8() (uint8, error) {
	if b.offset >= len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := b.data[b.offset]
	b.offset++
	return v, nil
}

// ReadUint16 读取uint16
func (b *Buffer) ReadUint16() (uint16, error) {
	if b.offset+2 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := binary.LittleEndian.Uint16(b.data[b.offset : b.offset+2])
	b.offset += 2
	return v, nil
}

// ReadUint32 读取uint32
func (b *Buffer) ReadUint32() (uint32, error) {
	if b.offset+4 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := binary.LittleEndian.Uint32(b.data[b.offset : b.offset+4])
	b.offset += 4
	return v, nil
}

// ReadUint64 读取uint64
func (b *Buffer) ReadUint64() (uint64, error) {
	if b.offset+8 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := binary.LittleEndian.Uint64(b.data[b.offset : b.offset+8])
	b.offset += 8
	return v, nil
}

// ReadInt8 读取int8
func (b *Buffer) ReadInt8() (int8, error) {
	if b.offset >= len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := int8(b.data[b.offset])
	b.offset++
	return v, nil
}

// ReadInt16 读取int16
func (b *Buffer) ReadInt16() (int16, error) {
	if b.offset+2 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := int16(binary.LittleEndian.Uint16(b.data[b.offset : b.offset+2]))
	b.offset += 2
	return v, nil
}

// ReadInt32 读取int32
func (b *Buffer) ReadInt32() (int32, error) {
	if b.offset+4 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := int32(binary.LittleEndian.Uint32(b.data[b.offset : b.offset+4]))
	b.offset += 4
	return v, nil
}

// ReadInt64 读取int64
func (b *Buffer) ReadInt64() (int64, error) {
	if b.offset+8 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	v := int64(binary.LittleEndian.Uint64(b.data[b.offset : b.offset+8]))
	b.offset += 8
	return v, nil
}

// ReadFloat32 读取float32
func (b *Buffer) ReadFloat32() (float32, error) {
	if b.offset+4 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	bits := binary.LittleEndian.Uint32(b.data[b.offset : b.offset+4])
	b.offset += 4
	return float32(bits), nil
}

// ReadFloat64 读取float64
func (b *Buffer) ReadFloat64() (float64, error) {
	if b.offset+8 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow")
	}
	bits := binary.LittleEndian.Uint64(b.data[b.offset : b.offset+8])
	b.offset += 8
	return float64(bits), nil
}

// ReadString 读取字符串
func (b *Buffer) ReadString() (string, error) {
	// 读取字符串长度
	length, err := b.ReadUint32()
	if err != nil {
		return "", err
	}
	if b.offset+int(length) > len(b.data) {
		return "", fmt.Errorf("buffer overflow")
	}
	// 读取字符串内容
	v := string(b.data[b.offset : b.offset+int(length)])
	b.offset += int(length)
	return v, nil
}

// ReadBool 读取布尔值
func (b *Buffer) ReadBool() (bool, error) {
	if b.offset >= len(b.data) {
		return false, fmt.Errorf("buffer overflow")
	}
	v := b.data[b.offset] != 0
	b.offset++
	return v, nil
}

// ReadBytes 读取指定长度的字节
func (b *Buffer) ReadBytes(length int) ([]byte, error) {
	if b.offset+length > len(b.data) {
		return nil, fmt.Errorf("buffer overflow")
	}
	v := b.data[b.offset : b.offset+length]
	b.offset += length
	return v, nil
}

// CRC32 计算CRC32校验和
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// SplitColumnName 分割列名
func SplitColumnName(name string) []string {
	return strings.SplitN(name, ".", 2)
}
