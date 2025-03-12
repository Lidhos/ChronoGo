package util

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	// 默认扇区大小，用于DirectIO对齐
	DefaultSectorSize = 512

	// 系统调用常量
	MADV_DONTNEED = 4      // 告诉内核不再需要这些页面
	O_DIRECT      = 0x4000 // 直接IO标志 (Linux)
)

// MmapFile 使用内存映射方式读写文件
type MmapFile struct {
	data     []byte
	file     *os.File
	fileSize int64
	writable bool
}

// NewMmapFile 创建新的内存映射文件
func NewMmapFile(path string, size int64, writable bool) (*MmapFile, error) {
	// 打开文件
	flags := os.O_RDONLY
	prot := syscall.PROT_READ

	if writable {
		flags = os.O_RDWR | os.O_CREATE
		prot = syscall.PROT_READ | syscall.PROT_WRITE
	}

	file, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// 如果是写入模式，确保文件大小正确
	if writable && size > 0 {
		if err := file.Truncate(size); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to truncate file: %w", err)
		}
	}

	// 获取文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	// 如果文件为空，无法映射
	if fileSize == 0 {
		if !writable {
			file.Close()
			return nil, fmt.Errorf("cannot mmap empty file in read-only mode")
		}
		// 写入模式下，确保文件至少有1字节
		if err := file.Truncate(1); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to initialize empty file: %w", err)
		}
		fileSize = 1
	}

	// 内存映射文件
	mapFlags := syscall.MAP_SHARED
	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), prot, mapFlags)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &MmapFile{
		data:     data,
		file:     file,
		fileSize: fileSize,
		writable: writable,
	}, nil
}

// Bytes 返回映射的字节切片
func (m *MmapFile) Bytes() []byte {
	return m.data
}

// Size 返回文件大小
func (m *MmapFile) Size() int64 {
	return m.fileSize
}

// Write 写入数据到指定偏移位置
func (m *MmapFile) Write(offset int64, data []byte) error {
	if !m.writable {
		return fmt.Errorf("file is not writable")
	}

	if offset+int64(len(data)) > m.fileSize {
		return fmt.Errorf("write beyond file size")
	}

	copy(m.data[offset:], data)
	return nil
}

// Sync 同步内存映射到磁盘
func (m *MmapFile) Sync() error {
	// 使用文件同步代替Msync
	return m.file.Sync()
}

// Close 关闭内存映射文件
func (m *MmapFile) Close() error {
	if err := syscall.Munmap(m.data); err != nil {
		return fmt.Errorf("failed to unmap file: %w", err)
	}

	return m.file.Close()
}

// Resize 调整内存映射文件大小
func (m *MmapFile) Resize(newSize int64) error {
	if !m.writable {
		return fmt.Errorf("file is not writable")
	}

	// 先解除映射
	if err := syscall.Munmap(m.data); err != nil {
		return fmt.Errorf("failed to unmap file: %w", err)
	}

	// 调整文件大小
	if err := m.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to resize file: %w", err)
	}

	// 重新映射
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	mapFlags := syscall.MAP_SHARED
	data, err := syscall.Mmap(int(m.file.Fd()), 0, int(newSize), prot, mapFlags)
	if err != nil {
		return fmt.Errorf("failed to remap file: %w", err)
	}

	m.data = data
	m.fileSize = newSize
	return nil
}

// OpenDirectIO 打开一个支持DirectIO的文件
func OpenDirectIO(path string, flag int, perm os.FileMode) (*os.File, error) {
	// 使用自定义的O_DIRECT常量
	return os.OpenFile(path, flag|O_DIRECT, perm)
}

// AlignedBuffer 创建一个适用于DirectIO的对齐缓冲区
func AlignedBuffer(size int) []byte {
	// 确保大小是扇区大小的倍数
	alignedSize := ((size + DefaultSectorSize - 1) / DefaultSectorSize) * DefaultSectorSize

	// 分配对齐的内存
	buf := make([]byte, alignedSize+DefaultSectorSize)
	offset := DefaultSectorSize - (uintptr(unsafe.Pointer(&buf[0])) % DefaultSectorSize)

	return buf[offset : offset+uintptr(alignedSize)]
}

// CopyFileWithSendfile 使用sendfile系统调用复制文件
func CopyFileWithSendfile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dstFile.Close()

	// 获取源文件大小
	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// 使用sendfile系统调用
	_, err = syscall.Sendfile(int(dstFile.Fd()), int(srcFile.Fd()), nil, int(srcInfo.Size()))
	if err != nil {
		return fmt.Errorf("sendfile failed: %w", err)
	}

	return nil
}

// File 获取内存映射文件的文件对象
func (m *MmapFile) File() *os.File {
	return m.file
}
