package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	// WAL操作类型
	walOpInsert byte = 1
	walOpDelete byte = 2

	// WAL文件大小限制 (64MB)
	defaultMaxWALFileSize = 64 * 1024 * 1024

	// WAL文件名格式
	walFileFormat = "%d.wal"
)

// WALEntry 表示WAL条目
type WALEntry struct {
	Type      byte   // 操作类型(插入/删除)
	Database  string // 数据库名
	Table     string // 表名
	Timestamp int64  // 时间戳
	Data      []byte // BSON编码数据
}

// WAL 表示预写日志
type WAL struct {
	dir          string        // WAL目录
	currentFile  *os.File      // 当前WAL文件
	fileSize     int64         // 当前文件大小
	maxFileSize  int64         // 最大文件大小
	mu           sync.Mutex    // 互斥锁
	syncInterval time.Duration // 同步间隔
	lastSync     time.Time     // 上次同步时间
}

// NewWAL 创建新的WAL
func NewWAL(dir string, maxFileSize int64) (*WAL, error) {
	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	if maxFileSize <= 0 {
		maxFileSize = defaultMaxWALFileSize
	}

	wal := &WAL{
		dir:          dir,
		maxFileSize:  maxFileSize,
		syncInterval: 200 * time.Millisecond,
		lastSync:     time.Now(),
	}

	// 打开或创建WAL文件
	if err := wal.openCurrentFile(); err != nil {
		return nil, err
	}

	return wal, nil
}

// openCurrentFile 打开当前WAL文件
func (w *WAL) openCurrentFile() error {
	// 查找最新的WAL文件
	files, err := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}

	var latestFile string
	var latestTime int64

	if len(files) == 0 {
		// 没有现有文件，创建新文件
		latestTime = time.Now().UnixNano()
		latestFile = filepath.Join(w.dir, fmt.Sprintf(walFileFormat, latestTime))
	} else {
		// 使用最新的文件
		latestFile = files[len(files)-1]
		// 获取文件信息
		info, err := os.Stat(latestFile)
		if err != nil {
			return fmt.Errorf("failed to stat WAL file: %w", err)
		}
		w.fileSize = info.Size()
	}

	// 打开文件
	file, err := os.OpenFile(latestFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	w.currentFile = file
	return nil
}

// Write 写入WAL条目
func (w *WAL) Write(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 检查是否需要创建新文件
	if w.fileSize >= w.maxFileSize {
		if err := w.rotateFile(); err != nil {
			return err
		}
	}

	// 序列化条目
	data, err := w.serializeEntry(entry)
	if err != nil {
		return err
	}

	// 写入文件
	n, err := w.currentFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	w.fileSize += int64(n)

	// 检查是否需要同步
	if time.Since(w.lastSync) > w.syncInterval {
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
		w.lastSync = time.Now()
	}

	return nil
}

// serializeEntry 序列化WAL条目
func (w *WAL) serializeEntry(entry WALEntry) ([]byte, error) {
	// 计算总长度
	dbLen := len(entry.Database)
	tableLen := len(entry.Table)
	dataLen := len(entry.Data)

	// 头部 + 数据库名长度 + 表名长度 + 时间戳 + 数据长度 + 数据库名 + 表名 + 数据
	totalLen := 1 + 4 + 4 + 8 + 4 + dbLen + tableLen + dataLen

	buf := make([]byte, totalLen)

	// 写入操作类型
	buf[0] = entry.Type

	// 写入数据库名长度
	binary.LittleEndian.PutUint32(buf[1:], uint32(dbLen))

	// 写入表名长度
	binary.LittleEndian.PutUint32(buf[5:], uint32(tableLen))

	// 写入时间戳
	binary.LittleEndian.PutUint64(buf[9:], uint64(entry.Timestamp))

	// 写入数据长度
	binary.LittleEndian.PutUint32(buf[17:], uint32(dataLen))

	// 写入数据库名
	copy(buf[21:], entry.Database)

	// 写入表名
	copy(buf[21+dbLen:], entry.Table)

	// 写入数据
	copy(buf[21+dbLen+tableLen:], entry.Data)

	return buf, nil
}

// rotateFile 轮换WAL文件
func (w *WAL) rotateFile() error {
	// 关闭当前文件
	if err := w.currentFile.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// 创建新文件
	newFile := filepath.Join(w.dir, fmt.Sprintf(walFileFormat, time.Now().UnixNano()))
	file, err := os.OpenFile(newFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}

	w.currentFile = file
	w.fileSize = 0
	w.lastSync = time.Now()

	return nil
}

// Sync 同步WAL文件到磁盘
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	w.lastSync = time.Now()
	return nil
}

// Close 关闭WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
		if err := w.currentFile.Close(); err != nil {
			return fmt.Errorf("failed to close WAL file: %w", err)
		}
		w.currentFile = nil
	}

	return nil
}

// Recover 从WAL恢复数据
func (w *WAL) Recover() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 关闭当前文件
	if w.currentFile != nil {
		if err := w.currentFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close current WAL file: %w", err)
		}
		w.currentFile = nil
	}

	// 获取所有WAL文件
	files, err := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}

	var entries []WALEntry

	// 按顺序处理每个文件
	for _, file := range files {
		fileEntries, err := w.recoverFile(file)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fileEntries...)
	}

	// 重新打开当前文件
	if err := w.openCurrentFile(); err != nil {
		return nil, err
	}

	return entries, nil
}

// recoverFile 从单个WAL文件恢复数据
func (w *WAL) recoverFile(filePath string) ([]WALEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	var entries []WALEntry

	for {
		entry, err := w.readEntry(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read WAL entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// readEntry 从文件读取单个WAL条目
func (w *WAL) readEntry(file *os.File) (WALEntry, error) {
	var entry WALEntry

	// 读取操作类型
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(file, typeBuf); err != nil {
		return entry, err
	}
	entry.Type = typeBuf[0]

	// 读取数据库名长度
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return entry, err
	}
	dbLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取表名长度
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return entry, err
	}
	tableLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取时间戳
	tsBuf := make([]byte, 8)
	if _, err := io.ReadFull(file, tsBuf); err != nil {
		return entry, err
	}
	entry.Timestamp = int64(binary.LittleEndian.Uint64(tsBuf))

	// 读取数据长度
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return entry, err
	}
	dataLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取数据库名
	dbBuf := make([]byte, dbLen)
	if _, err := io.ReadFull(file, dbBuf); err != nil {
		return entry, err
	}
	entry.Database = string(dbBuf)

	// 读取表名
	tableBuf := make([]byte, tableLen)
	if _, err := io.ReadFull(file, tableBuf); err != nil {
		return entry, err
	}
	entry.Table = string(tableBuf)

	// 读取数据
	dataBuf := make([]byte, dataLen)
	if _, err := io.ReadFull(file, dataBuf); err != nil {
		return entry, err
	}
	entry.Data = dataBuf

	return entry, nil
}

// CreateInsertEntry 创建插入操作的WAL条目
func CreateInsertEntry(db, table string, timestamp int64, doc bson.D) (WALEntry, error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return WALEntry{}, fmt.Errorf("failed to marshal document: %w", err)
	}

	return WALEntry{
		Type:      walOpInsert,
		Database:  db,
		Table:     table,
		Timestamp: timestamp,
		Data:      data,
	}, nil
}

// CreateDeleteEntry 创建删除操作的WAL条目
func CreateDeleteEntry(db, table string, timestamp int64) WALEntry {
	return WALEntry{
		Type:      walOpDelete,
		Database:  db,
		Table:     table,
		Timestamp: timestamp,
	}
}
