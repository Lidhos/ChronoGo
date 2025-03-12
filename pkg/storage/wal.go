package storage

import (
	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/util"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
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
	entryPool    *WALEntryPool // WAL条目对象池
	useDirectIO  bool          // 是否使用DirectIO
	alignedBuf   []byte        // 对齐的缓冲区，用于DirectIO
}

// 全局WALEntry对象池
var walEntryPool = NewWALEntryPool()

// NewWAL 创建新的WAL
func NewWAL(dir string, maxFileSize int64) (*WAL, error) {
	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	if maxFileSize <= 0 {
		maxFileSize = defaultMaxWALFileSize
	}

	// 检查是否支持DirectIO
	useDirectIO := true

	// 创建对齐的缓冲区，用于DirectIO
	var alignedBuf []byte
	if useDirectIO {
		alignedBuf = util.AlignedBuffer(64 * 1024) // 64KB对齐缓冲区
	}

	wal := &WAL{
		dir:          dir,
		maxFileSize:  maxFileSize,
		syncInterval: 200 * time.Millisecond,
		lastSync:     time.Now(),
		entryPool:    walEntryPool,
		useDirectIO:  useDirectIO,
		alignedBuf:   alignedBuf,
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
	var file *os.File
	var openErr error

	if w.useDirectIO {
		// 使用DirectIO打开文件
		file, openErr = util.OpenDirectIO(latestFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if openErr != nil {
			// 如果DirectIO失败，回退到标准IO
			logger.Printf("DirectIO not supported, falling back to standard IO: %v", openErr)
			w.useDirectIO = false
			file, openErr = os.OpenFile(latestFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		}
	} else {
		file, openErr = os.OpenFile(latestFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	}

	if openErr != nil {
		return fmt.Errorf("failed to open WAL file: %w", openErr)
	}

	w.currentFile = file
	return nil
}

// Write 写入WAL条目
func (w *WAL) Write(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 序列化条目
	data, err := w.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	// 写入数据
	var n int
	if w.useDirectIO {
		// 使用DirectIO写入
		n, err = w.writeWithDirectIO(data)
	} else {
		// 使用标准IO写入
		n, err = w.currentFile.Write(data)
	}

	if err != nil {
		return fmt.Errorf("failed to write to WAL file: %w", err)
	}

	// 更新文件大小
	w.fileSize += int64(n)

	// 检查是否需要同步
	if time.Since(w.lastSync) >= w.syncInterval {
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
		w.lastSync = time.Now()
	}

	// 检查是否需要轮换文件
	if w.fileSize >= w.maxFileSize {
		if err := w.rotateFile(); err != nil {
			return fmt.Errorf("failed to rotate WAL file: %w", err)
		}
	}

	return nil
}

// writeWithDirectIO 使用DirectIO写入数据
func (w *WAL) writeWithDirectIO(data []byte) (int, error) {
	// 确保数据对齐到扇区大小
	dataLen := len(data)
	alignedSize := ((dataLen + util.DefaultSectorSize - 1) / util.DefaultSectorSize) * util.DefaultSectorSize

	// 如果对齐缓冲区不够大，重新分配
	if len(w.alignedBuf) < alignedSize {
		w.alignedBuf = util.AlignedBuffer(alignedSize)
	}

	// 复制数据到对齐缓冲区
	copy(w.alignedBuf, data)

	// 清空未使用的部分，避免垃圾数据
	for i := dataLen; i < alignedSize; i++ {
		w.alignedBuf[i] = 0
	}

	// 使用DirectIO写入
	_, err := w.currentFile.Write(w.alignedBuf[:alignedSize])
	if err != nil {
		return 0, err
	}

	// 返回实际数据大小
	return dataLen, nil
}

// serializeEntry 序列化WAL条目
func (w *WAL) serializeEntry(entry *WALEntry) ([]byte, error) {
	// 计算条目大小
	dbLen := len(entry.Database)
	tableLen := len(entry.Table)
	dataLen := len(entry.Data)

	// 总大小 = 魔数(4) + 类型(1) + 数据库名长度(4) + 表名长度(4) + 时间戳(8) + 数据长度(4) + 数据库名 + 表名 + 数据 + CRC校验(4)
	totalLen := 4 + 1 + 4 + 4 + 8 + 4 + dbLen + tableLen + dataLen + 4

	// 创建缓冲区
	buf := make([]byte, totalLen)

	// 写入魔数 "CWAL" (ChronoGo WAL)
	copy(buf[0:], []byte("CWAL"))

	// 写入类型
	buf[4] = entry.Type

	// 写入数据库名长度
	binary.LittleEndian.PutUint32(buf[5:], uint32(dbLen))

	// 写入表名长度
	binary.LittleEndian.PutUint32(buf[9:], uint32(tableLen))

	// 写入时间戳
	binary.LittleEndian.PutUint64(buf[13:], uint64(entry.Timestamp))

	// 写入数据长度
	binary.LittleEndian.PutUint32(buf[21:], uint32(dataLen))

	// 写入数据库名
	copy(buf[25:], entry.Database)

	// 写入表名
	copy(buf[25+dbLen:], entry.Table)

	// 写入数据
	copy(buf[25+dbLen+tableLen:], entry.Data)

	// 计算并写入CRC校验
	crc := crc32.ChecksumIEEE(buf[:totalLen-4])
	binary.LittleEndian.PutUint32(buf[totalLen-4:], crc)

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

	var file *os.File
	var err error

	if w.useDirectIO {
		// 使用DirectIO打开文件
		file, err = util.OpenDirectIO(newFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			// 如果DirectIO失败，回退到标准IO
			logger.Printf("DirectIO not supported for new file, falling back to standard IO: %v", err)
			w.useDirectIO = false
			file, err = os.OpenFile(newFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		}
	} else {
		file, err = os.OpenFile(newFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	}

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
func (w *WAL) Recover() ([]*WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 获取所有WAL文件
	files, err := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}

	// 按文件序号排序
	sort.Strings(files)

	// 恢复所有文件
	var allEntries []*WALEntry
	var failedFiles int

	for _, file := range files {
		entries, err := w.recoverFile(file)
		if err != nil {
			// 记录错误但继续处理其他文件
			failedFiles++
			logger.Printf("WAL恢复警告: 从文件 %s 恢复失败: %v", file, err)
			continue
		}
		allEntries = append(allEntries, entries...)
	}

	if failedFiles > 0 {
		logger.Printf("WAL恢复: 完成恢复，共处理 %d 个文件，其中 %d 个文件恢复失败", len(files), failedFiles)
	} else {
		logger.Printf("WAL恢复: 成功恢复 %d 个文件中的 %d 个条目", len(files), len(allEntries))
	}

	return allEntries, nil
}

// recoverFile 从单个WAL文件恢复
func (w *WAL) recoverFile(filePath string) ([]*WALEntry, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	// 读取所有条目
	var entries []*WALEntry
	var corruptedEntries int

	for {
		entry, err := w.readEntry(file)
		if err != nil {
			if err == io.EOF {
				break
			}

			// 记录损坏的条目数量
			corruptedEntries++
			logger.Printf("WAL恢复警告: 文件 %s 中发现损坏的条目: %v", filePath, err)

			// 尝试跳过这个损坏的条目
			// 我们将尝试向前移动文件指针，寻找下一个有效的条目
			_, err = file.Seek(0, io.SeekCurrent)
			if err != nil {
				return entries, fmt.Errorf("failed to get current file position: %w", err)
			}

			// 尝试向前移动一个字节，然后继续读取
			_, err = file.Seek(1, io.SeekCurrent)
			if err != nil {
				// 如果无法继续，返回已恢复的条目
				logger.Printf("WAL恢复: 无法继续从文件 %s 恢复，已恢复 %d 个条目，跳过 %d 个损坏的条目",
					filePath, len(entries), corruptedEntries)
				return entries, nil
			}

			// 尝试寻找下一个魔数
			found := false
			buf := make([]byte, 1)
			magicBuf := make([]byte, 4)
			copy(magicBuf, []byte("CWA")) // 已经读取了3个字节

			for {
				n, err := file.Read(buf)
				if err != nil || n == 0 {
					break
				}

				// 移动魔数缓冲区
				magicBuf[0] = magicBuf[1]
				magicBuf[1] = magicBuf[2]
				magicBuf[2] = magicBuf[3]
				magicBuf[3] = buf[0]

				// 检查是否找到魔数
				if bytes.Equal(magicBuf, []byte("CWAL")) {
					// 找到魔数，回退4个字节，使readEntry可以正确读取
					_, err = file.Seek(-4, io.SeekCurrent)
					if err == nil {
						found = true
						break
					}
				}
			}

			if !found {
				// 如果无法找到下一个有效条目，返回已恢复的条目
				logger.Printf("WAL恢复: 无法在文件 %s 中找到下一个有效条目，已恢复 %d 个条目，跳过 %d 个损坏的条目",
					filePath, len(entries), corruptedEntries)
				return entries, nil
			}

			// 继续尝试读取下一个条目
			continue
		}

		entries = append(entries, entry)
	}

	if corruptedEntries > 0 {
		logger.Printf("WAL恢复: 文件 %s 恢复完成，共恢复 %d 个条目，跳过 %d 个损坏的条目",
			filePath, len(entries), corruptedEntries)
	}

	return entries, nil
}

// readEntry 从文件读取单个WAL条目
func (w *WAL) readEntry(file *os.File) (*WALEntry, error) {
	// 读取魔数
	magicBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, magicBuf); err != nil {
		return nil, err
	}

	// 验证魔数
	if !bytes.Equal(magicBuf, []byte("CWAL")) {
		return nil, fmt.Errorf("invalid WAL entry magic number")
	}

	// 读取类型
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(file, typeBuf); err != nil {
		return nil, err
	}
	entryType := typeBuf[0]

	// 读取数据库名长度
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return nil, err
	}
	dbLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取表名长度
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return nil, err
	}
	tableLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取时间戳
	tsBuf := make([]byte, 8)
	if _, err := io.ReadFull(file, tsBuf); err != nil {
		return nil, err
	}
	timestamp := int64(binary.LittleEndian.Uint64(tsBuf))

	// 读取数据长度
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(lenBuf)

	// 从对象池获取WALEntry
	entry := w.entryPool.Get()
	entry.Type = entryType
	entry.Timestamp = timestamp

	// 读取数据库名
	dbBuf := make([]byte, dbLen)
	if _, err := io.ReadFull(file, dbBuf); err != nil {
		w.entryPool.Put(entry)
		return nil, err
	}
	entry.Database = string(dbBuf)

	// 读取表名
	tableBuf := make([]byte, tableLen)
	if _, err := io.ReadFull(file, tableBuf); err != nil {
		w.entryPool.Put(entry)
		return nil, err
	}
	entry.Table = string(tableBuf)

	// 读取数据
	if dataLen > 0 {
		// 确保Data切片有足够的容量
		if cap(entry.Data) < int(dataLen) {
			entry.Data = make([]byte, dataLen)
		} else {
			entry.Data = entry.Data[:dataLen]
		}

		if _, err := io.ReadFull(file, entry.Data); err != nil {
			w.entryPool.Put(entry)
			return nil, err
		}
	} else {
		entry.Data = entry.Data[:0]
	}

	// 读取CRC校验
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, crcBuf); err != nil {
		w.entryPool.Put(entry)
		return nil, err
	}
	storedCRC := binary.LittleEndian.Uint32(crcBuf)

	// 计算条目的CRC
	// 重新构建条目的字节表示，不包括CRC部分
	entryBytes, err := w.serializeEntry(entry)
	if err != nil {
		w.entryPool.Put(entry)
		return nil, fmt.Errorf("failed to serialize entry for CRC check: %w", err)
	}

	// 计算CRC，不包括最后4个字节（CRC本身）
	calculatedCRC := crc32.ChecksumIEEE(entryBytes[:len(entryBytes)-4])

	// 验证CRC
	if calculatedCRC != storedCRC {
		w.entryPool.Put(entry)
		return nil, fmt.Errorf("CRC check failed: stored=%d, calculated=%d", storedCRC, calculatedCRC)
	}

	return entry, nil
}

// CreateInsertEntry 创建插入类型的WAL条目
func CreateInsertEntry(db, table string, timestamp int64, doc bson.D) (*WALEntry, error) {
	// 从对象池获取WALEntry
	entry := walEntryPool.GetInsertEntry()
	entry.Database = db
	entry.Table = table
	entry.Timestamp = timestamp

	// 序列化BSON文档
	data, err := bson.Marshal(doc)
	if err != nil {
		// 发生错误时，将对象放回池中
		walEntryPool.Put(entry)
		return nil, fmt.Errorf("failed to marshal BSON document: %w", err)
	}

	// 设置数据
	entry.Data = append(entry.Data, data...)

	return entry, nil
}

// CreateDeleteEntry 创建删除类型的WAL条目
func CreateDeleteEntry(db, table string, timestamp int64) *WALEntry {
	// 从对象池获取WALEntry
	entry := walEntryPool.GetDeleteEntry()
	entry.Database = db
	entry.Table = table
	entry.Timestamp = timestamp

	return entry
}

// CreateInsertEntryWithPool 使用对象池创建插入类型的WAL条目
func (w *WAL) CreateInsertEntryWithPool(db, table string, timestamp int64, doc bson.D) (*WALEntry, error) {
	// 从对象池获取WALEntry
	entry := w.entryPool.GetInsertEntry()
	entry.Database = db
	entry.Table = table
	entry.Timestamp = timestamp

	// 序列化BSON文档
	data, err := bson.Marshal(doc)
	if err != nil {
		// 发生错误时，将对象放回池中
		w.entryPool.Put(entry)
		return nil, fmt.Errorf("failed to marshal BSON document: %w", err)
	}

	// 设置数据
	entry.Data = append(entry.Data, data...)

	return entry, nil
}

// CreateDeleteEntryWithPool 使用对象池创建删除类型的WAL条目
func (w *WAL) CreateDeleteEntryWithPool(db, table string, timestamp int64) *WALEntry {
	// 从对象池获取WALEntry
	entry := w.entryPool.GetDeleteEntry()
	entry.Database = db
	entry.Table = table
	entry.Timestamp = timestamp
	return entry
}

// ReleaseEntry 将WAL条目放回对象池
func (w *WAL) ReleaseEntry(entry *WALEntry) {
	w.entryPool.Put(entry)
}

// Checkpoint 创建WAL检查点，清理已持久化的WAL条目
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 获取所有WAL文件
	files, err := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}

	// 按文件序号排序
	sort.Strings(files)

	// 如果只有一个或没有WAL文件，不需要清理
	if len(files) <= 1 {
		return nil
	}

	// 获取当前WAL文件路径
	currentFilePath := w.currentFile.Name()

	// 保留当前文件和最近的一个文件，删除其他文件
	// 这是一个简单的实现，实际上应该基于持久化状态决定哪些文件可以删除
	for i := 0; i < len(files)-1; i++ {
		// 跳过当前文件
		if files[i] == currentFilePath {
			continue
		}

		// 删除旧文件
		if err := os.Remove(files[i]); err != nil {
			logger.Printf("Failed to remove WAL file %s: %v", files[i], err)
		} else {
			logger.Printf("Checkpoint: removed WAL file %s", files[i])
		}
	}

	return nil
}
