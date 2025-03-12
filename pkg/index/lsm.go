package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 常量定义
const (
	// LSM树索引文件扩展名
	lsmFileExt = ".lsm"

	// LSM树索引文件名格式: dbname_collection_indexname_level_timestamp.lsm
	lsmFileFormat = "%s_%s_%s_%d_%d%s"

	// LSM树索引默认MemTable大小限制 (4MB)
	defaultMemTableSizeLimit = 4 * 1024 * 1024

	// LSM树索引默认布隆过滤器误判率 (0.01 = 1%)
	defaultBloomFilterFPRate = 0.01

	// LSM树索引默认缓存大小 (16MB)
	defaultCacheSize = 16 * 1024 * 1024

	// LSM树索引默认写缓冲区大小 (4KB)
	defaultWriteBufferSize = 4 * 1024

	// LSM树索引默认合并策略
	defaultCompactionStrategy = "leveled"

	// LSM树索引最大层级
	maxLSMLevel = 7

	// LSM树索引每层大小倍数
	levelSizeRatio = 10

	// LSM树索引魔数 (用于文件格式验证)
	lsmMagicNumber uint32 = 0x4C534D49 // "LSMI" in ASCII

	// LSM树索引文件格式版本
	lsmFormatVersion = 1

	// LSM树索引块类型
	lsmBlockTypeData   = 0 // 数据块
	lsmBlockTypeIndex  = 1 // 索引块
	lsmBlockTypeFilter = 2 // 过滤器块
	lsmBlockTypeMeta   = 3 // 元数据块

	// LSM树索引压缩类型
	lsmCompressionNone = 0 // 不压缩
	lsmCompressionGzip = 1 // Gzip压缩

	// LSM树索引页脚大小 (固定 48 字节)
	lsmFooterSize = 48
)

// LSMKey 表示LSM树索引键
type LSMKey struct {
	// 键值
	Value []byte
	// 序列号（用于版本控制）
	Sequence uint64
	// 操作类型（0=删除，1=插入）
	OpType byte
}

// Compare 比较两个LSM键
func (k *LSMKey) Compare(other *LSMKey) int {
	// 先比较键值
	cmp := bytes.Compare(k.Value, other.Value)
	if cmp != 0 {
		return cmp
	}
	// 键值相同，比较序列号（降序，较新的版本优先）
	if k.Sequence > other.Sequence {
		return -1
	} else if k.Sequence < other.Sequence {
		return 1
	}
	// 序列号相同，比较操作类型
	return int(other.OpType) - int(k.OpType)
}

// LSMEntry 表示LSM树索引条目
type LSMEntry struct {
	// 键
	Key *LSMKey
	// 时间序列ID
	SeriesID string
}

// LSMMemTable 表示LSM树索引内存表
type LSMMemTable struct {
	// 条目（按键排序）
	entries []*LSMEntry
	// 大小（字节）
	size int64
	// 最大序列号
	maxSequence uint64
	// 互斥锁
	mu sync.RWMutex
}

// NewLSMMemTable 创建新的LSM树索引内存表
func NewLSMMemTable() *LSMMemTable {
	return &LSMMemTable{
		entries:     make([]*LSMEntry, 0, 1024),
		size:        0,
		maxSequence: 0,
	}
}

// Size 返回内存表大小
func (m *LSMMemTable) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

// Count 返回条目数量
func (m *LSMMemTable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// MaxSequence 返回最大序列号
func (m *LSMMemTable) MaxSequence() uint64 {
	return atomic.LoadUint64(&m.maxSequence)
}

// Put 插入条目
func (m *LSMMemTable) Put(key []byte, seriesID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 生成新的序列号
	seq := atomic.AddUint64(&m.maxSequence, 1)

	// 创建新条目
	entry := &LSMEntry{
		Key: &LSMKey{
			Value:    key,
			Sequence: seq,
			OpType:   1, // 插入
		},
		SeriesID: seriesID,
	}

	// 计算条目大小
	entrySize := int64(len(key) + len(seriesID) + 9) // 9 = 序列号(8) + 操作类型(1)

	// 插入条目（保持排序）
	idx := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].Key.Compare(entry.Key) >= 0
	})

	// 检查是否已存在相同键
	if idx < len(m.entries) && bytes.Equal(m.entries[idx].Key.Value, key) {
		// 更新现有条目
		m.entries[idx] = entry
	} else {
		// 插入新条目
		m.entries = append(m.entries, nil)
		copy(m.entries[idx+1:], m.entries[idx:])
		m.entries[idx] = entry
	}

	// 更新大小
	atomic.AddInt64(&m.size, entrySize)

	return nil
}

// Delete 删除条目
func (m *LSMMemTable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 生成新的序列号
	seq := atomic.AddUint64(&m.maxSequence, 1)

	// 创建删除标记条目
	entry := &LSMEntry{
		Key: &LSMKey{
			Value:    key,
			Sequence: seq,
			OpType:   0, // 删除
		},
		SeriesID: "",
	}

	// 计算条目大小
	entrySize := int64(len(key) + 9) // 9 = 序列号(8) + 操作类型(1)

	// 插入条目（保持排序）
	idx := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].Key.Compare(entry.Key) >= 0
	})

	// 插入删除标记
	m.entries = append(m.entries, nil)
	copy(m.entries[idx+1:], m.entries[idx:])
	m.entries[idx] = entry

	// 更新大小
	atomic.AddInt64(&m.size, entrySize)

	return nil
}

// Get 获取条目
func (m *LSMMemTable) Get(key []byte) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 二分查找
	idx := sort.Search(len(m.entries), func(i int) bool {
		return bytes.Compare(m.entries[i].Key.Value, key) >= 0
	})

	// 检查是否找到
	if idx < len(m.entries) && bytes.Equal(m.entries[idx].Key.Value, key) {
		// 找到键，检查操作类型
		if m.entries[idx].Key.OpType == 0 {
			// 删除标记
			return "", false
		}
		// 返回值
		return m.entries[idx].SeriesID, true
	}

	// 未找到
	return "", false
}

// GetRange 范围查询
func (m *LSMMemTable) GetRange(startKey, endKey []byte, includeStart, includeEnd bool) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []string
	seen := make(map[string]struct{})

	// 查找起始位置
	startIdx := 0
	if startKey != nil {
		startIdx = sort.Search(len(m.entries), func(i int) bool {
			cmp := bytes.Compare(m.entries[i].Key.Value, startKey)
			if includeStart {
				return cmp >= 0
			}
			return cmp > 0
		})
	}

	// 遍历范围内的条目
	for i := startIdx; i < len(m.entries); i++ {
		// 检查上限
		if endKey != nil {
			cmp := bytes.Compare(m.entries[i].Key.Value, endKey)
			if includeEnd {
				if cmp > 0 {
					break
				}
			} else {
				if cmp >= 0 {
					break
				}
			}
		}

		// 检查操作类型和重复项
		if m.entries[i].Key.OpType == 1 && m.entries[i].SeriesID != "" {
			if _, exists := seen[m.entries[i].SeriesID]; !exists {
				result = append(result, m.entries[i].SeriesID)
				seen[m.entries[i].SeriesID] = struct{}{}
			}
		}
	}

	return result
}

// Iterator 返回迭代器
func (m *LSMMemTable) Iterator() *LSMMemTableIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 复制条目切片
	entries := make([]*LSMEntry, len(m.entries))
	copy(entries, m.entries)

	return &LSMMemTableIterator{
		entries: entries,
		pos:     -1,
	}
}

// LSMMemTableIterator 表示LSM树索引内存表迭代器
type LSMMemTableIterator struct {
	entries []*LSMEntry
	pos     int
}

// Next 移动到下一个条目
func (it *LSMMemTableIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

// Key 返回当前键
func (it *LSMMemTableIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	return it.entries[it.pos].Key.Value
}

// Value 返回当前值
func (it *LSMMemTableIterator) Value() string {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return ""
	}
	return it.entries[it.pos].SeriesID
}

// Entry 返回当前条目
func (it *LSMMemTableIterator) Entry() *LSMEntry {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	return it.entries[it.pos]
}

// LSMSSTable 表示LSM树索引SSTable
type LSMSSTable struct {
	// 文件路径
	path string
	// 文件句柄
	file *os.File
	// 索引块偏移量
	indexBlockOffset int64
	// 过滤器块偏移量
	filterBlockOffset int64
	// 元数据块偏移量
	metaBlockOffset int64
	// 数据块数量
	dataBlockCount int
	// 最小键
	minKey []byte
	// 最大键
	maxKey []byte
	// 条目数量
	entryCount int
	// 最大序列号
	maxSequence uint64
	// 层级
	level int
	// 创建时间
	createTime int64
	// 文件大小
	fileSize int64
	// 布隆过滤器
	bloomFilter *BloomFilter
	// 是否已关闭
	closed bool
	// 互斥锁
	mu sync.RWMutex
}

// BloomFilter 表示布隆过滤器
type BloomFilter struct {
	// 位数组
	bits []byte
	// 哈希函数数量
	hashCount int
}

// NewBloomFilter 创建新的布隆过滤器
func NewBloomFilter(expectedEntries int, fpRate float64) *BloomFilter {
	// 计算所需位数
	bitsPerEntry := int(math.Ceil(-math.Log(fpRate) / math.Ln2))
	bitCount := expectedEntries * bitsPerEntry
	byteCount := (bitCount + 7) / 8

	// 计算哈希函数数量
	hashCount := int(math.Ceil(math.Ln2 * float64(bitsPerEntry)))

	return &BloomFilter{
		bits:      make([]byte, byteCount),
		hashCount: hashCount,
	}
}

// Add 添加键到过滤器
func (bf *BloomFilter) Add(key []byte) {
	// 计算哈希值
	h := crc32.ChecksumIEEE(key)
	for i := 0; i < bf.hashCount; i++ {
		// 使用双重哈希法生成多个哈希值
		h1 := h
		h2 := h >> 16
		h = h1 + uint32(i)*h2

		// 设置对应位
		pos := h % uint32(len(bf.bits)*8)
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// Contains 检查键是否可能存在
func (bf *BloomFilter) Contains(key []byte) bool {
	// 计算哈希值
	h := crc32.ChecksumIEEE(key)
	for i := 0; i < bf.hashCount; i++ {
		// 使用双重哈希法生成多个哈希值
		h1 := h
		h2 := h >> 16
		h = h1 + uint32(i)*h2

		// 检查对应位
		pos := h % uint32(len(bf.bits)*8)
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode 编码布隆过滤器
func (bf *BloomFilter) Encode() []byte {
	// 创建缓冲区
	buf := bytes.NewBuffer(nil)

	// 写入哈希函数数量
	binary.Write(buf, binary.LittleEndian, uint32(bf.hashCount))

	// 写入位数组
	buf.Write(bf.bits)

	return buf.Bytes()
}

// DecodeBloomFilter 解码布隆过滤器
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid bloom filter data")
	}

	// 读取哈希函数数量
	hashCount := int(binary.LittleEndian.Uint32(data[:4]))

	// 读取位数组
	bits := make([]byte, len(data)-4)
	copy(bits, data[4:])

	return &BloomFilter{
		bits:      bits,
		hashCount: hashCount,
	}, nil
}

// LSMIndex LSM树索引实现
type LSMIndex struct {
	// 索引名称
	name string
	// 索引字段
	fields []string
	// 是否唯一索引
	unique bool
	// 数据库名称
	database string
	// 集合名称
	collection string
	// 索引目录
	directory string
	// 活跃MemTable
	activeMemTable *LSMMemTable
	// 不可变MemTable列表（等待刷盘）
	immutableMemTables []*LSMMemTable
	// SSTable列表（按层级和创建时间排序）
	ssTables [][]*LSMSSTable
	// 当前序列号
	sequence uint64
	// 选项
	options *LSMIndexOptions
	// 统计信息
	stats IndexStats
	// 后台合并任务是否运行中
	compactionRunning bool
	// 是否已关闭
	closed bool
	// 互斥锁
	mu sync.RWMutex
	// 条件变量（用于等待刷盘完成）
	flushCond *sync.Cond
	// WAL（预写日志）
	wal *LSMWal
}

// LSMWal 表示LSM树索引预写日志
type LSMWal struct {
	// 文件路径
	path string
	// 文件句柄
	file *os.File
	// 当前偏移量
	offset int64
	// 互斥锁
	mu sync.Mutex
}

// NewLSMWal 创建新的预写日志
func NewLSMWal(directory, database, collection, indexName string) (*LSMWal, error) {
	// 创建WAL目录
	walDir := filepath.Join(directory, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}

	// WAL文件路径
	walPath := filepath.Join(walDir, fmt.Sprintf("%s_%s_%s.wal", database, collection, indexName))

	// 打开WAL文件
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// 获取当前偏移量
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &LSMWal{
		path:   walPath,
		file:   file,
		offset: offset,
	}, nil
}

// Append 追加日志记录
func (w *LSMWal) Append(opType byte, key []byte, seriesID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 创建缓冲区
	buf := bytes.NewBuffer(nil)

	// 写入操作类型
	buf.WriteByte(opType)

	// 写入键长度和键
	keyLen := uint32(len(key))
	binary.Write(buf, binary.LittleEndian, keyLen)
	buf.Write(key)

	// 写入值长度和值
	valueLen := uint32(len(seriesID))
	binary.Write(buf, binary.LittleEndian, valueLen)
	buf.Write([]byte(seriesID))

	// 计算CRC32校验和
	data := buf.Bytes()
	crc := crc32.ChecksumIEEE(data)

	// 写入记录长度
	recordLen := uint32(len(data) + 4) // 4字节CRC32
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, recordLen)
	if _, err := w.file.Write(lenBuf); err != nil {
		return err
	}

	// 写入CRC32校验和
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	if _, err := w.file.Write(crcBuf); err != nil {
		return err
	}

	// 写入数据
	if _, err := w.file.Write(data); err != nil {
		return err
	}

	// 更新偏移量
	w.offset += int64(4 + recordLen)

	// 同步到磁盘
	return w.file.Sync()
}

// Close 关闭WAL
func (w *LSMWal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// Delete 删除WAL文件
func (w *LSMWal) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		w.file.Close()
		w.file = nil
	}
	return os.Remove(w.path)
}

// Recovery 从WAL恢复数据
func (w *LSMWal) Recovery(memTable *LSMMemTable) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 重置文件指针到开头
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// 读取并应用所有记录
	offset := int64(0)
	for {
		// 读取记录长度
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(w.file, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		recordLen := binary.LittleEndian.Uint32(lenBuf)
		offset += 4

		// 读取CRC32校验和
		crcBuf := make([]byte, 4)
		if _, err := io.ReadFull(w.file, crcBuf); err != nil {
			return err
		}
		expectedCRC := binary.LittleEndian.Uint32(crcBuf)
		offset += 4

		// 读取数据
		data := make([]byte, recordLen-4)
		if _, err := io.ReadFull(w.file, data); err != nil {
			return err
		}
		offset += int64(recordLen - 4)

		// 验证CRC32校验和
		actualCRC := crc32.ChecksumIEEE(data)
		if actualCRC != expectedCRC {
			return errors.New("WAL record corrupted: CRC32 mismatch")
		}

		// 解析记录
		if len(data) < 1 {
			return errors.New("WAL record corrupted: too short")
		}
		opType := data[0]
		data = data[1:]

		// 读取键
		if len(data) < 4 {
			return errors.New("WAL record corrupted: key length missing")
		}
		keyLen := binary.LittleEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < keyLen {
			return errors.New("WAL record corrupted: key data missing")
		}
		key := data[:keyLen]
		data = data[keyLen:]

		// 读取值
		if len(data) < 4 {
			return errors.New("WAL record corrupted: value length missing")
		}
		valueLen := binary.LittleEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < valueLen {
			return errors.New("WAL record corrupted: value data missing")
		}
		value := string(data[:valueLen])

		// 应用操作
		if opType == 0 {
			// 删除操作
			memTable.Delete(key)
		} else {
			// 插入操作
			memTable.Put(key, value)
		}
	}

	// 重置文件指针到末尾
	w.offset = offset
	_, err := w.file.Seek(offset, io.SeekStart)
	return err
}

// NewLSMIndex 创建新的LSM树索引
func NewLSMIndex(name string, fields []string, unique bool, database, collection, directory string, options *LSMIndexOptions) (*LSMIndex, error) {
	// 使用默认选项（如果未提供）
	if options == nil {
		options = &LSMIndexOptions{
			MemTableSizeLimit:       defaultMemTableSizeLimit,
			BloomFilterFPRate:       defaultBloomFilterFPRate,
			EnablePrefixCompression: true,
			CacheSize:               defaultCacheSize,
			CompactionStrategy:      defaultCompactionStrategy,
			WriteBufferSize:         defaultWriteBufferSize,
			EnableWAL:               true,
			EnableSnapshot:          true,
		}
	}

	// 创建索引目录
	indexDir := filepath.Join(directory, database, collection, "indices", name)
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return nil, err
	}

	// 创建LSM索引
	idx := &LSMIndex{
		name:               name,
		fields:             fields,
		unique:             unique,
		database:           database,
		collection:         collection,
		directory:          indexDir,
		activeMemTable:     NewLSMMemTable(),
		immutableMemTables: make([]*LSMMemTable, 0),
		ssTables:           make([][]*LSMSSTable, maxLSMLevel),
		sequence:           0,
		options:            options,
		stats: IndexStats{
			Name:           name,
			Type:           IndexTypeLSM,
			LastUpdateTime: time.Now().UnixNano(),
			LSMStats: &LSMIndexStats{
				MemTableSize:    0,
				SSTableCount:    0,
				LevelSSTables:   make([]int, maxLSMLevel),
				CompactionCount: 0,
			},
		},
		compactionRunning: false,
		closed:            false,
	}

	// 初始化条件变量
	idx.flushCond = sync.NewCond(&idx.mu)

	// 创建WAL
	if options.EnableWAL {
		wal, err := NewLSMWal(directory, database, collection, name)
		if err != nil {
			return nil, err
		}
		idx.wal = wal

		// 从WAL恢复数据
		if err := wal.Recovery(idx.activeMemTable); err != nil {
			wal.Close()
			return nil, err
		}
	}

	// 加载现有SSTable文件
	if err := idx.loadSSTables(); err != nil {
		if idx.wal != nil {
			idx.wal.Close()
		}
		return nil, err
	}

	// 启动后台合并任务
	go idx.backgroundCompaction()

	return idx, nil
}

// loadSSTables 加载现有SSTable文件
func (idx *LSMIndex) loadSSTables() error {
	// 遍历索引目录
	files, err := os.ReadDir(idx.directory)
	if err != nil {
		return err
	}

	// 按层级分组SSTable文件
	for _, file := range files {
		if filepath.Ext(file.Name()) != lsmFileExt {
			continue
		}

		// 解析文件名
		parts := strings.Split(strings.TrimSuffix(file.Name(), lsmFileExt), "_")
		if len(parts) != 5 {
			continue
		}

		// 提取层级
		level, err := strconv.Atoi(parts[3])
		if err != nil || level < 0 || level >= maxLSMLevel {
			continue
		}

		// 打开SSTable文件
		sstPath := filepath.Join(idx.directory, file.Name())
		sstFile, err := os.Open(sstPath)
		if err != nil {
			return err
		}

		// 读取文件大小
		fileInfo, err := sstFile.Stat()
		if err != nil {
			sstFile.Close()
			return err
		}
		fileSize := fileInfo.Size()

		// 读取页脚
		footerBuf := make([]byte, lsmFooterSize)
		if _, err := sstFile.ReadAt(footerBuf, fileSize-lsmFooterSize); err != nil {
			sstFile.Close()
			return err
		}

		// 验证魔数
		magic := binary.LittleEndian.Uint32(footerBuf[44:48])
		if magic != lsmMagicNumber {
			sstFile.Close()
			return errors.New("invalid SSTable file: wrong magic number")
		}

		// 读取块偏移量
		indexBlockOffset := int64(binary.LittleEndian.Uint64(footerBuf[0:8]))
		filterBlockOffset := int64(binary.LittleEndian.Uint64(footerBuf[8:16]))
		metaBlockOffset := int64(binary.LittleEndian.Uint64(footerBuf[16:24]))
		dataBlockCount := int(binary.LittleEndian.Uint32(footerBuf[24:28]))
		entryCount := int(binary.LittleEndian.Uint32(footerBuf[28:32]))
		maxSequence := binary.LittleEndian.Uint64(footerBuf[32:40])
		createTime := int64(binary.LittleEndian.Uint64(footerBuf[40:48]))

		// 读取元数据块
		metaBlockSizeBuf := make([]byte, 4)
		if _, err := sstFile.ReadAt(metaBlockSizeBuf, metaBlockOffset); err != nil {
			sstFile.Close()
			return err
		}
		metaBlockSize := binary.LittleEndian.Uint32(metaBlockSizeBuf)
		metaBlockBuf := make([]byte, metaBlockSize)
		if _, err := sstFile.ReadAt(metaBlockBuf, metaBlockOffset+4); err != nil {
			sstFile.Close()
			return err
		}

		// 解析元数据
		metaReader := bytes.NewReader(metaBlockBuf)
		var minKeyLen, maxKeyLen uint32
		if err := binary.Read(metaReader, binary.LittleEndian, &minKeyLen); err != nil {
			sstFile.Close()
			return err
		}
		minKey := make([]byte, minKeyLen)
		if _, err := io.ReadFull(metaReader, minKey); err != nil {
			sstFile.Close()
			return err
		}
		if err := binary.Read(metaReader, binary.LittleEndian, &maxKeyLen); err != nil {
			sstFile.Close()
			return err
		}
		maxKey := make([]byte, maxKeyLen)
		if _, err := io.ReadFull(metaReader, maxKey); err != nil {
			sstFile.Close()
			return err
		}

		// 读取过滤器块
		filterBlockSizeBuf := make([]byte, 4)
		if _, err := sstFile.ReadAt(filterBlockSizeBuf, filterBlockOffset); err != nil {
			sstFile.Close()
			return err
		}
		filterBlockSize := binary.LittleEndian.Uint32(filterBlockSizeBuf)
		filterBlockBuf := make([]byte, filterBlockSize)
		if _, err := sstFile.ReadAt(filterBlockBuf, filterBlockOffset+4); err != nil {
			sstFile.Close()
			return err
		}

		// 解析布隆过滤器
		bloomFilter, err := DecodeBloomFilter(filterBlockBuf)
		if err != nil {
			sstFile.Close()
			return err
		}

		// 创建SSTable对象
		sst := &LSMSSTable{
			path:              sstPath,
			file:              sstFile,
			indexBlockOffset:  indexBlockOffset,
			filterBlockOffset: filterBlockOffset,
			metaBlockOffset:   metaBlockOffset,
			dataBlockCount:    dataBlockCount,
			minKey:            minKey,
			maxKey:            maxKey,
			entryCount:        entryCount,
			maxSequence:       maxSequence,
			level:             level,
			createTime:        createTime,
			fileSize:          fileSize,
			bloomFilter:       bloomFilter,
			closed:            false,
		}

		// 添加到SSTable列表
		idx.ssTables[level] = append(idx.ssTables[level], sst)

		// 更新统计信息
		idx.stats.LSMStats.SSTableCount++
		idx.stats.LSMStats.LevelSSTables[level]++
		idx.stats.SizeBytes += fileSize
		idx.stats.ItemCount += int64(entryCount)

		// 更新序列号
		if maxSequence > idx.sequence {
			idx.sequence = maxSequence
		}
	}

	// 按创建时间排序每层的SSTable
	for level := 0; level < maxLSMLevel; level++ {
		sort.Slice(idx.ssTables[level], func(i, j int) bool {
			return idx.ssTables[level][i].createTime < idx.ssTables[level][j].createTime
		})
	}

	return nil
}

// Name 返回索引名称
func (idx *LSMIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *LSMIndex) Type() IndexType {
	return IndexTypeLSM
}

// Fields 返回索引字段
func (idx *LSMIndex) Fields() []string {
	return idx.fields
}

// Insert 插入索引项
func (idx *LSMIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
	// 检查是否已关闭
	if idx.closed {
		return errors.New("index is closed")
	}

	// 转换键为字节数组
	keyBytes, err := idx.encodeKey(key)
	if err != nil {
		return err
	}

	// 检查是否需要刷盘
	idx.mu.Lock()
	if idx.activeMemTable.Size() >= idx.options.MemTableSizeLimit {
		// 将活跃MemTable转为不可变MemTable
		idx.immutableMemTables = append(idx.immutableMemTables, idx.activeMemTable)
		idx.activeMemTable = NewLSMMemTable()

		// 触发后台刷盘
		go idx.flushMemTable()
	}
	idx.mu.Unlock()

	// 写入WAL
	if idx.options.EnableWAL && idx.wal != nil {
		if err := idx.wal.Append(1, keyBytes, seriesID); err != nil {
			return err
		}
	}

	// 插入到活跃MemTable
	if err := idx.activeMemTable.Put(keyBytes, seriesID); err != nil {
		return err
	}

	// 更新统计信息
	idx.mu.Lock()
	idx.stats.ItemCount++
	idx.stats.LSMStats.MemTableSize = idx.activeMemTable.Size()
	idx.stats.LastUpdateTime = time.Now().UnixNano()
	idx.mu.Unlock()

	return nil
}

// Remove 删除索引项
func (idx *LSMIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
	// 检查是否已关闭
	if idx.closed {
		return errors.New("index is closed")
	}

	// 转换键为字节数组
	keyBytes, err := idx.encodeKey(key)
	if err != nil {
		return err
	}

	// 检查是否需要刷盘
	idx.mu.Lock()
	if idx.activeMemTable.Size() >= idx.options.MemTableSizeLimit {
		// 将活跃MemTable转为不可变MemTable
		idx.immutableMemTables = append(idx.immutableMemTables, idx.activeMemTable)
		idx.activeMemTable = NewLSMMemTable()

		// 触发后台刷盘
		go idx.flushMemTable()
	}
	idx.mu.Unlock()

	// 写入WAL
	if idx.options.EnableWAL && idx.wal != nil {
		if err := idx.wal.Append(0, keyBytes, ""); err != nil {
			return err
		}
	}

	// 插入删除标记到活跃MemTable
	if err := idx.activeMemTable.Delete(keyBytes); err != nil {
		return err
	}

	// 更新统计信息
	idx.mu.Lock()
	idx.stats.LSMStats.MemTableSize = idx.activeMemTable.Size()
	idx.stats.LastUpdateTime = time.Now().UnixNano()
	idx.mu.Unlock()

	return nil
}

// Update 更新索引项
func (idx *LSMIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧键
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}

	// 再插入新键
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *LSMIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
	// 检查是否已关闭
	if idx.closed {
		return nil, errors.New("index is closed")
	}

	// 只支持等值查询
	if condition.Operator != "=" {
		return nil, fmt.Errorf("unsupported operator: %s", condition.Operator)
	}

	// 转换键为字节数组
	keyBytes, err := idx.encodeKey(condition.Value)
	if err != nil {
		return nil, err
	}

	// 在活跃MemTable中查找
	if seriesID, found := idx.activeMemTable.Get(keyBytes); found {
		if seriesID != "" {
			return []string{seriesID}, nil
		}
		return nil, nil // 找到删除标记
	}

	// 在不可变MemTable中查找
	idx.mu.RLock()
	immutableTables := make([]*LSMMemTable, len(idx.immutableMemTables))
	copy(immutableTables, idx.immutableMemTables)
	idx.mu.RUnlock()

	for i := len(immutableTables) - 1; i >= 0; i-- {
		if seriesID, found := immutableTables[i].Get(keyBytes); found {
			if seriesID != "" {
				return []string{seriesID}, nil
			}
			return nil, nil // 找到删除标记
		}
	}

	// 在SSTable中查找
	idx.mu.RLock()
	sstables := make([][]*LSMSSTable, len(idx.ssTables))
	for i := range idx.ssTables {
		sstables[i] = make([]*LSMSSTable, len(idx.ssTables[i]))
		copy(sstables[i], idx.ssTables[i])
	}
	idx.mu.RUnlock()

	// 从最新到最旧的顺序查找
	for level := 0; level < len(sstables); level++ {
		for i := len(sstables[level]) - 1; i >= 0; i-- {
			sst := sstables[level][i]

			// 检查键范围
			if bytes.Compare(keyBytes, sst.minKey) < 0 || bytes.Compare(keyBytes, sst.maxKey) > 0 {
				continue
			}

			// 检查布隆过滤器
			if !sst.bloomFilter.Contains(keyBytes) {
				continue
			}

			// 在SSTable中查找
			seriesID, found, err := idx.searchSSTable(sst, keyBytes)
			if err != nil {
				return nil, err
			}
			if found {
				if seriesID != "" {
					return []string{seriesID}, nil
				}
				return nil, nil // 找到删除标记
			}
		}
	}

	// 未找到
	return nil, nil
}

// SearchRange 范围搜索
func (idx *LSMIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	// 检查是否已关闭
	if idx.closed {
		return nil, errors.New("index is closed")
	}

	// 转换键为字节数组
	var startKeyBytes, endKeyBytes []byte
	var err error
	if startKey != nil {
		startKeyBytes, err = idx.encodeKey(startKey)
		if err != nil {
			return nil, err
		}
	}
	if endKey != nil {
		endKeyBytes, err = idx.encodeKey(endKey)
		if err != nil {
			return nil, err
		}
	}

	// 结果集（去重）
	resultSet := make(map[string]struct{})

	// 在活跃MemTable中查找
	for _, seriesID := range idx.activeMemTable.GetRange(startKeyBytes, endKeyBytes, includeStart, includeEnd) {
		resultSet[seriesID] = struct{}{}
	}

	// 在不可变MemTable中查找
	idx.mu.RLock()
	immutableTables := make([]*LSMMemTable, len(idx.immutableMemTables))
	copy(immutableTables, idx.immutableMemTables)
	idx.mu.RUnlock()

	for i := len(immutableTables) - 1; i >= 0; i-- {
		for _, seriesID := range immutableTables[i].GetRange(startKeyBytes, endKeyBytes, includeStart, includeEnd) {
			resultSet[seriesID] = struct{}{}
		}
	}

	// 在SSTable中查找
	idx.mu.RLock()
	sstables := make([][]*LSMSSTable, len(idx.ssTables))
	for i := range idx.ssTables {
		sstables[i] = make([]*LSMSSTable, len(idx.ssTables[i]))
		copy(sstables[i], idx.ssTables[i])
	}
	idx.mu.RUnlock()

	// 从最新到最旧的顺序查找
	for level := 0; level < len(sstables); level++ {
		for i := len(sstables[level]) - 1; i >= 0; i-- {
			sst := sstables[level][i]

			// 检查键范围是否有交集
			if startKeyBytes != nil && bytes.Compare(startKeyBytes, sst.maxKey) > 0 {
				continue
			}
			if endKeyBytes != nil && bytes.Compare(endKeyBytes, sst.minKey) < 0 {
				continue
			}

			// 在SSTable中范围查找
			results, err := idx.searchSSTableRange(sst, startKeyBytes, endKeyBytes, includeStart, includeEnd)
			if err != nil {
				return nil, err
			}

			// 合并结果
			for _, seriesID := range results {
				resultSet[seriesID] = struct{}{}
			}
		}
	}

	// 转换为切片
	result := make([]string, 0, len(resultSet))
	for seriesID := range resultSet {
		result = append(result, seriesID)
	}

	return result, nil
}

// Clear 清空索引
func (idx *LSMIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查是否已关闭
	if idx.closed {
		return errors.New("index is closed")
	}

	// 关闭所有SSTable
	for level := range idx.ssTables {
		for _, sst := range idx.ssTables[level] {
			sst.mu.Lock()
			if !sst.closed && sst.file != nil {
				sst.file.Close()
				sst.closed = true
			}
			sst.mu.Unlock()
		}
	}

	// 删除所有SSTable文件
	files, err := os.ReadDir(idx.directory)
	if err != nil {
		return err
	}
	for _, file := range files {
		if filepath.Ext(file.Name()) == lsmFileExt {
			if err := os.Remove(filepath.Join(idx.directory, file.Name())); err != nil {
				return err
			}
		}
	}

	// 重置内存状态
	idx.activeMemTable = NewLSMMemTable()
	idx.immutableMemTables = make([]*LSMMemTable, 0)
	idx.ssTables = make([][]*LSMSSTable, maxLSMLevel)
	idx.sequence = 0

	// 重置WAL
	if idx.options.EnableWAL && idx.wal != nil {
		idx.wal.Close()
		idx.wal.Delete()
		wal, err := NewLSMWal(filepath.Dir(filepath.Dir(idx.directory)), idx.database, idx.collection, idx.name)
		if err != nil {
			return err
		}
		idx.wal = wal
	}

	// 重置统计信息
	idx.stats = IndexStats{
		Name:           idx.name,
		Type:           IndexTypeLSM,
		LastUpdateTime: time.Now().UnixNano(),
		LSMStats: &LSMIndexStats{
			MemTableSize:    0,
			SSTableCount:    0,
			LevelSSTables:   make([]int, maxLSMLevel),
			CompactionCount: 0,
		},
	}

	return nil
}

// Stats 返回索引统计信息
func (idx *LSMIndex) Stats(ctx context.Context) (IndexStats, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查是否已关闭
	if idx.closed {
		return IndexStats{}, errors.New("index is closed")
	}

	// 复制统计信息
	stats := idx.stats
	stats.LSMStats.MemTableSize = idx.activeMemTable.Size()

	return stats, nil
}

// Close 关闭索引
func (idx *LSMIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查是否已关闭
	if idx.closed {
		return nil
	}

	// 标记为已关闭
	idx.closed = true

	// 等待所有不可变MemTable刷盘完成
	for len(idx.immutableMemTables) > 0 {
		idx.flushCond.Wait()
	}

	// 刷盘活跃MemTable
	if idx.activeMemTable.Count() > 0 {
		if err := idx.flushMemTableInternal(idx.activeMemTable); err != nil {
			return err
		}
	}

	// 关闭WAL
	if idx.options.EnableWAL && idx.wal != nil {
		if err := idx.wal.Close(); err != nil {
			return err
		}
	}

	// 关闭所有SSTable
	for level := range idx.ssTables {
		for _, sst := range idx.ssTables[level] {
			sst.mu.Lock()
			if !sst.closed && sst.file != nil {
				sst.file.Close()
				sst.closed = true
			}
			sst.mu.Unlock()
		}
	}

	return nil
}

// Snapshot 创建索引快照
func (idx *LSMIndex) Snapshot() (IndexSnapshot, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查是否已关闭
	if idx.closed {
		return nil, errors.New("index is closed")
	}

	// 检查是否支持快照
	if !idx.options.EnableSnapshot {
		return nil, errors.New("snapshot not enabled")
	}

	// 创建快照
	snapshot := &LSMIndexSnapshot{
		index:              idx,
		sequence:           atomic.LoadUint64(&idx.sequence),
		activeMemTable:     idx.activeMemTable,
		immutableMemTables: make([]*LSMMemTable, len(idx.immutableMemTables)),
		ssTables:           make([][]*LSMSSTable, len(idx.ssTables)),
		closed:             false,
	}

	// 复制不可变MemTable
	copy(snapshot.immutableMemTables, idx.immutableMemTables)

	// 复制SSTable
	for i := range idx.ssTables {
		snapshot.ssTables[i] = make([]*LSMSSTable, len(idx.ssTables[i]))
		copy(snapshot.ssTables[i], idx.ssTables[i])
	}

	return snapshot, nil
}

// LSMIndexSnapshot 表示LSM树索引快照
type LSMIndexSnapshot struct {
	// 索引引用
	index *LSMIndex
	// 序列号
	sequence uint64
	// 活跃MemTable
	activeMemTable *LSMMemTable
	// 不可变MemTable列表
	immutableMemTables []*LSMMemTable
	// SSTable列表
	ssTables [][]*LSMSSTable
	// 是否已关闭
	closed bool
	// 互斥锁
	mu sync.RWMutex
}

// Search 在快照中搜索
func (s *LSMIndexSnapshot) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查是否已关闭
	if s.closed {
		return nil, errors.New("snapshot is closed")
	}

	// 只支持等值查询
	if condition.Operator != "=" {
		return nil, fmt.Errorf("unsupported operator: %s", condition.Operator)
	}

	// 转换键为字节数组
	keyBytes, err := s.index.encodeKey(condition.Value)
	if err != nil {
		return nil, err
	}

	// 在活跃MemTable中查找
	if seriesID, found := s.activeMemTable.Get(keyBytes); found {
		if seriesID != "" {
			return []string{seriesID}, nil
		}
		return nil, nil // 找到删除标记
	}

	// 在不可变MemTable中查找
	for i := len(s.immutableMemTables) - 1; i >= 0; i-- {
		if seriesID, found := s.immutableMemTables[i].Get(keyBytes); found {
			if seriesID != "" {
				return []string{seriesID}, nil
			}
			return nil, nil // 找到删除标记
		}
	}

	// 在SSTable中查找
	for level := 0; level < len(s.ssTables); level++ {
		for i := len(s.ssTables[level]) - 1; i >= 0; i-- {
			sst := s.ssTables[level][i]

			// 检查键范围
			if bytes.Compare(keyBytes, sst.minKey) < 0 || bytes.Compare(keyBytes, sst.maxKey) > 0 {
				continue
			}

			// 检查布隆过滤器
			if !sst.bloomFilter.Contains(keyBytes) {
				continue
			}

			// 在SSTable中查找
			seriesID, found, err := s.index.searchSSTable(sst, keyBytes)
			if err != nil {
				return nil, err
			}
			if found {
				if seriesID != "" {
					return []string{seriesID}, nil
				}
				return nil, nil // 找到删除标记
			}
		}
	}

	// 未找到
	return nil, nil
}

// SearchRange 在快照中范围搜索
func (s *LSMIndexSnapshot) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查是否已关闭
	if s.closed {
		return nil, errors.New("snapshot is closed")
	}

	// 转换键为字节数组
	var startKeyBytes, endKeyBytes []byte
	var err error
	if startKey != nil {
		startKeyBytes, err = s.index.encodeKey(startKey)
		if err != nil {
			return nil, err
		}
	}
	if endKey != nil {
		endKeyBytes, err = s.index.encodeKey(endKey)
		if err != nil {
			return nil, err
		}
	}

	// 结果集（去重）
	resultSet := make(map[string]struct{})

	// 在活跃MemTable中查找
	for _, seriesID := range s.activeMemTable.GetRange(startKeyBytes, endKeyBytes, includeStart, includeEnd) {
		resultSet[seriesID] = struct{}{}
	}

	// 在不可变MemTable中查找
	for i := len(s.immutableMemTables) - 1; i >= 0; i-- {
		for _, seriesID := range s.immutableMemTables[i].GetRange(startKeyBytes, endKeyBytes, includeStart, includeEnd) {
			resultSet[seriesID] = struct{}{}
		}
	}

	// 在SSTable中查找
	for level := 0; level < len(s.ssTables); level++ {
		for i := len(s.ssTables[level]) - 1; i >= 0; i-- {
			sst := s.ssTables[level][i]

			// 检查键范围是否有交集
			if startKeyBytes != nil && bytes.Compare(startKeyBytes, sst.maxKey) > 0 {
				continue
			}
			if endKeyBytes != nil && bytes.Compare(endKeyBytes, sst.minKey) < 0 {
				continue
			}

			// 在SSTable中范围查找
			results, err := s.index.searchSSTableRange(sst, startKeyBytes, endKeyBytes, includeStart, includeEnd)
			if err != nil {
				return nil, err
			}

			// 合并结果
			for _, seriesID := range results {
				resultSet[seriesID] = struct{}{}
			}
		}
	}

	// 转换为切片
	result := make([]string, 0, len(resultSet))
	for seriesID := range resultSet {
		result = append(result, seriesID)
	}

	return result, nil
}

// Release 释放快照资源
func (s *LSMIndexSnapshot) Release() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查是否已关闭
	if s.closed {
		return nil
	}

	// 标记为已关闭
	s.closed = true

	// 清空引用
	s.activeMemTable = nil
	s.immutableMemTables = nil
	s.ssTables = nil

	return nil
}

// Flush 将内存中的数据刷新到磁盘
func (idx *LSMIndex) Flush(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查是否已关闭
	if idx.closed {
		return errors.New("index is closed")
	}

	// 将活跃MemTable转为不可变MemTable
	if idx.activeMemTable.Count() > 0 {
		idx.immutableMemTables = append(idx.immutableMemTables, idx.activeMemTable)
		idx.activeMemTable = NewLSMMemTable()
	}

	// 刷盘所有不可变MemTable
	for len(idx.immutableMemTables) > 0 {
		memTable := idx.immutableMemTables[0]
		idx.immutableMemTables = idx.immutableMemTables[1:]

		if err := idx.flushMemTableInternal(memTable); err != nil {
			return err
		}

		// 通知等待的goroutine
		idx.flushCond.Broadcast()
	}

	return nil
}

// flushMemTable 刷盘MemTable
func (idx *LSMIndex) flushMemTable() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查是否有不可变MemTable
	if len(idx.immutableMemTables) == 0 {
		return
	}

	// 获取第一个不可变MemTable
	memTable := idx.immutableMemTables[0]
	idx.immutableMemTables = idx.immutableMemTables[1:]

	// 刷盘
	if err := idx.flushMemTableInternal(memTable); err != nil {
		// 记录错误日志
		fmt.Printf("Error flushing memtable: %v\n", err)
	}

	// 通知等待的goroutine
	idx.flushCond.Broadcast()

	// 检查是否需要触发合并
	if idx.shouldCompact() {
		go idx.Compact(context.Background())
	}
}

// flushMemTableInternal 内部刷盘MemTable实现
func (idx *LSMIndex) flushMemTableInternal(memTable *LSMMemTable) error {
	// 检查是否为空
	if memTable.Count() == 0 {
		return nil
	}

	// 创建SSTable文件
	sstPath := filepath.Join(idx.directory, fmt.Sprintf(lsmFileFormat, idx.database, idx.collection, idx.name, 0, time.Now().UnixNano(), lsmFileExt))
	sstFile, err := os.Create(sstPath)
	if err != nil {
		return err
	}
	defer sstFile.Close()

	// 创建布隆过滤器
	bloomFilter := NewBloomFilter(memTable.Count(), idx.options.BloomFilterFPRate)

	// 获取迭代器
	it := memTable.Iterator()

	// 写入数据块
	dataBlockOffsets := make([]int64, 0)
	dataBlockKeys := make([][]byte, 0)
	entryCount := 0
	var minKey, maxKey []byte

	// 创建数据块缓冲区
	dataBlockBuf := bytes.NewBuffer(nil)
	for it.Next() {
		entry := it.Entry()

		// 更新最小/最大键
		if minKey == nil || bytes.Compare(entry.Key.Value, minKey) < 0 {
			minKey = entry.Key.Value
		}
		if maxKey == nil || bytes.Compare(entry.Key.Value, maxKey) > 0 {
			maxKey = entry.Key.Value
		}

		// 添加到布隆过滤器
		bloomFilter.Add(entry.Key.Value)

		// 写入键长度和键
		binary.Write(dataBlockBuf, binary.LittleEndian, uint32(len(entry.Key.Value)))
		dataBlockBuf.Write(entry.Key.Value)

		// 写入序列号和操作类型
		binary.Write(dataBlockBuf, binary.LittleEndian, entry.Key.Sequence)
		dataBlockBuf.WriteByte(entry.Key.OpType)

		// 写入值长度和值
		binary.Write(dataBlockBuf, binary.LittleEndian, uint32(len(entry.SeriesID)))
		dataBlockBuf.Write([]byte(entry.SeriesID))

		// 记录块中的第一个键
		if len(dataBlockKeys) == 0 || len(dataBlockOffsets) != len(dataBlockKeys) {
			dataBlockKeys = append(dataBlockKeys, entry.Key.Value)
		}

		entryCount++

		// 检查数据块是否已满
		if dataBlockBuf.Len() >= 4096 {
			// 记录数据块偏移量
			offset, err := sstFile.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
			dataBlockOffsets = append(dataBlockOffsets, offset)

			// 写入数据块大小
			binary.Write(sstFile, binary.LittleEndian, uint32(dataBlockBuf.Len()))

			// 写入数据块
			if _, err := dataBlockBuf.WriteTo(sstFile); err != nil {
				return err
			}

			// 重置数据块缓冲区
			dataBlockBuf.Reset()
		}
	}

	// 写入最后一个数据块（如果有）
	if dataBlockBuf.Len() > 0 {
		// 记录数据块偏移量
		offset, err := sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		dataBlockOffsets = append(dataBlockOffsets, offset)

		// 写入数据块大小
		binary.Write(sstFile, binary.LittleEndian, uint32(dataBlockBuf.Len()))

		// 写入数据块
		if _, err := dataBlockBuf.WriteTo(sstFile); err != nil {
			return err
		}
	}

	// 写入索引块
	indexBlockOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 创建索引块缓冲区
	indexBlockBuf := bytes.NewBuffer(nil)

	// 写入数据块数量
	binary.Write(indexBlockBuf, binary.LittleEndian, uint32(len(dataBlockOffsets)))

	// 写入数据块索引
	for i, offset := range dataBlockOffsets {
		// 写入键长度和键
		binary.Write(indexBlockBuf, binary.LittleEndian, uint32(len(dataBlockKeys[i])))
		indexBlockBuf.Write(dataBlockKeys[i])

		// 写入偏移量
		binary.Write(indexBlockBuf, binary.LittleEndian, uint64(offset))
	}

	// 写入索引块大小
	binary.Write(sstFile, binary.LittleEndian, uint32(indexBlockBuf.Len()))

	// 写入索引块
	if _, err := indexBlockBuf.WriteTo(sstFile); err != nil {
		return err
	}

	// 写入过滤器块
	filterBlockOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 编码布隆过滤器
	filterData := bloomFilter.Encode()

	// 写入过滤器块大小
	binary.Write(sstFile, binary.LittleEndian, uint32(len(filterData)))

	// 写入过滤器块
	if _, err := sstFile.Write(filterData); err != nil {
		return err
	}

	// 写入元数据块
	metaBlockOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// 创建元数据块缓冲区
	metaBlockBuf := bytes.NewBuffer(nil)

	// 写入最小键
	binary.Write(metaBlockBuf, binary.LittleEndian, uint32(len(minKey)))
	metaBlockBuf.Write(minKey)

	// 写入最大键
	binary.Write(metaBlockBuf, binary.LittleEndian, uint32(len(maxKey)))
	metaBlockBuf.Write(maxKey)

	// 写入元数据块大小
	binary.Write(sstFile, binary.LittleEndian, uint32(metaBlockBuf.Len()))

	// 写入元数据块
	if _, err := metaBlockBuf.WriteTo(sstFile); err != nil {
		return err
	}

	// 写入页脚
	footerBuf := bytes.NewBuffer(nil)

	// 写入块偏移量
	binary.Write(footerBuf, binary.LittleEndian, uint64(indexBlockOffset))
	binary.Write(footerBuf, binary.LittleEndian, uint64(filterBlockOffset))
	binary.Write(footerBuf, binary.LittleEndian, uint64(metaBlockOffset))

	// 写入数据块数量
	binary.Write(footerBuf, binary.LittleEndian, uint32(len(dataBlockOffsets)))

	// 写入条目数量
	binary.Write(footerBuf, binary.LittleEndian, uint32(entryCount))

	// 写入最大序列号
	binary.Write(footerBuf, binary.LittleEndian, memTable.MaxSequence())

	// 写入创建时间
	binary.Write(footerBuf, binary.LittleEndian, uint64(time.Now().UnixNano()))

	// 写入魔数
	binary.Write(footerBuf, binary.LittleEndian, lsmMagicNumber)

	// 写入页脚
	if _, err := footerBuf.WriteTo(sstFile); err != nil {
		return err
	}

	// 同步到磁盘
	if err := sstFile.Sync(); err != nil {
		return err
	}

	// 获取文件大小
	fileInfo, err := sstFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// 创建SSTable对象
	sst := &LSMSSTable{
		path:              sstPath,
		file:              nil, // 稍后再打开
		indexBlockOffset:  indexBlockOffset,
		filterBlockOffset: filterBlockOffset,
		metaBlockOffset:   metaBlockOffset,
		dataBlockCount:    len(dataBlockOffsets),
		minKey:            minKey,
		maxKey:            maxKey,
		entryCount:        entryCount,
		maxSequence:       memTable.MaxSequence(),
		level:             0,
		createTime:        time.Now().UnixNano(),
		fileSize:          fileSize,
		bloomFilter:       bloomFilter,
		closed:            false,
	}

	// 添加到SSTable列表
	idx.ssTables[0] = append(idx.ssTables[0], sst)

	// 更新统计信息
	idx.stats.LSMStats.SSTableCount++
	idx.stats.LSMStats.LevelSSTables[0]++
	idx.stats.SizeBytes += fileSize
	idx.stats.ItemCount += int64(entryCount)
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// shouldCompact 检查是否需要触发合并
func (idx *LSMIndex) shouldCompact() bool {
	// 检查每一层是否需要压缩
	for level := 0; level < maxLSMLevel-1; level++ {
		// 计算当前层的大小
		var levelSize int64
		for _, sst := range idx.ssTables[level] {
			levelSize += sst.fileSize
		}

		// 计算下一层的大小限制
		nextLevelLimit := int64(levelSizeRatio) * levelSize

		// 计算下一层的实际大小
		var nextLevelSize int64
		for _, sst := range idx.ssTables[level+1] {
			nextLevelSize += sst.fileSize
		}

		// 如果下一层的大小超过限制，需要压缩
		if nextLevelSize > nextLevelLimit {
			return true
		}

		// 如果当前层的文件数量过多，需要压缩
		if len(idx.ssTables[level]) > 4 {
			return true
		}
	}

	return false
}

// Compact 手动触发压缩
func (idx *LSMIndex) Compact(ctx context.Context) error {
	idx.mu.Lock()

	// 检查是否已关闭
	if idx.closed {
		idx.mu.Unlock()
		return errors.New("index is closed")
	}

	// 检查是否已经在运行压缩任务
	if idx.compactionRunning {
		idx.mu.Unlock()
		return nil
	}

	// 标记为正在运行压缩任务
	idx.compactionRunning = true
	idx.mu.Unlock()

	// 确保在函数返回时重置标记
	defer func() {
		idx.mu.Lock()
		idx.compactionRunning = false
		idx.mu.Unlock()
	}()

	// 执行压缩
	return idx.doCompaction()
}

// doCompaction 执行压缩
func (idx *LSMIndex) doCompaction() error {
	// 从第0层开始压缩
	for level := 0; level < maxLSMLevel-1; level++ {
		// 检查当前层是否需要压缩
		idx.mu.Lock()
		if len(idx.ssTables[level]) <= 1 && level > 0 {
			idx.mu.Unlock()
			continue
		}
		idx.mu.Unlock()

		// 执行当前层的压缩
		if err := idx.compactLevel(level); err != nil {
			return err
		}
	}

	return nil
}

// compactLevel 压缩指定层级
func (idx *LSMIndex) compactLevel(level int) error {
	idx.mu.Lock()

	// 检查是否有文件需要压缩
	if len(idx.ssTables[level]) == 0 {
		idx.mu.Unlock()
		return nil
	}

	// 选择要压缩的文件
	var compactSSTables []*LSMSSTable
	if level == 0 {
		// 第0层：选择所有文件
		compactSSTables = make([]*LSMSSTable, len(idx.ssTables[level]))
		copy(compactSSTables, idx.ssTables[level])
		idx.ssTables[level] = nil
	} else {
		// 其他层：选择最旧的文件
		compactSSTables = []*LSMSSTable{idx.ssTables[level][0]}
		idx.ssTables[level] = idx.ssTables[level][1:]
	}

	// 查找下一层中与选定文件有重叠的文件
	var nextLevelSSTables []*LSMSSTable
	var remainingNextLevel []*LSMSSTable

	for _, sst := range idx.ssTables[level+1] {
		overlap := false
		for _, compactSST := range compactSSTables {
			// 检查键范围是否有重叠
			if bytes.Compare(compactSST.maxKey, sst.minKey) >= 0 && bytes.Compare(compactSST.minKey, sst.maxKey) <= 0 {
				overlap = true
				break
			}
		}

		if overlap {
			nextLevelSSTables = append(nextLevelSSTables, sst)
		} else {
			remainingNextLevel = append(remainingNextLevel, sst)
		}
	}

	// 更新下一层的SSTable列表
	idx.ssTables[level+1] = remainingNextLevel

	// 解锁，以便在合并过程中不阻塞其他操作
	idx.mu.Unlock()

	// 合并选定的文件
	newSST, err := idx.mergeSSTable(append(compactSSTables, nextLevelSSTables...), level+1)
	if err != nil {
		// 合并失败，恢复原始状态
		idx.mu.Lock()
		idx.ssTables[level] = append(idx.ssTables[level], compactSSTables...)
		idx.ssTables[level+1] = append(idx.ssTables[level+1], nextLevelSSTables...)
		idx.mu.Unlock()
		return err
	}

	// 更新统计信息
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 添加新的SSTable
	idx.ssTables[level+1] = append(idx.ssTables[level+1], newSST)

	// 更新统计信息
	idx.stats.LSMStats.CompactionCount++
	idx.stats.LSMStats.LastCompactionTime = time.Now().UnixNano()
	idx.stats.LSMStats.SSTableCount = 0
	idx.stats.LSMStats.LevelSSTables = make([]int, maxLSMLevel)
	idx.stats.SizeBytes = 0

	// 重新计算统计信息
	for l := 0; l < maxLSMLevel; l++ {
		idx.stats.LSMStats.LevelSSTables[l] = len(idx.ssTables[l])
		idx.stats.LSMStats.SSTableCount += len(idx.ssTables[l])
		for _, sst := range idx.ssTables[l] {
			idx.stats.SizeBytes += sst.fileSize
		}
	}

	// 删除旧的SSTable文件
	for _, sst := range compactSSTables {
		sst.mu.Lock()
		if !sst.closed && sst.file != nil {
			sst.file.Close()
			sst.closed = true
		}
		sst.mu.Unlock()
		os.Remove(sst.path)
	}
	for _, sst := range nextLevelSSTables {
		sst.mu.Lock()
		if !sst.closed && sst.file != nil {
			sst.file.Close()
			sst.closed = true
		}
		sst.mu.Unlock()
		os.Remove(sst.path)
	}

	return nil
}

// mergeSSTable 合并多个SSTable
func (idx *LSMIndex) mergeSSTable(ssTables []*LSMSSTable, level int) (*LSMSSTable, error) {
	// 创建合并迭代器
	iterators := make([]*SSTableIterator, 0, len(ssTables))
	for _, sst := range ssTables {
		it, err := idx.newSSTableIterator(sst)
		if err != nil {
			// 关闭已创建的迭代器
			for _, iterator := range iterators {
				iterator.Close()
			}
			return nil, err
		}
		iterators = append(iterators, it)
	}

	// 创建合并迭代器
	mergeIt := NewMergeIterator(iterators)

	// 创建新的SSTable文件
	sstPath := filepath.Join(idx.directory, fmt.Sprintf(lsmFileFormat, idx.database, idx.collection, idx.name, level, time.Now().UnixNano(), lsmFileExt))
	sstFile, err := os.Create(sstPath)
	if err != nil {
		mergeIt.Close()
		return nil, err
	}
	defer sstFile.Close()

	// 创建布隆过滤器
	bloomFilter := NewBloomFilter(1000000, idx.options.BloomFilterFPRate) // 估计条目数量

	// 写入数据块
	dataBlockOffsets := make([]int64, 0)
	dataBlockKeys := make([][]byte, 0)
	entryCount := 0
	var minKey, maxKey []byte
	var maxSequence uint64

	// 创建数据块缓冲区
	dataBlockBuf := bytes.NewBuffer(nil)
	var lastKey []byte

	// 遍历合并迭代器
	for mergeIt.Next() {
		key := mergeIt.Key()
		value := mergeIt.Value()
		sequence := mergeIt.Sequence()
		opType := mergeIt.OpType()

		// 跳过重复键（保留最新版本）
		if lastKey != nil && bytes.Equal(key, lastKey) {
			continue
		}
		lastKey = key

		// 更新最小/最大键
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}

		// 更新最大序列号
		if sequence > maxSequence {
			maxSequence = sequence
		}

		// 添加到布隆过滤器
		bloomFilter.Add(key)

		// 写入键长度和键
		binary.Write(dataBlockBuf, binary.LittleEndian, uint32(len(key)))
		dataBlockBuf.Write(key)

		// 写入序列号和操作类型
		binary.Write(dataBlockBuf, binary.LittleEndian, sequence)
		dataBlockBuf.WriteByte(opType)

		// 写入值长度和值
		binary.Write(dataBlockBuf, binary.LittleEndian, uint32(len(value)))
		dataBlockBuf.Write([]byte(value))

		// 记录块中的第一个键
		if len(dataBlockKeys) == 0 || len(dataBlockOffsets) != len(dataBlockKeys) {
			dataBlockKeys = append(dataBlockKeys, key)
		}

		entryCount++

		// 检查数据块是否已满
		if dataBlockBuf.Len() >= 4096 {
			// 记录数据块偏移量
			offset, err := sstFile.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			dataBlockOffsets = append(dataBlockOffsets, offset)

			// 写入数据块大小
			binary.Write(sstFile, binary.LittleEndian, uint32(dataBlockBuf.Len()))

			// 写入数据块
			if _, err := dataBlockBuf.WriteTo(sstFile); err != nil {
				mergeIt.Close()
				return nil, err
			}

			// 重置数据块缓冲区
			dataBlockBuf.Reset()
		}
	}

	// 关闭合并迭代器
	mergeIt.Close()

	// 写入最后一个数据块（如果有）
	if dataBlockBuf.Len() > 0 {
		// 记录数据块偏移量
		offset, err := sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		dataBlockOffsets = append(dataBlockOffsets, offset)

		// 写入数据块大小
		binary.Write(sstFile, binary.LittleEndian, uint32(dataBlockBuf.Len()))

		// 写入数据块
		if _, err := dataBlockBuf.WriteTo(sstFile); err != nil {
			return nil, err
		}
	}

	// 写入索引块
	indexBlockOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// 创建索引块缓冲区
	indexBlockBuf := bytes.NewBuffer(nil)

	// 写入数据块数量
	binary.Write(indexBlockBuf, binary.LittleEndian, uint32(len(dataBlockOffsets)))

	// 写入数据块索引
	for i, offset := range dataBlockOffsets {
		// 写入键长度和键
		binary.Write(indexBlockBuf, binary.LittleEndian, uint32(len(dataBlockKeys[i])))
		indexBlockBuf.Write(dataBlockKeys[i])

		// 写入偏移量
		binary.Write(indexBlockBuf, binary.LittleEndian, uint64(offset))
	}

	// 写入索引块大小
	binary.Write(sstFile, binary.LittleEndian, uint32(indexBlockBuf.Len()))

	// 写入索引块
	if _, err := indexBlockBuf.WriteTo(sstFile); err != nil {
		return nil, err
	}

	// 写入过滤器块
	filterBlockOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// 编码布隆过滤器
	filterData := bloomFilter.Encode()

	// 写入过滤器块大小
	binary.Write(sstFile, binary.LittleEndian, uint32(len(filterData)))

	// 写入过滤器块
	if _, err := sstFile.Write(filterData); err != nil {
		return nil, err
	}

	// 写入元数据块
	metaBlockOffset, err := sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// 创建元数据块缓冲区
	metaBlockBuf := bytes.NewBuffer(nil)

	// 写入最小键
	binary.Write(metaBlockBuf, binary.LittleEndian, uint32(len(minKey)))
	metaBlockBuf.Write(minKey)

	// 写入最大键
	binary.Write(metaBlockBuf, binary.LittleEndian, uint32(len(maxKey)))
	metaBlockBuf.Write(maxKey)

	// 写入元数据块大小
	binary.Write(sstFile, binary.LittleEndian, uint32(metaBlockBuf.Len()))

	// 写入元数据块
	if _, err := metaBlockBuf.WriteTo(sstFile); err != nil {
		return nil, err
	}

	// 写入页脚
	footerBuf := bytes.NewBuffer(nil)

	// 写入块偏移量
	binary.Write(footerBuf, binary.LittleEndian, uint64(indexBlockOffset))
	binary.Write(footerBuf, binary.LittleEndian, uint64(filterBlockOffset))
	binary.Write(footerBuf, binary.LittleEndian, uint64(metaBlockOffset))

	// 写入数据块数量
	binary.Write(footerBuf, binary.LittleEndian, uint32(len(dataBlockOffsets)))

	// 写入条目数量
	binary.Write(footerBuf, binary.LittleEndian, uint32(entryCount))

	// 写入最大序列号
	binary.Write(footerBuf, binary.LittleEndian, maxSequence)

	// 写入创建时间
	binary.Write(footerBuf, binary.LittleEndian, uint64(time.Now().UnixNano()))

	// 写入魔数
	binary.Write(footerBuf, binary.LittleEndian, lsmMagicNumber)

	// 写入页脚
	if _, err := footerBuf.WriteTo(sstFile); err != nil {
		return nil, err
	}

	// 同步到磁盘
	if err := sstFile.Sync(); err != nil {
		return nil, err
	}

	// 获取文件大小
	fileInfo, err := sstFile.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// 创建SSTable对象
	sst := &LSMSSTable{
		path:              sstPath,
		file:              nil, // 稍后再打开
		indexBlockOffset:  indexBlockOffset,
		filterBlockOffset: filterBlockOffset,
		metaBlockOffset:   metaBlockOffset,
		dataBlockCount:    len(dataBlockOffsets),
		minKey:            minKey,
		maxKey:            maxKey,
		entryCount:        entryCount,
		maxSequence:       maxSequence,
		level:             level,
		createTime:        time.Now().UnixNano(),
		fileSize:          fileSize,
		bloomFilter:       bloomFilter,
		closed:            false,
	}

	return sst, nil
}

// backgroundCompaction 后台压缩任务
func (idx *LSMIndex) backgroundCompaction() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 检查索引是否已关闭
			idx.mu.RLock()
			if idx.closed {
				idx.mu.RUnlock()
				return
			}
			idx.mu.RUnlock()

			// 检查是否需要压缩
			if idx.shouldCompact() {
				idx.Compact(context.Background())
			}
		}
	}
}

// searchSSTable 在SSTable中搜索键
func (idx *LSMIndex) searchSSTable(sst *LSMSSTable, key []byte) (string, bool, error) {
	// 打开文件（如果尚未打开）
	sst.mu.Lock()
	if sst.closed || sst.file == nil {
		file, err := os.Open(sst.path)
		if err != nil {
			sst.mu.Unlock()
			return "", false, err
		}
		sst.file = file
		sst.closed = false
	}
	sst.mu.Unlock()

	// 读取索引块
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	// 检查是否已关闭
	if sst.closed || sst.file == nil {
		return "", false, errors.New("SSTable is closed")
	}

	// 读取索引块大小
	indexBlockSizeBuf := make([]byte, 4)
	if _, err := sst.file.ReadAt(indexBlockSizeBuf, sst.indexBlockOffset); err != nil {
		return "", false, err
	}
	indexBlockSize := binary.LittleEndian.Uint32(indexBlockSizeBuf)

	// 读取索引块
	indexBlockBuf := make([]byte, indexBlockSize)
	if _, err := sst.file.ReadAt(indexBlockBuf, sst.indexBlockOffset+4); err != nil {
		return "", false, err
	}

	// 解析索引块
	indexReader := bytes.NewReader(indexBlockBuf)
	var blockCount uint32
	if err := binary.Read(indexReader, binary.LittleEndian, &blockCount); err != nil {
		return "", false, err
	}

	// 查找包含键的数据块
	var dataBlockOffset int64 = -1
	for i := uint32(0); i < blockCount; i++ {
		// 读取键长度
		var keyLen uint32
		if err := binary.Read(indexReader, binary.LittleEndian, &keyLen); err != nil {
			return "", false, err
		}

		// 读取键
		blockKey := make([]byte, keyLen)
		if _, err := io.ReadFull(indexReader, blockKey); err != nil {
			return "", false, err
		}

		// 读取偏移量
		var offset uint64
		if err := binary.Read(indexReader, binary.LittleEndian, &offset); err != nil {
			return "", false, err
		}

		// 检查键是否在当前块中
		if bytes.Compare(key, blockKey) >= 0 {
			dataBlockOffset = int64(offset)
			break
		}
	}

	// 如果未找到包含键的数据块
	if dataBlockOffset == -1 {
		return "", false, nil
	}

	// 读取数据块大小
	dataBlockSizeBuf := make([]byte, 4)
	if _, err := sst.file.ReadAt(dataBlockSizeBuf, dataBlockOffset); err != nil {
		return "", false, err
	}
	dataBlockSize := binary.LittleEndian.Uint32(dataBlockSizeBuf)

	// 读取数据块
	dataBlockBuf := make([]byte, dataBlockSize)
	if _, err := sst.file.ReadAt(dataBlockBuf, dataBlockOffset+4); err != nil {
		return "", false, err
	}

	// 解析数据块
	dataReader := bytes.NewReader(dataBlockBuf)
	for dataReader.Len() > 0 {
		// 读取键长度
		var keyLen uint32
		if err := binary.Read(dataReader, binary.LittleEndian, &keyLen); err != nil {
			return "", false, err
		}

		// 读取键
		entryKey := make([]byte, keyLen)
		if _, err := io.ReadFull(dataReader, entryKey); err != nil {
			return "", false, err
		}

		// 读取序列号和操作类型
		var sequence uint64
		if err := binary.Read(dataReader, binary.LittleEndian, &sequence); err != nil {
			return "", false, err
		}
		var opType byte
		if err := binary.Read(dataReader, binary.LittleEndian, &opType); err != nil {
			return "", false, err
		}

		// 读取值
		var valueLen uint32
		if err := binary.Read(dataReader, binary.LittleEndian, &valueLen); err != nil {
			return "", false, err
		}

		// 读取值
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(dataReader, value); err != nil {
			return "", false, err
		}

		// 检查键是否匹配
		if bytes.Equal(entryKey, key) {
			// 检查操作类型
			if opType == 0 {
				// 删除标记
				return "", true, nil
			}

			// 返回值
			return string(value), true, nil
		}
	}

	return "", false, nil
}

// encodeKey 将键转换为字节数组
func (idx *LSMIndex) encodeKey(key interface{}) ([]byte, error) {
	switch v := key.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case float64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v))
		return buf, nil
	case bool:
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case nil:
		return []byte{}, nil
	default:
		// 尝试JSON序列化
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("unsupported key type: %T", key)
		}
		return data, nil
	}
}

// SSTableIterator 表示SSTable迭代器
type SSTableIterator struct {
	sst         *LSMSSTable
	dataBlock   []byte
	pos         int
	currentKey  []byte
	currentVal  string
	currentSeq  uint64
	currentType byte
	closed      bool
}

// newSSTableIterator 创建新的SSTable迭代器
func (idx *LSMIndex) newSSTableIterator(sst *LSMSSTable) (*SSTableIterator, error) {
	// 打开文件（如果尚未打开）
	sst.mu.Lock()
	if sst.closed || sst.file == nil {
		file, err := os.Open(sst.path)
		if err != nil {
			sst.mu.Unlock()
			return nil, err
		}
		sst.file = file
		sst.closed = false
	}
	sst.mu.Unlock()

	// 读取索引块
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	// 检查是否已关闭
	if sst.closed || sst.file == nil {
		return nil, errors.New("SSTable is closed")
	}

	// 读取索引块大小
	indexBlockSizeBuf := make([]byte, 4)
	if _, err := sst.file.ReadAt(indexBlockSizeBuf, sst.indexBlockOffset); err != nil {
		return nil, err
	}
	indexBlockSize := binary.LittleEndian.Uint32(indexBlockSizeBuf)

	// 读取索引块
	indexBlockBuf := make([]byte, indexBlockSize)
	if _, err := sst.file.ReadAt(indexBlockBuf, sst.indexBlockOffset+4); err != nil {
		return nil, err
	}

	// 解析索引块
	indexReader := bytes.NewReader(indexBlockBuf)
	var blockCount uint32
	if err := binary.Read(indexReader, binary.LittleEndian, &blockCount); err != nil {
		return nil, err
	}

	// 读取第一个数据块
	if blockCount == 0 {
		return nil, errors.New("empty SSTable")
	}

	// 读取第一个键
	var keyLen uint32
	if err := binary.Read(indexReader, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}
	firstKey := make([]byte, keyLen)
	if _, err := io.ReadFull(indexReader, firstKey); err != nil {
		return nil, err
	}

	// 读取第一个数据块偏移量
	var offset uint64
	if err := binary.Read(indexReader, binary.LittleEndian, &offset); err != nil {
		return nil, err
	}

	// 读取数据块大小
	dataBlockSizeBuf := make([]byte, 4)
	if _, err := sst.file.ReadAt(dataBlockSizeBuf, int64(offset)); err != nil {
		return nil, err
	}
	dataBlockSize := binary.LittleEndian.Uint32(dataBlockSizeBuf)

	// 读取数据块
	dataBlockBuf := make([]byte, dataBlockSize)
	if _, err := sst.file.ReadAt(dataBlockBuf, int64(offset)+4); err != nil {
		return nil, err
	}

	return &SSTableIterator{
		sst:       sst,
		dataBlock: dataBlockBuf,
		pos:       0,
		closed:    false,
	}, nil
}

// Next 移动到下一个条目
func (it *SSTableIterator) Next() bool {
	if it.closed || it.pos >= len(it.dataBlock) {
		return false
	}

	// 读取键长度
	if it.pos+4 > len(it.dataBlock) {
		return false
	}
	keyLen := binary.LittleEndian.Uint32(it.dataBlock[it.pos : it.pos+4])
	it.pos += 4

	// 读取键
	if it.pos+int(keyLen) > len(it.dataBlock) {
		return false
	}
	it.currentKey = make([]byte, keyLen)
	copy(it.currentKey, it.dataBlock[it.pos:it.pos+int(keyLen)])
	it.pos += int(keyLen)

	// 读取序列号
	if it.pos+8 > len(it.dataBlock) {
		return false
	}
	it.currentSeq = binary.LittleEndian.Uint64(it.dataBlock[it.pos : it.pos+8])
	it.pos += 8

	// 读取操作类型
	if it.pos >= len(it.dataBlock) {
		return false
	}
	it.currentType = it.dataBlock[it.pos]
	it.pos++

	// 读取值长度
	if it.pos+4 > len(it.dataBlock) {
		return false
	}
	valueLen := binary.LittleEndian.Uint32(it.dataBlock[it.pos : it.pos+4])
	it.pos += 4

	// 读取值
	if it.pos+int(valueLen) > len(it.dataBlock) {
		return false
	}
	it.currentVal = string(it.dataBlock[it.pos : it.pos+int(valueLen)])
	it.pos += int(valueLen)

	return true
}

// Key 返回当前键
func (it *SSTableIterator) Key() []byte {
	return it.currentKey
}

// Value 返回当前值
func (it *SSTableIterator) Value() string {
	return it.currentVal
}

// Sequence 返回当前序列号
func (it *SSTableIterator) Sequence() uint64 {
	return it.currentSeq
}

// OpType 返回当前操作类型
func (it *SSTableIterator) OpType() byte {
	return it.currentType
}

// Close 关闭迭代器
func (it *SSTableIterator) Close() {
	it.closed = true
	it.dataBlock = nil
}

// MergeIterator 表示合并迭代器
type MergeIterator struct {
	iterators []*SSTableIterator
	current   int
	closed    bool
}

// NewMergeIterator 创建新的合并迭代器
func NewMergeIterator(iterators []*SSTableIterator) *MergeIterator {
	// 初始化所有迭代器
	for _, it := range iterators {
		it.Next()
	}

	return &MergeIterator{
		iterators: iterators,
		current:   -1,
		closed:    false,
	}
}

// Next 移动到下一个条目
func (it *MergeIterator) Next() bool {
	if it.closed {
		return false
	}

	// 如果是第一次调用，找到最小的键
	if it.current == -1 {
		return it.findSmallest()
	}

	// 当前迭代器移动到下一个
	if !it.iterators[it.current].Next() {
		// 当前迭代器已结束，找下一个最小的键
		it.iterators[it.current].Close()
		it.iterators[it.current] = nil
		return it.findSmallest()
	}

	// 找到最小的键
	return it.findSmallest()
}

// findSmallest 找到所有迭代器中最小的键
func (it *MergeIterator) findSmallest() bool {
	smallest := -1
	var smallestKey []byte

	for i, iterator := range it.iterators {
		if iterator == nil {
			continue
		}

		key := iterator.Key()
		if key == nil {
			continue
		}

		if smallest == -1 || bytes.Compare(key, smallestKey) < 0 {
			smallest = i
			smallestKey = key
		}
	}

	if smallest == -1 {
		it.current = -1
		return false
	}

	it.current = smallest
	return true
}

// Key 返回当前键
func (it *MergeIterator) Key() []byte {
	if it.current == -1 || it.iterators[it.current] == nil {
		return nil
	}
	return it.iterators[it.current].Key()
}

// Value 返回当前值
func (it *MergeIterator) Value() string {
	if it.current == -1 || it.iterators[it.current] == nil {
		return ""
	}
	return it.iterators[it.current].Value()
}

// Sequence 返回当前序列号
func (it *MergeIterator) Sequence() uint64 {
	if it.current == -1 || it.iterators[it.current] == nil {
		return 0
	}
	return it.iterators[it.current].Sequence()
}

// OpType 返回当前操作类型
func (it *MergeIterator) OpType() byte {
	if it.current == -1 || it.iterators[it.current] == nil {
		return 0
	}
	return it.iterators[it.current].OpType()
}

// Close 关闭迭代器
func (it *MergeIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	for _, iterator := range it.iterators {
		if iterator != nil {
			iterator.Close()
		}
	}
	it.iterators = nil
}

// searchSSTableRange 在SSTable中范围查找
func (idx *LSMIndex) searchSSTableRange(sst *LSMSSTable, startKey, endKey []byte, includeStart, includeEnd bool) ([]string, error) {
	// 打开文件（如果尚未打开）
	sst.mu.Lock()
	if sst.closed || sst.file == nil {
		file, err := os.Open(sst.path)
		if err != nil {
			sst.mu.Unlock()
			return nil, err
		}
		sst.file = file
		sst.closed = false
	}
	sst.mu.Unlock()

	// 创建迭代器
	it, err := idx.newSSTableIterator(sst)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	// 结果集（去重）
	resultSet := make(map[string]struct{})

	// 遍历所有条目
	for it.Next() {
		key := it.Key()
		value := it.Value()
		opType := it.OpType()

		// 检查下限
		if startKey != nil {
			cmp := bytes.Compare(key, startKey)
			if includeStart {
				if cmp < 0 {
					continue
				}
			} else {
				if cmp <= 0 {
					continue
				}
			}
		}

		// 检查上限
		if endKey != nil {
			cmp := bytes.Compare(key, endKey)
			if includeEnd {
				if cmp > 0 {
					break
				}
			} else {
				if cmp >= 0 {
					break
				}
			}
		}

		// 检查操作类型
		if opType == 1 && value != "" {
			resultSet[value] = struct{}{}
		}
	}

	// 转换为切片
	result := make([]string, 0, len(resultSet))
	for value := range resultSet {
		result = append(result, value)
	}

	return result, nil
}
