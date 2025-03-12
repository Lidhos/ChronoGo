package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/model"
	"ChronoGo/pkg/util"
)

const (
	// SSTable 文件格式版本
	sstableFormatVersion = 1

	// SSTable 文件扩展名
	sstableFileExt = ".sst"

	// SSTable 文件名格式: dbname_tablename_level_timestamp.sst
	sstableFileFormat = "%s_%s_%d_%d%s"

	// SSTable 索引块大小 (默认 4KB)
	defaultIndexBlockSize = 4 * 1024

	// SSTable 数据块大小 (默认 64KB)
	defaultDataBlockSize = 64 * 1024

	// SSTable 过滤器块大小 (默认 2KB)
	defaultFilterBlockSize = 2 * 1024

	// SSTable 元数据块大小 (默认 1KB)
	defaultMetaBlockSize = 1 * 1024

	// SSTable 页脚大小 (固定 48 字节)
	sstableFooterSize = 48

	// SSTable 魔数 (用于文件格式验证)
	sstableMagicNumber uint32 = 0x53535442 // "SSTB" in ASCII

	// SSTable 压缩类型
	sstableCompressionNone   = 0 // 不压缩
	sstableCompressionSnappy = 1 // Snappy 压缩
	sstableCompressionGzip   = 2 // Gzip 压缩
	sstableCompressionZstd   = 3 // Zstd 压缩
	sstableCompressionLz4    = 4 // LZ4 压缩

	// SSTable 块类型
	sstableBlockTypeData    = 0 // 数据块
	sstableBlockTypeIndex   = 1 // 索引块
	sstableBlockTypeFilter  = 2 // 过滤器块
	sstableBlockTypeMeta    = 3 // 元数据块
	sstableBlockTypeTagMeta = 4 // 标签元数据块

	// SSTable 合并策略
	sstableMergeStrategyTiered  = 0 // 分层合并策略
	sstableMergeStrategyLeveled = 1 // 分级合并策略
	sstableMergeStrategyHybrid  = 2 // 混合合并策略

	// SSTable 最大层级
	sstableMaxLevel = 7

	// SSTable 每层大小倍数 (默认为 10)
	sstableSizeRatio = 10
)

// SSTableOptions 表示 SSTable 配置选项
type SSTableOptions struct {
	DataBlockSize     int  // 数据块大小
	IndexBlockSize    int  // 索引块大小
	FilterBlockSize   int  // 过滤器块大小
	MetaBlockSize     int  // 元数据块大小
	CompressionType   int  // 压缩类型
	UseBloomFilter    bool // 是否使用布隆过滤器
	BloomFilterBits   int  // 布隆过滤器位数 (每个键)
	CacheIndexBlocks  bool // 是否缓存索引块
	CacheFilterBlocks bool // 是否缓存过滤器块
	MergeStrategy     int  // 合并策略
}

// DefaultSSTableOptions 返回默认的 SSTable 配置选项
func DefaultSSTableOptions() *SSTableOptions {
	return &SSTableOptions{
		DataBlockSize:     defaultDataBlockSize,
		IndexBlockSize:    defaultIndexBlockSize,
		FilterBlockSize:   defaultFilterBlockSize,
		MetaBlockSize:     defaultMetaBlockSize,
		CompressionType:   sstableCompressionSnappy,    // 默认使用 Snappy 压缩
		UseBloomFilter:    true,                        // 默认使用布隆过滤器
		BloomFilterBits:   10,                          // 默认每个键使用 10 位
		CacheIndexBlocks:  true,                        // 默认缓存索引块
		CacheFilterBlocks: true,                        // 默认缓存过滤器块
		MergeStrategy:     sstableMergeStrategyLeveled, // 默认使用分级合并策略
	}
}

// SSTableManager 管理 SSTable 文件
type SSTableManager struct {
	dataDir      string                    // 数据目录
	options      *SSTableOptions           // 配置选项
	tables       map[string]*SSTableInfo   // SSTable 信息映射 (文件名 -> 信息)
	levels       [sstableMaxLevel][]string // 每个层级的 SSTable 文件列表
	manifest     *SSTableManifest          // SSTable 清单
	manifestFile string                    // 清单文件路径
	mu           sync.RWMutex              // 读写锁
	bgCompactor  *SSTableCompactor         // 后台压缩器
	engine       *StorageEngine            // 存储引擎引用
}

// SSTableInfo 表示 SSTable 文件信息
type SSTableInfo struct {
	FileName    string    // 文件名
	DBName      string    // 数据库名
	TableName   string    // 表名
	Level       int       // 层级
	TimeRange   TimeRange // 时间范围
	Size        int64     // 文件大小
	KeyCount    int       // 键数量
	CreatedAt   time.Time // 创建时间
	MinKey      int64     // 最小键 (时间戳)
	MaxKey      int64     // 最大键 (时间戳)
	IndexOffset int64     // 索引偏移量
	IndexSize   int       // 索引大小
	HasFilter   bool      // 是否有过滤器
	TagMeta     []TagMeta // 标签元数据
}

// TagMeta 表示标签元数据
type TagMeta struct {
	Key      string   // 标签键
	Values   []string // 标签值列表
	ValueMap []int64  // 值到时间戳的映射
}

// SSTableManifest 表示 SSTable 清单
type SSTableManifest struct {
	FormatVersion int                       // 格式版本
	Tables        map[string]SSTableInfo    // SSTable 信息映射
	Levels        [sstableMaxLevel][]string // 每个层级的 SSTable 文件列表
	LastCompact   time.Time                 // 上次压缩时间
	NextFileID    int64                     // 下一个文件 ID
}

// SSTableReader 用于读取 SSTable 文件
type SSTableReader struct {
	file        *os.File        // 文件句柄
	info        *SSTableInfo    // 文件信息
	indexCache  []byte          // 索引缓存
	filterCache []byte          // 过滤器缓存
	options     *SSTableOptions // 配置选项
	mu          sync.RWMutex    // 读写锁
}

// SSTableWriter 用于写入 SSTable 文件
type SSTableWriter struct {
	file                *os.File                      // 文件句柄
	writer              *bufio.Writer                 // 缓冲写入器
	mmapFile            *util.MmapFile                // 内存映射文件
	dataBlocks          [][]byte                      // 数据块列表
	indexEntries        []indexEntry                  // 索引条目列表
	currentBlock        *bytes.Buffer                 // 当前数据块
	currentBlockEntries int                           // 当前数据块条目数
	lastKey             int64                         // 上一个键 (时间戳)
	options             *SSTableOptions               // 配置选项
	dbName              string                        // 数据库名
	tableName           string                        // 表名
	level               int                           // 层级
	minKey              int64                         // 最小键 (时间戳)
	maxKey              int64                         // 最大键 (时间戳)
	keyCount            int64                         // 键数量 - 修改为int64类型
	tagMeta             map[string]map[string][]int64 // 标签元数据
	currentOffset       int64                         // 当前写入偏移量
	useZeroCopy         bool                          // 是否使用零拷贝
}

// indexEntry 表示索引条目
type indexEntry struct {
	Key         int64 // 键 (时间戳)
	BlockOffset int64 // 块偏移量
	BlockSize   int   // 块大小
}

// blockHeader 表示块头
type blockHeader struct {
	Type             byte   // 块类型
	CompressionType  byte   // 压缩类型
	UncompressedSize uint32 // 未压缩大小
	CompressedSize   uint32 // 压缩大小
	EntryCount       uint32 // 条目数量
}

// SSTableCompactor 用于 SSTable 压缩
type SSTableCompactor struct {
	manager        *SSTableManager  // SSTable 管理器
	compactChan    chan compactTask // 压缩任务通道
	closing        chan struct{}    // 关闭信号
	wg             sync.WaitGroup   // 等待组
	compactWorkers int              // 压缩工作线程数
}

// compactTask 表示压缩任务
type compactTask struct {
	level    int      // 层级
	files    []string // 文件列表
	priority int      // 优先级
}

// SkipListIterator 表示跳表迭代器
type SkipListIterator struct {
	list    *SkipList
	current *SkipNode
}

// Iterator 创建新的跳表迭代器
func (sl *SkipList) Iterator() *SkipListIterator {
	return &SkipListIterator{
		list:    sl,
		current: nil,
	}
}

// SeekToFirst 定位到第一个节点
func (it *SkipListIterator) SeekToFirst() {
	it.current = it.list.head.forward[0]
}

// Seek 定位到指定键
func (it *SkipListIterator) Seek(key int64) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	current := it.list.head
	for i := it.list.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}

	it.current = current.forward[0]
}

// Next 移动到下一个节点
func (it *SkipListIterator) Next() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}

// Valid 检查迭代器是否有效
func (it *SkipListIterator) Valid() bool {
	return it.current != nil
}

// Key 获取当前键
func (it *SkipListIterator) Key() int64 {
	if it.current == nil {
		return 0
	}
	return it.current.key
}

// Value 获取当前值
func (it *SkipListIterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.value
}

// NewSSTableManager 创建新的 SSTable 管理器
func NewSSTableManager(dataDir string, options *SSTableOptions, engine *StorageEngine) (*SSTableManager, error) {
	if options == nil {
		options = DefaultSSTableOptions()
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create SSTable data directory: %w", err)
	}

	manager := &SSTableManager{
		dataDir:      dataDir,
		options:      options,
		tables:       make(map[string]*SSTableInfo),
		manifestFile: filepath.Join(dataDir, "MANIFEST"),
		engine:       engine,
	}

	// 加载清单文件
	if err := manager.loadManifest(); err != nil {
		// 如果清单文件不存在，创建新的
		if os.IsNotExist(err) {
			manager.manifest = &SSTableManifest{
				FormatVersion: sstableFormatVersion,
				Tables:        make(map[string]SSTableInfo),
				LastCompact:   time.Now(),
				NextFileID:    1,
			}
			// 保存新的清单文件
			if err := manager.saveManifest(); err != nil {
				return nil, fmt.Errorf("failed to save new manifest: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to load manifest: %w", err)
		}
	}

	// 初始化后台压缩器
	manager.bgCompactor = NewSSTableCompactor(manager, 2) // 2个压缩工作线程
	manager.bgCompactor.Start()

	return manager, nil
}

// loadManifest 加载清单文件
func (m *SSTableManager) loadManifest() error {
	// 打开清单文件
	file, err := os.Open(m.manifestFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// 读取清单数据
	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read manifest file: %w", err)
	}

	// 解析清单数据
	manifest := &SSTableManifest{}
	if err := json.Unmarshal(data, manifest); err != nil {
		return fmt.Errorf("failed to parse manifest file: %w", err)
	}

	m.manifest = manifest

	// 加载 SSTable 信息
	for fileName, info := range manifest.Tables {
		m.tables[fileName] = &SSTableInfo{
			FileName:    info.FileName,
			DBName:      info.DBName,
			TableName:   info.TableName,
			Level:       info.Level,
			TimeRange:   info.TimeRange,
			Size:        info.Size,
			KeyCount:    info.KeyCount,
			CreatedAt:   info.CreatedAt,
			MinKey:      info.MinKey,
			MaxKey:      info.MaxKey,
			IndexOffset: info.IndexOffset,
			IndexSize:   info.IndexSize,
			HasFilter:   info.HasFilter,
			TagMeta:     info.TagMeta,
		}
	}

	// 加载层级信息
	for level, files := range manifest.Levels {
		m.levels[level] = make([]string, len(files))
		copy(m.levels[level], files)
	}

	return nil
}

// saveManifest 保存清单文件
func (m *SSTableManager) saveManifest() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新清单中的表信息
	for fileName, info := range m.tables {
		m.manifest.Tables[fileName] = *info
	}

	// 更新清单中的层级信息
	for level, files := range m.levels {
		m.manifest.Levels[level] = make([]string, len(files))
		copy(m.manifest.Levels[level], files)
	}

	// 序列化清单数据
	data, err := json.MarshalIndent(m.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize manifest: %w", err)
	}

	// 写入临时文件
	tempFile := m.manifestFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary manifest file: %w", err)
	}

	// 原子替换清单文件
	if err := os.Rename(tempFile, m.manifestFile); err != nil {
		return fmt.Errorf("failed to replace manifest file: %w", err)
	}

	return nil
}

// CreateSSTableFromMemTable 从内存表创建 SSTable 文件
func (m *SSTableManager) CreateSSTableFromMemTable(memTable *MemTable, dbName, tableName string) (string, error) {
	m.mu.Lock()
	fileID := m.manifest.NextFileID
	m.manifest.NextFileID++
	m.mu.Unlock()

	// 生成文件名
	fileName := fmt.Sprintf(sstableFileFormat, dbName, tableName, 0, fileID, sstableFileExt)
	filePath := filepath.Join(m.dataDir, fileName)

	// 创建 SSTable 写入器 - 使用零拷贝优化
	writer, err := NewSSTableWriter(filePath, dbName, tableName, 0, m.options)
	if err != nil {
		return "", fmt.Errorf("failed to create SSTable writer: %w", err)
	}
	defer writer.Close()

	// 使用 SkipList 的迭代器
	iter := memTable.data.Iterator()

	// 写入所有数据点
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// 解析 BSON 数据
		var doc bson.D
		if err := bson.Unmarshal(value, &doc); err != nil {
			logger.Printf("Failed to unmarshal BSON data: %v", err)
			continue
		}

		// 提取标签
		tags := make(map[string]string)
		for _, elem := range doc {
			if elem.Key == "tags" {
				if tagsMap, ok := elem.Value.(bson.D); ok {
					for _, tag := range tagsMap {
						if tagValue, ok := tag.Value.(string); ok {
							tags[tag.Key] = tagValue
						}
					}
				}
			}
		}

		// 写入数据点
		if err := writer.Add(key, value, tags); err != nil {
			return "", fmt.Errorf("failed to add data point to SSTable: %w", err)
		}
	}

	// 完成写入
	info, err := writer.Finish()
	if err != nil {
		return "", fmt.Errorf("failed to finish SSTable writing: %w", err)
	}

	// 添加到管理器
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tables[fileName] = info
	m.levels[0] = append(m.levels[0], fileName)

	// 保存清单
	if err := m.saveManifest(); err != nil {
		return "", fmt.Errorf("failed to save manifest after creating SSTable: %w", err)
	}

	// 触发后台压缩
	m.triggerCompaction(0)

	return fileName, nil
}

// triggerCompaction 触发指定层级的压缩
func (m *SSTableManager) triggerCompaction(level int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查是否需要压缩
	if level >= sstableMaxLevel-1 {
		return
	}

	// 计算层级大小限制
	levelSizeLimit := int64(m.options.DataBlockSize) * int64(sstableSizeRatio) * int64(1<<uint(level))

	// 计算当前层级大小
	var levelSize int64
	for _, fileName := range m.levels[level] {
		if info, ok := m.tables[fileName]; ok {
			levelSize += info.Size
		}
	}

	// 如果超过限制，触发压缩
	if levelSize > levelSizeLimit {
		files := make([]string, len(m.levels[level]))
		copy(files, m.levels[level])

		// 创建压缩任务
		task := compactTask{
			level:    level,
			files:    files,
			priority: sstableMaxLevel - level, // 优先级与层级成反比
		}

		// 发送到压缩通道
		select {
		case m.bgCompactor.compactChan <- task:
			logger.Printf("INFO: Triggered compaction for level %d with %d files", level, len(files))
		default:
			logger.Printf("WARN: Compaction channel is full, skipping compaction for level %d", level)
		}
	}
}

// Get 从 SSTable 中获取指定时间戳的数据
func (m *SSTableManager) Get(dbName, tableName string, timestamp int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 按层级从低到高查找
	for level := 0; level < sstableMaxLevel; level++ {
		for _, fileName := range m.levels[level] {
			info, ok := m.tables[fileName]
			if !ok {
				continue
			}

			// 检查数据库和表名是否匹配
			if info.DBName != dbName || info.TableName != tableName {
				continue
			}

			// 检查时间戳是否在范围内
			if timestamp < info.MinKey || timestamp > info.MaxKey {
				continue
			}

			// 打开 SSTable 读取器
			reader, err := m.openReader(fileName)
			if err != nil {
				logger.Printf("WARN: Failed to open SSTable reader for %s: %v", fileName, err)
				continue
			}

			// 查找数据
			value, err := reader.Get(timestamp)
			if err == nil {
				return value, nil
			}

			// 如果是未找到错误，继续查找下一个文件
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			// 其他错误直接返回
			return nil, err
		}
	}

	return nil, os.ErrNotExist
}

// Query 从 SSTable 中查询指定时间范围和标签的数据
func (m *SSTableManager) Query(dbName, tableName string, timeRange TimeRange, tags map[string]string) ([]*model.TimeSeriesPoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*model.TimeSeriesPoint

	// 按层级从低到高查找
	for level := 0; level < sstableMaxLevel; level++ {
		for _, fileName := range m.levels[level] {
			info, ok := m.tables[fileName]
			if !ok {
				continue
			}

			// 检查数据库和表名是否匹配
			if info.DBName != dbName || info.TableName != tableName {
				continue
			}

			// 检查时间范围是否有交集
			if timeRange.End < info.MinKey || timeRange.Start > info.MaxKey {
				continue
			}

			// 检查标签是否匹配
			if !m.matchTags(info, tags) {
				continue
			}

			// 打开 SSTable 读取器
			reader, err := m.openReader(fileName)
			if err != nil {
				logger.Printf("WARN: Failed to open SSTable reader for %s: %v", fileName, err)
				continue
			}

			// 查询数据
			points, err := reader.Query(timeRange, tags)
			if err != nil {
				logger.Printf("WARN: Failed to query SSTable %s: %v", fileName, err)
				continue
			}

			results = append(results, points...)
		}
	}

	// 按时间戳排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results, nil
}

// matchTags 检查 SSTable 是否包含指定的标签
func (m *SSTableManager) matchTags(info *SSTableInfo, tags map[string]string) bool {
	if len(tags) == 0 {
		return true
	}

	// 检查每个标签
	for key, value := range tags {
		found := false
		for _, meta := range info.TagMeta {
			if meta.Key == key {
				// 检查值是否存在
				for _, v := range meta.Values {
					if v == value {
						found = true
						break
					}
				}
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// openReader 打开 SSTable 读取器
func (m *SSTableManager) openReader(fileName string) (*SSTableReader, error) {
	info, ok := m.tables[fileName]
	if !ok {
		return nil, fmt.Errorf("SSTable %s not found", fileName)
	}

	filePath := filepath.Join(m.dataDir, fileName)
	return NewSSTableReader(filePath, info, m.options)
}

// CompactLevel 压缩指定层级的 SSTable 文件
func (m *SSTableManager) CompactLevel(level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查层级是否有效
	if level < 0 || level >= sstableMaxLevel-1 {
		return fmt.Errorf("invalid level: %d", level)
	}

	// 获取当前层级的文件
	files := m.levels[level]
	if len(files) == 0 {
		return nil
	}

	// 获取下一层级的文件
	nextLevelFiles := m.levels[level+1]

	// 创建合并器
	merger := NewSSTableMerger(m.dataDir, m.options)

	// 添加当前层级的文件
	for _, fileName := range files {
		info, ok := m.tables[fileName]
		if !ok {
			continue
		}
		filePath := filepath.Join(m.dataDir, fileName)
		if err := merger.AddFile(filePath, info); err != nil {
			return fmt.Errorf("failed to add file %s to merger: %w", fileName, err)
		}
	}

	// 添加下一层级中与当前层级有重叠的文件
	for _, fileName := range nextLevelFiles {
		info, ok := m.tables[fileName]
		if !ok {
			continue
		}

		// 检查是否有重叠
		overlap := false
		for _, currentFile := range files {
			currentInfo, ok := m.tables[currentFile]
			if !ok {
				continue
			}

			// 检查时间范围是否有交集
			if !(info.MaxKey < currentInfo.MinKey || info.MinKey > currentInfo.MaxKey) {
				overlap = true
				break
			}
		}

		if overlap {
			filePath := filepath.Join(m.dataDir, fileName)
			if err := merger.AddFile(filePath, info); err != nil {
				return fmt.Errorf("failed to add file %s to merger: %w", fileName, err)
			}
		}
	}

	// 执行合并
	newFiles, err := merger.Merge(level + 1)
	if err != nil {
		return fmt.Errorf("failed to merge files: %w", err)
	}

	// 更新清单
	for _, fileName := range files {
		delete(m.tables, fileName)
		// 删除文件
		filePath := filepath.Join(m.dataDir, fileName)
		if err := os.Remove(filePath); err != nil {
			logger.Printf("WARN: Failed to remove compacted file %s: %v", fileName, err)
		}
	}

	// 更新下一层级中被合并的文件
	for _, fileName := range nextLevelFiles {
		info, ok := m.tables[fileName]
		if !ok {
			continue
		}

		// 检查是否有重叠
		overlap := false
		for _, currentFile := range files {
			currentInfo, ok := m.tables[currentFile]
			if !ok {
				continue
			}

			// 检查时间范围是否有交集
			if !(info.MaxKey < currentInfo.MinKey || info.MinKey > currentInfo.MaxKey) {
				overlap = true
				break
			}
		}

		if overlap {
			delete(m.tables, fileName)
			// 删除文件
			filePath := filepath.Join(m.dataDir, fileName)
			if err := os.Remove(filePath); err != nil {
				logger.Printf("WARN: Failed to remove compacted file %s: %v", fileName, err)
			}
		}
	}

	// 更新层级信息
	m.levels[level] = nil
	for _, file := range newFiles {
		fileName := filepath.Base(file)
		m.levels[level+1] = append(m.levels[level+1], fileName)
	}

	// 保存清单
	if err := m.saveManifest(); err != nil {
		return fmt.Errorf("failed to save manifest after compaction: %w", err)
	}

	// 触发下一层级的压缩
	m.triggerCompaction(level + 1)

	return nil
}

// NewSSTableReader 创建新的 SSTable 读取器
func NewSSTableReader(filePath string, info *SSTableInfo, options *SSTableOptions) (*SSTableReader, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}

	reader := &SSTableReader{
		file:    file,
		info:    info,
		options: options,
	}

	// 预加载索引
	if options.CacheIndexBlocks {
		if err := reader.loadIndex(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to load index: %w", err)
		}
	}

	// 预加载过滤器
	if options.CacheFilterBlocks && info.HasFilter {
		if err := reader.loadFilter(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to load filter: %w", err)
		}
	}

	return reader, nil
}

// loadIndex 加载索引块
func (r *SSTableReader) loadIndex() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果已经加载，直接返回
	if r.indexCache != nil {
		return nil
	}

	// 定位到索引块
	if _, err := r.file.Seek(r.info.IndexOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to index block: %w", err)
	}

	// 读取索引块
	indexData := make([]byte, r.info.IndexSize)
	if _, err := io.ReadFull(r.file, indexData); err != nil {
		return fmt.Errorf("failed to read index block: %w", err)
	}

	// 解析索引块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(indexData) < headerSize {
		return fmt.Errorf("index block too small")
	}

	// 解析头部
	headerBuf := bytes.NewReader(indexData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to parse index block header: %w", err)
	}

	// 检查块类型
	if header.Type != sstableBlockTypeIndex {
		return fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeIndex, header.Type)
	}

	// 解压缩数据
	var indexBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		indexBytes, err = decompressBlock(indexData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return fmt.Errorf("failed to decompress index block: %w", err)
		}
	} else {
		indexBytes = indexData[headerSize:]
	}

	// 缓存索引数据
	r.indexCache = indexBytes

	return nil
}

// loadFilter 加载过滤器块
func (r *SSTableReader) loadFilter() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果已经加载，直接返回
	if r.filterCache != nil {
		return nil
	}

	// 定位到页脚
	fileSize, err := r.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	// 读取页脚
	footerData := make([]byte, sstableFooterSize)
	if _, err := r.file.Seek(fileSize-sstableFooterSize, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to footer: %w", err)
	}
	if _, err := io.ReadFull(r.file, footerData); err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}

	// 解析页脚
	footer := parseFooter(footerData)

	// 检查魔数
	if footer.MagicNumber != sstableMagicNumber {
		return fmt.Errorf("invalid magic number: expected %d, got %d", sstableMagicNumber, footer.MagicNumber)
	}

	// 如果没有过滤器，直接返回
	if footer.FilterOffset == 0 || footer.FilterSize == 0 {
		return nil
	}

	// 定位到过滤器块
	if _, err := r.file.Seek(int64(footer.FilterOffset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to filter block: %w", err)
	}

	// 读取过滤器块
	filterData := make([]byte, footer.FilterSize)
	if _, err := io.ReadFull(r.file, filterData); err != nil {
		return fmt.Errorf("failed to read filter block: %w", err)
	}

	// 解析过滤器块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(filterData) < headerSize {
		return fmt.Errorf("filter block too small")
	}

	// 解析头部
	headerBuf := bytes.NewReader(filterData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to parse filter block header: %w", err)
	}

	// 检查块类型
	if header.Type != sstableBlockTypeFilter {
		return fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeFilter, header.Type)
	}

	// 解压缩数据
	var filterBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		filterBytes, err = decompressBlock(filterData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return fmt.Errorf("failed to decompress filter block: %w", err)
		}
	} else {
		filterBytes = filterData[headerSize:]
	}

	// 缓存过滤器数据
	r.filterCache = filterBytes

	return nil
}

// Get 获取指定时间戳的数据
func (r *SSTableReader) Get(timestamp int64) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 检查时间戳是否在范围内
	if timestamp < r.info.MinKey || timestamp > r.info.MaxKey {
		return nil, os.ErrNotExist
	}

	// 如果有过滤器，先检查过滤器
	if r.info.HasFilter && r.filterCache != nil {
		if !r.checkFilter(timestamp) {
			return nil, os.ErrNotExist
		}
	}

	// 查找索引
	blockOffset, blockSize, err := r.findBlock(timestamp)
	if err != nil {
		return nil, err
	}

	// 读取数据块
	blockData, err := r.readBlock(blockOffset, blockSize)
	if err != nil {
		return nil, err
	}

	// 在数据块中查找
	return r.findInBlock(blockData, timestamp)
}

// Query 查询指定时间范围和标签的数据
func (r *SSTableReader) Query(timeRange TimeRange, tags map[string]string) ([]*model.TimeSeriesPoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 检查时间范围是否有交集
	if timeRange.End < r.info.MinKey || timeRange.Start > r.info.MaxKey {
		return nil, nil
	}

	// 查找索引范围
	startBlockOffset, _, err := r.findBlock(timeRange.Start)
	if err != nil {
		// 如果找不到起始块，使用第一个块
		startBlockOffset, _, err = r.findFirstBlock()
		if err != nil {
			return nil, err
		}
	}

	var results []*model.TimeSeriesPoint

	// 读取并处理数据块
	currentOffset := startBlockOffset
	for currentOffset < r.info.IndexOffset {
		// 读取数据块
		blockData, err := r.readBlock(currentOffset, 0)
		if err != nil {
			return nil, err
		}

		// 解析数据块
		points, nextOffset, err := r.parseBlock(blockData, timeRange, tags)
		if err != nil {
			return nil, err
		}

		// 添加结果
		results = append(results, points...)

		// 更新偏移量
		if nextOffset <= currentOffset {
			break
		}
		currentOffset = nextOffset
	}

	return results, nil
}

// findBlock 查找包含指定时间戳的数据块
func (r *SSTableReader) findBlock(timestamp int64) (int64, int, error) {
	// 如果索引已缓存，使用缓存
	if r.indexCache != nil {
		return r.findBlockInCache(timestamp)
	}

	// 否则，从文件读取索引
	if _, err := r.file.Seek(r.info.IndexOffset, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("failed to seek to index: %w", err)
	}

	// 读取索引块
	indexData := make([]byte, r.info.IndexSize)
	if _, err := io.ReadFull(r.file, indexData); err != nil {
		return 0, 0, fmt.Errorf("failed to read index: %w", err)
	}

	// 解析索引块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(indexData) < headerSize {
		return 0, 0, fmt.Errorf("index block too small")
	}

	// 解析头部
	headerBuf := bytes.NewReader(indexData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		return 0, 0, fmt.Errorf("failed to parse index block header: %w", err)
	}

	// 检查块类型
	if header.Type != sstableBlockTypeIndex {
		return 0, 0, fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeIndex, header.Type)
	}

	// 解压缩数据
	var indexBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		indexBytes, err = decompressBlock(indexData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decompress index block: %w", err)
		}
	} else {
		indexBytes = indexData[headerSize:]
	}

	// 在索引中查找
	return r.findBlockInBytes(indexBytes, timestamp)
}

// findBlockInCache 在缓存的索引中查找数据块
func (r *SSTableReader) findBlockInCache(timestamp int64) (int64, int, error) {
	return r.findBlockInBytes(r.indexCache, timestamp)
}

// findBlockInBytes 在索引字节中查找数据块
func (r *SSTableReader) findBlockInBytes(indexBytes []byte, timestamp int64) (int64, int, error) {
	// 解析索引条目数量
	if len(indexBytes) < 4 {
		return 0, 0, fmt.Errorf("invalid index format")
	}
	entryCount := int(binary.LittleEndian.Uint32(indexBytes[:4]))

	// 二分查找
	entrySize := 16 // 每个索引条目的大小: key(8) + offset(4) + size(4)
	left, right := 0, entryCount-1

	for left <= right {
		mid := (left + right) / 2
		offset := 4 + mid*entrySize

		if offset+entrySize > len(indexBytes) {
			return 0, 0, fmt.Errorf("index entry out of bounds")
		}

		// 解析索引条目
		key := int64(binary.LittleEndian.Uint64(indexBytes[offset : offset+8]))
		blockOffset := int64(binary.LittleEndian.Uint32(indexBytes[offset+8 : offset+12]))
		blockSize := int(binary.LittleEndian.Uint32(indexBytes[offset+12 : offset+16]))

		if key == timestamp {
			return blockOffset, blockSize, nil
		} else if key < timestamp {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	// 如果没有精确匹配，返回最后一个小于等于目标的条目
	if right >= 0 {
		offset := 4 + right*entrySize
		blockOffset := int64(binary.LittleEndian.Uint32(indexBytes[offset+8 : offset+12]))
		blockSize := int(binary.LittleEndian.Uint32(indexBytes[offset+12 : offset+16]))
		return blockOffset, blockSize, nil
	}

	return 0, 0, os.ErrNotExist
}

// findFirstBlock 查找第一个数据块
func (r *SSTableReader) findFirstBlock() (int64, int, error) {
	// 如果索引已缓存，使用缓存
	if r.indexCache != nil {
		// 解析索引条目数量
		if len(r.indexCache) < 4 {
			return 0, 0, fmt.Errorf("invalid index format")
		}

		// 检查是否有条目
		entryCount := int(binary.LittleEndian.Uint32(r.indexCache[:4]))
		if entryCount == 0 {
			return 0, 0, fmt.Errorf("empty index")
		}

		// 获取第一个条目
		entrySize := 16 // 每个索引条目的大小: key(8) + offset(4) + size(4)
		offset := 4     // 跳过条目数量

		if offset+entrySize > len(r.indexCache) {
			return 0, 0, fmt.Errorf("index entry out of bounds")
		}

		// 解析索引条目
		blockOffset := int64(binary.LittleEndian.Uint32(r.indexCache[offset+8 : offset+12]))
		blockSize := int(binary.LittleEndian.Uint32(r.indexCache[offset+12 : offset+16]))

		return blockOffset, blockSize, nil
	}

	// 否则，从文件读取索引
	if _, err := r.file.Seek(r.info.IndexOffset, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("failed to seek to index: %w", err)
	}

	// 读取索引块头和条目数量
	var header blockHeader
	headerSize := binary.Size(header)
	headerAndCount := make([]byte, headerSize+4)
	if _, err := io.ReadFull(r.file, headerAndCount); err != nil {
		return 0, 0, fmt.Errorf("failed to read index header: %w", err)
	}

	// 解析条目数量
	entryCount := int(binary.LittleEndian.Uint32(headerAndCount[headerSize:]))
	if entryCount == 0 {
		return 0, 0, fmt.Errorf("empty index")
	}

	// 读取第一个条目
	entryData := make([]byte, 16)
	if _, err := io.ReadFull(r.file, entryData); err != nil {
		return 0, 0, fmt.Errorf("failed to read index entry: %w", err)
	}

	// 解析索引条目
	blockOffset := int64(binary.LittleEndian.Uint32(entryData[8:12]))
	blockSize := int(binary.LittleEndian.Uint32(entryData[12:16]))

	return blockOffset, blockSize, nil
}

// readBlock 读取数据块
func (r *SSTableReader) readBlock(offset int64, size int) ([]byte, error) {
	// 如果大小未知，先读取块头
	if size == 0 {
		// 定位到块头
		if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to block: %w", err)
		}

		// 读取块头
		var header blockHeader
		headerSize := binary.Size(header)
		headerData := make([]byte, headerSize)
		if _, err := io.ReadFull(r.file, headerData); err != nil {
			return nil, fmt.Errorf("failed to read block header: %w", err)
		}

		// 解析块头
		headerBuf := bytes.NewReader(headerData)
		if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
			return nil, fmt.Errorf("failed to parse block header: %w", err)
		}

		// 计算总大小
		size = int(header.CompressedSize) + headerSize
	}

	// 定位到块开始
	if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to block: %w", err)
	}

	// 读取整个块
	blockData := make([]byte, size)
	if _, err := io.ReadFull(r.file, blockData); err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	return blockData, nil
}

// findInBlock 在数据块中查找指定时间戳的数据
func (r *SSTableReader) findInBlock(blockData []byte, timestamp int64) ([]byte, error) {
	// 解析块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(blockData) < headerSize {
		return nil, fmt.Errorf("block too small")
	}

	// 解析头部
	headerBuf := bytes.NewReader(blockData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to parse block header: %w", err)
	}

	// 检查块类型
	if header.Type != sstableBlockTypeData {
		return nil, fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeData, header.Type)
	}

	// 解压缩数据
	var dataBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		dataBytes, err = decompressBlock(blockData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress data block: %w", err)
		}
	} else {
		dataBytes = blockData[headerSize:]
	}

	// 解析条目数量
	if len(dataBytes) < 4 {
		return nil, fmt.Errorf("invalid data block format")
	}
	entryCount := int(binary.LittleEndian.Uint32(dataBytes[:4]))

	// 二分查找
	offset := 4 // 跳过条目数量
	for i := 0; i < entryCount; i++ {
		// 检查边界
		if offset+8 > len(dataBytes) {
			return nil, fmt.Errorf("data entry out of bounds")
		}

		// 解析键和值大小
		key := int64(binary.LittleEndian.Uint64(dataBytes[offset : offset+8]))
		offset += 8

		if offset+4 > len(dataBytes) {
			return nil, fmt.Errorf("data entry out of bounds")
		}

		valueSize := int(binary.LittleEndian.Uint32(dataBytes[offset : offset+4]))
		offset += 4

		if offset+valueSize > len(dataBytes) {
			return nil, fmt.Errorf("data entry out of bounds")
		}

		// 检查键是否匹配
		if key == timestamp {
			return dataBytes[offset : offset+valueSize], nil
		}

		// 移动到下一个条目
		offset += valueSize
	}

	return nil, os.ErrNotExist
}

// parseBlock 解析数据块并提取符合条件的数据点
func (r *SSTableReader) parseBlock(blockData []byte, timeRange TimeRange, tags map[string]string) ([]*model.TimeSeriesPoint, int64, error) {
	// 解析块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(blockData) < headerSize {
		return nil, 0, fmt.Errorf("block too small")
	}

	// 解析头部
	headerBuf := bytes.NewReader(blockData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		return nil, 0, fmt.Errorf("failed to parse block header: %w", err)
	}

	// 检查块类型
	if header.Type != sstableBlockTypeData {
		return nil, 0, fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeData, header.Type)
	}

	// 解压缩数据
	var dataBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		dataBytes, err = decompressBlock(blockData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decompress data block: %w", err)
		}
	} else {
		dataBytes = blockData[headerSize:]
	}

	// 解析条目数量
	if len(dataBytes) < 4 {
		return nil, 0, fmt.Errorf("invalid data block format")
	}
	entryCount := int(binary.LittleEndian.Uint32(dataBytes[:4]))

	var results []*model.TimeSeriesPoint
	offset := 4 // 跳过条目数量

	for i := 0; i < entryCount; i++ {
		// 检查边界
		if offset+8 > len(dataBytes) {
			return nil, 0, fmt.Errorf("data entry out of bounds")
		}

		// 解析键和值大小
		key := int64(binary.LittleEndian.Uint64(dataBytes[offset : offset+8]))
		offset += 8

		if offset+4 > len(dataBytes) {
			return nil, 0, fmt.Errorf("data entry out of bounds")
		}

		valueSize := int(binary.LittleEndian.Uint32(dataBytes[offset : offset+4]))
		offset += 4

		if offset+valueSize > len(dataBytes) {
			return nil, 0, fmt.Errorf("data entry out of bounds")
		}

		// 检查时间范围
		if key >= timeRange.Start && key <= timeRange.End {
			// 解析 BSON 数据
			var doc bson.D
			if err := bson.Unmarshal(dataBytes[offset:offset+valueSize], &doc); err != nil {
				logger.Printf("WARN: Failed to unmarshal BSON data: %v", err)
				offset += valueSize
				continue
			}

			// 提取标签和字段
			tagsMap := make(map[string]string)
			fieldsMap := make(map[string]interface{})

			for _, elem := range doc {
				if elem.Key == "tags" {
					if tagsDoc, ok := elem.Value.(bson.D); ok {
						for _, tag := range tagsDoc {
							if tagValue, ok := tag.Value.(string); ok {
								tagsMap[tag.Key] = tagValue
							}
						}
					}
				} else if elem.Key == "fields" {
					if fieldsDoc, ok := elem.Value.(bson.D); ok {
						for _, field := range fieldsDoc {
							fieldsMap[field.Key] = field.Value
						}
					}
				}
			}

			// 检查标签是否匹配
			match := true
			for tagKey, tagValue := range tags {
				if val, ok := tagsMap[tagKey]; !ok || val != tagValue {
					match = false
					break
				}
			}

			if match {
				// 创建时序数据点
				point := &model.TimeSeriesPoint{
					Timestamp: key,
					Tags:      tagsMap,
					Fields:    fieldsMap,
				}

				results = append(results, point)
			}
		}

		// 移动到下一个条目
		offset += valueSize
	}

	// 计算下一个块的偏移量
	nextOffset := int64(0)
	if header.Type == sstableBlockTypeData {
		nextOffset = int64(offset) + int64(headerSize)
	}

	return results, nextOffset, nil
}

// checkFilter 检查时间戳是否可能存在于 SSTable 中
func (r *SSTableReader) checkFilter(timestamp int64) bool {
	// 如果没有过滤器，假设存在
	if r.filterCache == nil {
		return true
	}

	// 解析过滤器类型
	if len(r.filterCache) < 4 {
		return true
	}

	filterType := binary.LittleEndian.Uint32(r.filterCache[:4])

	// 目前只支持布隆过滤器
	if filterType != 1 {
		return true
	}

	// 解析布隆过滤器参数
	if len(r.filterCache) < 12 {
		return true
	}

	// 不使用 bitsPerKey 变量，直接使用值
	numHashes := binary.LittleEndian.Uint32(r.filterCache[8:12])

	// 检查过滤器数据
	if len(r.filterCache) < 16 {
		return true
	}

	filterSize := binary.LittleEndian.Uint32(r.filterCache[12:16])
	if len(r.filterCache) < 16+int(filterSize) {
		return true
	}

	filterData := r.filterCache[16 : 16+filterSize]

	// 计算哈希
	key := []byte(strconv.FormatInt(timestamp, 10))
	h1 := fnv1a(key)
	h2 := murmur3(key)

	// 检查位
	for i := uint32(0); i < numHashes; i++ {
		hash := (h1 + i*h2) % (filterSize * 8)
		byteIndex := hash / 8
		bitOffset := hash % 8

		if byteIndex >= uint32(len(filterData)) {
			return true
		}

		if (filterData[byteIndex] & (1 << bitOffset)) == 0 {
			return false
		}
	}

	return true
}

// Close 关闭 SSTable 读取器
func (r *SSTableReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}

	return nil
}

// NewSSTableWriter 创建新的 SSTable 写入器
func NewSSTableWriter(filePath string, dbName, tableName string, level int, options *SSTableOptions) (*SSTableWriter, error) {
	if options == nil {
		options = DefaultSSTableOptions()
	}

	// 创建文件
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}

	// 检查是否支持零拷贝
	useZeroCopy := true

	writer := &SSTableWriter{
		file:          file,
		writer:        bufio.NewWriterSize(file, 64*1024), // 64KB 缓冲区
		dataBlocks:    make([][]byte, 0),
		currentBlock:  bytes.NewBuffer(make([]byte, 0, options.DataBlockSize)),
		options:       options,
		dbName:        dbName,
		tableName:     tableName,
		level:         level,
		minKey:        math.MaxInt64,
		maxKey:        math.MinInt64,
		keyCount:      0, // 修改为int64类型
		tagMeta:       make(map[string]map[string][]int64),
		currentOffset: 0,
		useZeroCopy:   useZeroCopy,
	}

	// 写入文件头
	header := []byte("SSTABLE")
	if _, err := writer.writer.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write file header: %w", err)
	}
	writer.currentOffset += int64(len(header))

	// 写入格式版本
	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, uint32(sstableFormatVersion))
	if _, err := writer.writer.Write(versionBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write format version: %w", err)
	}
	writer.currentOffset += 4

	// 如果使用零拷贝，刷新缓冲区并创建内存映射
	if useZeroCopy {
		// 刷新已写入的数据
		if err := writer.writer.Flush(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to flush writer: %w", err)
		}

		// 关闭文件，重新以内存映射方式打开
		fileName := file.Name()
		file.Close()

		// 预估文件大小 (根据数据量估算，这里使用一个较大的初始值)
		estimatedSize := int64(10 * 1024 * 1024) // 10MB

		// 创建内存映射文件
		mmapFile, err := util.NewMmapFile(fileName, estimatedSize, true)
		if err != nil {
			return nil, fmt.Errorf("failed to create mmap file: %w", err)
		}

		writer.mmapFile = mmapFile
		writer.file = nil   // 不再使用标准文件句柄
		writer.writer = nil // 不再使用缓冲写入器
	}

	return writer, nil
}

// writeToFile 写入数据到文件
func (w *SSTableWriter) writeToFile(data []byte) error {
	if w.useZeroCopy && w.mmapFile != nil {
		// 检查是否需要扩展映射文件大小
		if w.currentOffset+int64(len(data)) > w.mmapFile.Size() {
			newSize := (w.currentOffset + int64(len(data))) * 2 // 扩展为当前需要的两倍
			if err := w.mmapFile.Resize(newSize); err != nil {
				return fmt.Errorf("failed to resize mmap file: %w", err)
			}
		}

		// 使用内存映射写入
		if err := w.mmapFile.Write(w.currentOffset, data); err != nil {
			return fmt.Errorf("failed to write to mmap file: %w", err)
		}

		w.currentOffset += int64(len(data))
		return nil
	} else {
		// 使用标准写入
		n, err := w.writer.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		}

		w.currentOffset += int64(n)
		return nil
	}
}

// Add 添加数据点
func (w *SSTableWriter) Add(key int64, value []byte, tags map[string]string) error {
	// 更新键范围
	if key < w.minKey {
		w.minKey = key
	}
	if key > w.maxKey {
		w.maxKey = key
	}

	// 更新标签元数据
	for tagKey, tagValue := range tags {
		if _, ok := w.tagMeta[tagKey]; !ok {
			w.tagMeta[tagKey] = make(map[string][]int64)
		}
		w.tagMeta[tagKey][tagValue] = append(w.tagMeta[tagKey][tagValue], key)
	}

	// 检查是否需要刷新当前块
	if w.currentBlock.Len()+8+4+len(value) > w.options.DataBlockSize && w.currentBlockEntries > 0 {
		if err := w.flushCurrentBlock(); err != nil {
			return err
		}
	}

	// 写入键
	if err := binary.Write(w.currentBlock, binary.LittleEndian, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	// 写入值大小
	if err := binary.Write(w.currentBlock, binary.LittleEndian, uint32(len(value))); err != nil {
		return fmt.Errorf("failed to write value size: %w", err)
	}

	// 写入值
	if _, err := w.currentBlock.Write(value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	// 更新计数器
	w.currentBlockEntries++
	w.keyCount++
	w.lastKey = key

	return nil
}

// flushCurrentBlock 刷新当前数据块
func (w *SSTableWriter) flushCurrentBlock() error {
	// 如果当前块为空，直接返回
	if w.currentBlockEntries == 0 {
		return nil
	}

	// 准备块数据
	blockData := w.currentBlock.Bytes()

	// 在块开头写入条目数量
	entryCountBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(entryCountBuf, uint32(w.currentBlockEntries))
	blockData = append(entryCountBuf, blockData...)

	// 压缩块数据
	var compressedData []byte
	var compressionType byte

	if w.options.CompressionType != sstableCompressionNone && len(blockData) > 32 {
		var err error
		compressedData, err = compressBlock(blockData, w.options.CompressionType)
		if err != nil {
			return fmt.Errorf("failed to compress block: %w", err)
		}

		// 如果压缩后的大小大于原始大小的 90%，不使用压缩
		if float64(len(compressedData)) > float64(len(blockData))*0.9 {
			compressedData = blockData
			compressionType = sstableCompressionNone
		} else {
			compressionType = byte(w.options.CompressionType)
		}
	} else {
		compressedData = blockData
		compressionType = sstableCompressionNone
	}

	// 创建块头
	header := blockHeader{
		Type:             sstableBlockTypeData,
		CompressionType:  compressionType,
		UncompressedSize: uint32(len(blockData)),
		CompressedSize:   uint32(len(compressedData)),
		EntryCount:       uint32(w.currentBlockEntries),
	}

	// 获取当前位置作为块偏移量
	blockOffset := w.currentOffset

	// 写入块头
	headerBytes := make([]byte, 14) // 块头大小: 1+1+4+4+4 = 14字节
	headerBytes[0] = header.Type
	headerBytes[1] = header.CompressionType
	binary.LittleEndian.PutUint32(headerBytes[2:6], header.UncompressedSize)
	binary.LittleEndian.PutUint32(headerBytes[6:10], header.CompressedSize)
	binary.LittleEndian.PutUint32(headerBytes[10:14], header.EntryCount)

	if err := w.writeToFile(headerBytes); err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	}

	// 写入压缩后的块数据
	if err := w.writeToFile(compressedData); err != nil {
		return fmt.Errorf("failed to write block data: %w", err)
	}

	// 添加索引条目
	w.indexEntries = append(w.indexEntries, indexEntry{
		Key:         w.lastKey,
		BlockOffset: blockOffset,
		BlockSize:   len(headerBytes) + len(compressedData),
	})

	// 重置当前块
	w.currentBlock.Reset()
	w.currentBlockEntries = 0

	return nil
}

// writeIndexBlock 写入索引块
func (w *SSTableWriter) writeIndexBlock() (int64, int, error) {
	// 如果没有索引条目，直接返回
	if len(w.indexEntries) == 0 {
		return 0, 0, nil
	}

	// 创建索引块
	indexBlock := bytes.NewBuffer(make([]byte, 0, w.options.IndexBlockSize))

	// 写入条目数量
	if err := binary.Write(indexBlock, binary.LittleEndian, uint32(len(w.indexEntries))); err != nil {
		return 0, 0, fmt.Errorf("failed to write index entry count: %w", err)
	}

	// 写入索引条目
	for _, entry := range w.indexEntries {
		// 写入键
		if err := binary.Write(indexBlock, binary.LittleEndian, entry.Key); err != nil {
			return 0, 0, fmt.Errorf("failed to write index key: %w", err)
		}

		// 写入块偏移量
		if err := binary.Write(indexBlock, binary.LittleEndian, uint32(entry.BlockOffset)); err != nil {
			return 0, 0, fmt.Errorf("failed to write index block offset: %w", err)
		}

		// 写入块大小
		if err := binary.Write(indexBlock, binary.LittleEndian, uint32(entry.BlockSize)); err != nil {
			return 0, 0, fmt.Errorf("failed to write index block size: %w", err)
		}
	}

	// 压缩索引块
	indexData := indexBlock.Bytes()
	var compressedData []byte
	var compressionType byte

	if w.options.CompressionType != sstableCompressionNone && len(indexData) > 32 {
		var err error
		compressedData, err = compressBlock(indexData, w.options.CompressionType)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to compress index block: %w", err)
		}

		// 如果压缩后的大小大于原始大小的 90%，不使用压缩
		if float64(len(compressedData)) > float64(len(indexData))*0.9 {
			compressedData = indexData
			compressionType = sstableCompressionNone
		} else {
			compressionType = byte(w.options.CompressionType)
		}
	} else {
		compressedData = indexData
		compressionType = sstableCompressionNone
	}

	// 创建索引块头
	header := blockHeader{
		Type:             sstableBlockTypeIndex,
		CompressionType:  compressionType,
		UncompressedSize: uint32(len(indexData)),
		CompressedSize:   uint32(len(compressedData)),
		EntryCount:       uint32(len(w.indexEntries)),
	}

	// 获取当前位置作为索引块偏移量
	indexOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	// 写入索引块头
	if err := binary.Write(w.writer, binary.LittleEndian, header); err != nil {
		return 0, 0, fmt.Errorf("failed to write index block header: %w", err)
	}

	// 写入压缩后的索引块数据
	if _, err := w.writer.Write(compressedData); err != nil {
		return 0, 0, fmt.Errorf("failed to write index block data: %w", err)
	}

	return indexOffset, binary.Size(header) + len(compressedData), nil
}

// writeFilterBlock 写入过滤器块
func (w *SSTableWriter) writeFilterBlock() (int64, int, error) {
	// 如果不使用布隆过滤器，直接返回
	if !w.options.UseBloomFilter {
		return 0, 0, nil
	}

	// 创建布隆过滤器
	filterSize := (w.keyCount*int64(w.options.BloomFilterBits) + 7) / 8
	if filterSize > int64(w.options.FilterBlockSize) {
		filterSize = int64(w.options.FilterBlockSize)
	}

	filter := make([]byte, filterSize)

	// 计算每个键的哈希并设置相应的位
	for _, entry := range w.indexEntries {
		key := []byte(strconv.FormatInt(entry.Key, 10))
		h1 := fnv1a(key)
		h2 := murmur3(key)

		for i := uint32(0); i < uint32(w.options.BloomFilterBits); i++ {
			hash := (h1 + i*h2) % (uint32(filterSize) * 8)
			byteIndex := hash / 8
			bitOffset := hash % 8

			filter[byteIndex] |= 1 << bitOffset
		}
	}

	// 创建过滤器块
	filterBlock := bytes.NewBuffer(make([]byte, 0, w.options.FilterBlockSize))

	// 写入过滤器类型 (1 = 布隆过滤器)
	if err := binary.Write(filterBlock, binary.LittleEndian, uint32(1)); err != nil {
		return 0, 0, fmt.Errorf("failed to write filter type: %w", err)
	}

	// 写入布隆过滤器参数
	if err := binary.Write(filterBlock, binary.LittleEndian, uint32(w.options.BloomFilterBits)); err != nil {
		return 0, 0, fmt.Errorf("failed to write bloom filter bits: %w", err)
	}

	if err := binary.Write(filterBlock, binary.LittleEndian, uint32(w.options.BloomFilterBits)); err != nil {
		return 0, 0, fmt.Errorf("failed to write bloom filter hashes: %w", err)
	}

	// 写入过滤器大小
	if err := binary.Write(filterBlock, binary.LittleEndian, uint32(len(filter))); err != nil {
		return 0, 0, fmt.Errorf("failed to write filter size: %w", err)
	}

	// 写入过滤器数据
	if _, err := filterBlock.Write(filter); err != nil {
		return 0, 0, fmt.Errorf("failed to write filter data: %w", err)
	}

	// 压缩过滤器块
	filterData := filterBlock.Bytes()
	var compressedData []byte
	var compressionType byte

	if w.options.CompressionType != sstableCompressionNone && len(filterData) > 32 {
		var err error
		compressedData, err = compressBlock(filterData, w.options.CompressionType)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to compress filter block: %w", err)
		}

		// 如果压缩后的大小大于原始大小的 90%，不使用压缩
		if float64(len(compressedData)) > float64(len(filterData))*0.9 {
			compressedData = filterData
			compressionType = sstableCompressionNone
		} else {
			compressionType = byte(w.options.CompressionType)
		}
	} else {
		compressedData = filterData
		compressionType = sstableCompressionNone
	}

	// 创建过滤器块头
	header := blockHeader{
		Type:             sstableBlockTypeFilter,
		CompressionType:  compressionType,
		UncompressedSize: uint32(len(filterData)),
		CompressedSize:   uint32(len(compressedData)),
		EntryCount:       uint32(w.keyCount),
	}

	// 获取当前位置作为过滤器块偏移量
	filterOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	// 写入过滤器块头
	if err := binary.Write(w.writer, binary.LittleEndian, header); err != nil {
		return 0, 0, fmt.Errorf("failed to write filter block header: %w", err)
	}

	// 写入压缩后的过滤器块数据
	if _, err := w.writer.Write(compressedData); err != nil {
		return 0, 0, fmt.Errorf("failed to write filter block data: %w", err)
	}

	return filterOffset, binary.Size(header) + len(compressedData), nil
}

// writeTagMetaBlock 写入标签元数据块
func (w *SSTableWriter) writeTagMetaBlock() (int64, int, error) {
	// 如果没有标签元数据，直接返回
	if len(w.tagMeta) == 0 {
		return 0, 0, nil
	}

	// 创建标签元数据块
	metaBlock := bytes.NewBuffer(make([]byte, 0, w.options.MetaBlockSize))

	// 写入标签键数量
	tagKeyCount := uint32(len(w.tagMeta))
	if err := binary.Write(metaBlock, binary.LittleEndian, tagKeyCount); err != nil {
		return 0, 0, fmt.Errorf("failed to write tag key count: %w", err)
	}

	// 写入每个标签键及其值
	for tagKey, valueMap := range w.tagMeta {
		// 写入标签键长度
		if err := binary.Write(metaBlock, binary.LittleEndian, uint16(len(tagKey))); err != nil {
			return 0, 0, fmt.Errorf("failed to write tag key length: %w", err)
		}

		// 写入标签键
		if _, err := metaBlock.WriteString(tagKey); err != nil {
			return 0, 0, fmt.Errorf("failed to write tag key: %w", err)
		}

		// 写入标签值数量
		if err := binary.Write(metaBlock, binary.LittleEndian, uint32(len(valueMap))); err != nil {
			return 0, 0, fmt.Errorf("failed to write tag value count: %w", err)
		}

		// 写入每个标签值及其时间戳列表
		for tagValue, timestamps := range valueMap {
			// 写入标签值长度
			if err := binary.Write(metaBlock, binary.LittleEndian, uint16(len(tagValue))); err != nil {
				return 0, 0, fmt.Errorf("failed to write tag value length: %w", err)
			}

			// 写入标签值
			if _, err := metaBlock.WriteString(tagValue); err != nil {
				return 0, 0, fmt.Errorf("failed to write tag value: %w", err)
			}

			// 写入时间戳数量
			if err := binary.Write(metaBlock, binary.LittleEndian, uint32(len(timestamps))); err != nil {
				return 0, 0, fmt.Errorf("failed to write timestamp count: %w", err)
			}

			// 写入时间戳列表
			for _, ts := range timestamps {
				if err := binary.Write(metaBlock, binary.LittleEndian, ts); err != nil {
					return 0, 0, fmt.Errorf("failed to write timestamp: %w", err)
				}
			}
		}
	}

	// 压缩标签元数据块
	metaData := metaBlock.Bytes()
	var compressedData []byte
	var compressionType byte

	if w.options.CompressionType != sstableCompressionNone && len(metaData) > 32 {
		var err error
		compressedData, err = compressBlock(metaData, w.options.CompressionType)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to compress tag meta block: %w", err)
		}

		// 如果压缩后的大小大于原始大小的 90%，不使用压缩
		if float64(len(compressedData)) > float64(len(metaData))*0.9 {
			compressedData = metaData
			compressionType = sstableCompressionNone
		} else {
			compressionType = byte(w.options.CompressionType)
		}
	} else {
		compressedData = metaData
		compressionType = sstableCompressionNone
	}

	// 创建标签元数据块头
	header := blockHeader{
		Type:             sstableBlockTypeTagMeta,
		CompressionType:  compressionType,
		UncompressedSize: uint32(len(metaData)),
		CompressedSize:   uint32(len(compressedData)),
		EntryCount:       tagKeyCount,
	}

	// 获取当前位置作为标签元数据块偏移量
	metaOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	// 写入标签元数据块头
	if err := binary.Write(w.writer, binary.LittleEndian, header); err != nil {
		return 0, 0, fmt.Errorf("failed to write tag meta block header: %w", err)
	}

	// 写入压缩后的标签元数据块数据
	if _, err := w.writer.Write(compressedData); err != nil {
		return 0, 0, fmt.Errorf("failed to write tag meta block data: %w", err)
	}

	return metaOffset, binary.Size(header) + len(compressedData), nil
}

// writeFooter 写入页脚
func (w *SSTableWriter) writeFooter(indexOffset, indexSize int64, filterOffset, filterSize int64, metaOffset, metaSize int64) error {
	// 创建页脚
	footer := make([]byte, sstableFooterSize)

	// 写入索引偏移量和大小
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(indexSize))

	// 写入过滤器偏移量和大小
	binary.LittleEndian.PutUint64(footer[12:20], uint64(filterOffset))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(filterSize))

	// 写入标签元数据偏移量和大小
	binary.LittleEndian.PutUint64(footer[24:32], uint64(metaOffset))
	binary.LittleEndian.PutUint32(footer[32:36], uint32(metaSize))

	// 写入格式版本
	binary.LittleEndian.PutUint32(footer[36:40], uint32(sstableFormatVersion))

	// 写入魔数
	binary.LittleEndian.PutUint32(footer[40:44], sstableMagicNumber)

	// 写入校验和
	checksum := calculateCRC32(footer[:44])
	binary.LittleEndian.PutUint32(footer[44:48], checksum)

	// 写入页脚
	if _, err := w.writer.Write(footer); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	return nil
}

// Finish 完成 SSTable 写入
func (w *SSTableWriter) Finish() (*SSTableInfo, error) {
	// 刷新当前块
	if err := w.flushCurrentBlock(); err != nil {
		return nil, fmt.Errorf("failed to flush current block: %w", err)
	}

	// 写入索引块
	indexOffset, indexSize, err := w.writeIndexBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to write index block: %w", err)
	}

	// 写入过滤器块
	filterOffset, filterSize, err := w.writeFilterBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to write filter block: %w", err)
	}

	// 写入标签元数据块
	metaOffset, metaSize, err := w.writeTagMetaBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to write tag meta block: %w", err)
	}

	// 写入页脚
	if err := w.writeFooter(indexOffset, int64(indexSize), filterOffset, int64(filterSize), metaOffset, int64(metaSize)); err != nil {
		return nil, fmt.Errorf("failed to write footer: %w", err)
	}

	// 同步到磁盘
	if w.useZeroCopy && w.mmapFile != nil {
		// 使用内存映射同步
		if err := w.mmapFile.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync mmap file: %w", err)
		}

		// 获取文件大小
		fileSize := w.mmapFile.Size()

		// 获取文件名
		fileName := filepath.Base(w.mmapFile.File().Name())

		// 关闭内存映射文件
		if err := w.mmapFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close mmap file: %w", err)
		}

		// 提取标签元数据
		tagMeta := make([]TagMeta, 0, len(w.tagMeta))
		for tagKey, valueMap := range w.tagMeta {
			values := make([]string, 0, len(valueMap))
			for value := range valueMap {
				values = append(values, value)
			}

			tagMeta = append(tagMeta, TagMeta{
				Key:    tagKey,
				Values: values,
			})
		}

		// 创建 SSTable 信息
		return &SSTableInfo{
			FileName:    fileName,
			DBName:      w.dbName,
			TableName:   w.tableName,
			Level:       w.level,
			TimeRange:   TimeRange{Start: w.minKey, End: w.maxKey},
			Size:        fileSize,
			KeyCount:    int(w.keyCount),
			CreatedAt:   time.Now(),
			MinKey:      w.minKey,
			MaxKey:      w.maxKey,
			IndexOffset: indexOffset,
			IndexSize:   int(indexSize),
			HasFilter:   w.options.UseBloomFilter,
			TagMeta:     tagMeta,
		}, nil
	} else {
		// 使用标准文件操作
		if err := w.writer.Flush(); err != nil {
			return nil, fmt.Errorf("failed to flush writer: %w", err)
		}

		if err := w.file.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync file: %w", err)
		}

		// 获取文件大小
		fileInfo, err := w.file.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to get file info: %w", err)
		}

		// 提取标签元数据
		tagMeta := make([]TagMeta, 0, len(w.tagMeta))
		for tagKey, valueMap := range w.tagMeta {
			values := make([]string, 0, len(valueMap))
			for value := range valueMap {
				values = append(values, value)
			}

			tagMeta = append(tagMeta, TagMeta{
				Key:    tagKey,
				Values: values,
			})
		}

		// 创建 SSTable 信息
		return &SSTableInfo{
			FileName:    filepath.Base(w.file.Name()),
			DBName:      w.dbName,
			TableName:   w.tableName,
			Level:       w.level,
			TimeRange:   TimeRange{Start: w.minKey, End: w.maxKey},
			Size:        fileInfo.Size(),
			KeyCount:    int(w.keyCount),
			CreatedAt:   time.Now(),
			MinKey:      w.minKey,
			MaxKey:      w.maxKey,
			IndexOffset: indexOffset,
			IndexSize:   int(indexSize),
			HasFilter:   w.options.UseBloomFilter,
			TagMeta:     tagMeta,
		}, nil
	}
}

// Close 关闭 SSTable 写入器
func (w *SSTableWriter) Close() error {
	if w.useZeroCopy && w.mmapFile != nil {
		return w.mmapFile.Close()
	} else if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// NewSSTableCompactor 创建新的 SSTable 压缩器
func NewSSTableCompactor(manager *SSTableManager, workers int) *SSTableCompactor {
	return &SSTableCompactor{
		manager:        manager,
		compactChan:    make(chan compactTask, 100),
		closing:        make(chan struct{}),
		compactWorkers: workers,
	}
}

// Start 启动压缩工作线程
func (c *SSTableCompactor) Start() {
	for i := 0; i < c.compactWorkers; i++ {
		c.wg.Add(1)
		go c.compactWorker(i)
	}
}

// Stop 停止压缩工作线程
func (c *SSTableCompactor) Stop() {
	close(c.closing)
	c.wg.Wait()
}

// compactWorker 压缩工作线程
func (c *SSTableCompactor) compactWorker(id int) {
	defer c.wg.Done()

	logger.Printf("INFO: SSTable compactor worker %d started", id)

	for {
		select {
		case <-c.closing:
			logger.Printf("INFO: SSTable compactor worker %d stopped", id)
			return
		case task := <-c.compactChan:
			logger.Printf("INFO: SSTable compactor worker %d processing task for level %d with %d files", id, task.level, len(task.files))

			// 执行压缩
			if err := c.manager.CompactLevel(task.level); err != nil {
				logger.Printf("ERROR: Failed to compact level %d: %v", task.level, err)
			}
		}
	}
}

// parseFooter 解析页脚
func parseFooter(data []byte) struct {
	IndexOffset   uint64
	IndexSize     uint32
	FilterOffset  uint64
	FilterSize    uint32
	MetaOffset    uint64
	MetaSize      uint32
	FormatVersion uint32
	MagicNumber   uint32
	Checksum      uint32
} {
	return struct {
		IndexOffset   uint64
		IndexSize     uint32
		FilterOffset  uint64
		FilterSize    uint32
		MetaOffset    uint64
		MetaSize      uint32
		FormatVersion uint32
		MagicNumber   uint32
		Checksum      uint32
	}{
		IndexOffset:   binary.LittleEndian.Uint64(data[0:8]),
		IndexSize:     binary.LittleEndian.Uint32(data[8:12]),
		FilterOffset:  binary.LittleEndian.Uint64(data[12:20]),
		FilterSize:    binary.LittleEndian.Uint32(data[20:24]),
		MetaOffset:    binary.LittleEndian.Uint64(data[24:32]),
		MetaSize:      binary.LittleEndian.Uint32(data[32:36]),
		FormatVersion: binary.LittleEndian.Uint32(data[36:40]),
		MagicNumber:   binary.LittleEndian.Uint32(data[40:44]),
		Checksum:      binary.LittleEndian.Uint32(data[44:48]),
	}
}

// compressBlock 压缩数据块
func compressBlock(data []byte, compressionType int) ([]byte, error) {
	switch compressionType {
	case sstableCompressionNone:
		return data, nil
	case sstableCompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case sstableCompressionSnappy:
		// 这里应该使用 Snappy 压缩，但为了简化，我们使用 Gzip
		// 在实际实现中，应该使用 github.com/golang/snappy
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// decompressBlock 解压缩数据块
func decompressBlock(data []byte, compressionType byte, uncompressedSize int) ([]byte, error) {
	switch compressionType {
	case sstableCompressionNone:
		return data, nil
	case sstableCompressionGzip:
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()

		result := make([]byte, uncompressedSize)
		if _, err := io.ReadFull(r, result); err != nil {
			return nil, err
		}
		return result, nil
	case sstableCompressionSnappy:
		// 这里应该使用 Snappy 解压缩，但为了简化，我们使用 Gzip
		// 在实际实现中，应该使用 github.com/golang/snappy
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()

		result := make([]byte, uncompressedSize)
		if _, err := io.ReadFull(r, result); err != nil {
			return nil, err
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// fnv1a 计算 FNV-1a 哈希
func fnv1a(data []byte) uint32 {
	var hash uint32 = 2166136261
	for _, b := range data {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash
}

// murmur3 计算 Murmur3 哈希
func murmur3(data []byte) uint32 {
	var c1 uint32 = 0xcc9e2d51
	var c2 uint32 = 0x1b873593
	var r1 uint32 = 15
	var r2 uint32 = 13
	var m uint32 = 5
	var n uint32 = 0xe6546b64

	var hash uint32 = 0

	for i := 0; i < len(data)-3; i += 4 {
		k := binary.LittleEndian.Uint32(data[i : i+4])

		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2

		hash ^= k
		hash = (hash << r2) | (hash >> (32 - r2))
		hash = hash*m + n
	}

	// 处理剩余字节
	tail := data[len(data)-(len(data)%4):]
	var k1 uint32

	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << r1) | (k1 >> (32 - r1))
		k1 *= c2
		hash ^= k1
	}

	// 最终混合
	hash ^= uint32(len(data))
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16

	return hash
}

// calculateCRC32 计算 CRC32 校验和
func calculateCRC32(data []byte) uint32 {
	var crc uint32 = 0xffffffff
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ 0xedb88320
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}

// SSTableMerger 用于合并多个 SSTable 文件
type SSTableMerger struct {
	dataDir   string
	options   *SSTableOptions
	files     []string
	infos     []*SSTableInfo
	dbName    string
	tableName string
}

// NewSSTableMerger 创建新的 SSTable 合并器
func NewSSTableMerger(dataDir string, options *SSTableOptions) *SSTableMerger {
	return &SSTableMerger{
		dataDir: dataDir,
		options: options,
		files:   make([]string, 0),
		infos:   make([]*SSTableInfo, 0),
	}
}

// AddFile 添加要合并的 SSTable 文件
func (m *SSTableMerger) AddFile(filePath string, info *SSTableInfo) error {
	m.files = append(m.files, filePath)
	m.infos = append(m.infos, info)

	// 设置数据库和表名
	if m.dbName == "" {
		m.dbName = info.DBName
	}
	if m.tableName == "" {
		m.tableName = info.TableName
	}

	return nil
}

// Merge 执行合并操作
func (m *SSTableMerger) Merge(level int) ([]string, error) {
	if len(m.files) == 0 {
		return nil, nil
	}

	// 创建合并后的文件名
	fileID := time.Now().UnixNano()
	fileName := fmt.Sprintf(sstableFileFormat, m.dbName, m.tableName, level, fileID, sstableFileExt)
	filePath := filepath.Join(m.dataDir, fileName)

	// 创建 SSTable 写入器
	writer, err := NewSSTableWriter(filePath, m.dbName, m.tableName, level, m.options)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable writer: %w", err)
	}
	defer writer.Close()

	// 创建多路归并迭代器
	iterators := make([]*SSTableIterator, 0, len(m.files))
	for i, file := range m.files {
		reader, err := NewSSTableReader(file, m.infos[i], m.options)
		if err != nil {
			// 关闭已打开的迭代器
			for _, iter := range iterators {
				iter.Close()
			}
			return nil, fmt.Errorf("failed to create SSTable reader for %s: %w", file, err)
		}

		iter := NewSSTableIterator(reader)
		iterators = append(iterators, iter)
	}

	// 创建合并迭代器
	merger := NewMergeIterator(iterators)

	// 写入所有数据点
	for merger.SeekToFirst(); merger.Valid(); merger.Next() {
		key := merger.Key()
		value := merger.Value()

		// 解析 BSON 数据
		var doc bson.D
		if err := bson.Unmarshal(value, &doc); err != nil {
			logger.Printf("Failed to unmarshal BSON data: %v\n", err)
			continue
		}

		// 提取标签
		tags := make(map[string]string)
		for _, elem := range doc {
			if elem.Key == "tags" {
				if tagsMap, ok := elem.Value.(bson.D); ok {
					for _, tag := range tagsMap {
						if tagValue, ok := tag.Value.(string); ok {
							tags[tag.Key] = tagValue
						}
					}
				}
			}
		}

		// 写入数据点
		if err := writer.Add(key, value, tags); err != nil {
			// 关闭迭代器
			merger.Close()
			for _, iter := range iterators {
				iter.Close()
			}
			return nil, fmt.Errorf("failed to add data point to SSTable: %w", err)
		}
	}

	// 关闭迭代器
	merger.Close()
	for _, iter := range iterators {
		iter.Close()
	}

	// 完成写入
	_, err = writer.Finish()
	if err != nil {
		return nil, fmt.Errorf("failed to finish SSTable writing: %w", err)
	}

	return []string{filePath}, nil
}

// SSTableIterator 用于遍历 SSTable 文件
type SSTableIterator struct {
	reader       *SSTableReader
	currentBlock []byte
	blockOffset  int
	currentKey   int64
	currentValue []byte
	valid        bool
	err          error
}

// NewSSTableIterator 创建新的 SSTable 迭代器
func NewSSTableIterator(reader *SSTableReader) *SSTableIterator {
	return &SSTableIterator{
		reader: reader,
		valid:  false,
	}
}

// SeekToFirst 定位到第一个数据点
func (it *SSTableIterator) SeekToFirst() {
	// 查找第一个数据块
	blockOffset, blockSize, err := it.reader.findFirstBlock()
	if err != nil {
		it.err = err
		it.valid = false
		return
	}

	// 读取数据块
	blockData, err := it.reader.readBlock(blockOffset, blockSize)
	if err != nil {
		it.err = err
		it.valid = false
		return
	}

	// 解析块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(blockData) < headerSize {
		it.err = fmt.Errorf("block too small")
		it.valid = false
		return
	}

	// 解析头部
	headerBuf := bytes.NewReader(blockData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		it.err = fmt.Errorf("failed to parse block header: %w", err)
		it.valid = false
		return
	}

	// 检查块类型
	if header.Type != sstableBlockTypeData {
		it.err = fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeData, header.Type)
		it.valid = false
		return
	}

	// 解压缩数据
	var dataBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		dataBytes, err = decompressBlock(blockData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			it.err = fmt.Errorf("failed to decompress data block: %w", err)
			it.valid = false
			return
		}
	} else {
		dataBytes = blockData[headerSize:]
	}

	// 解析条目数量
	if len(dataBytes) < 4 {
		it.err = fmt.Errorf("invalid data block format")
		it.valid = false
		return
	}
	entryCount := int(binary.LittleEndian.Uint32(dataBytes[:4]))

	// 如果没有条目，迭代器无效
	if entryCount == 0 {
		it.valid = false
		return
	}

	// 保存当前块
	it.currentBlock = dataBytes
	it.blockOffset = 4 // 跳过条目数量

	// 读取第一个条目
	it.readEntry()
}

// Seek 定位到指定时间戳
func (it *SSTableIterator) Seek(timestamp int64) {
	// 查找包含指定时间戳的数据块
	blockOffset, blockSize, err := it.reader.findBlock(timestamp)
	if err != nil {
		it.err = err
		it.valid = false
		return
	}

	// 读取数据块
	blockData, err := it.reader.readBlock(blockOffset, blockSize)
	if err != nil {
		it.err = err
		it.valid = false
		return
	}

	// 解析块头
	var header blockHeader
	headerSize := binary.Size(header)
	if len(blockData) < headerSize {
		it.err = fmt.Errorf("block too small")
		it.valid = false
		return
	}

	// 解析头部
	headerBuf := bytes.NewReader(blockData[:headerSize])
	if err := binary.Read(headerBuf, binary.LittleEndian, &header); err != nil {
		it.err = fmt.Errorf("failed to parse block header: %w", err)
		it.valid = false
		return
	}

	// 检查块类型
	if header.Type != sstableBlockTypeData {
		it.err = fmt.Errorf("invalid block type: expected %d, got %d", sstableBlockTypeData, header.Type)
		it.valid = false
		return
	}

	// 解压缩数据
	var dataBytes []byte
	if header.CompressionType != sstableCompressionNone {
		// 解压缩
		var err error
		dataBytes, err = decompressBlock(blockData[headerSize:], header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			it.err = fmt.Errorf("failed to decompress data block: %w", err)
			it.valid = false
			return
		}
	} else {
		dataBytes = blockData[headerSize:]
	}

	// 解析条目数量
	if len(dataBytes) < 4 {
		it.err = fmt.Errorf("invalid data block format")
		it.valid = false
		return
	}
	entryCount := int(binary.LittleEndian.Uint32(dataBytes[:4]))

	// 保存当前块
	it.currentBlock = dataBytes
	it.blockOffset = 4 // 跳过条目数量

	// 查找第一个大于等于指定时间戳的条目
	for i := 0; i < entryCount; i++ {
		// 检查边界
		if it.blockOffset+8 > len(it.currentBlock) {
			it.valid = false
			return
		}

		// 解析键
		key := int64(binary.LittleEndian.Uint64(it.currentBlock[it.blockOffset : it.blockOffset+8]))

		// 如果找到大于等于指定时间戳的条目，读取它
		if key >= timestamp {
			it.readEntry()
			return
		}

		// 否则，跳过当前条目
		it.blockOffset += 8

		if it.blockOffset+4 > len(it.currentBlock) {
			it.valid = false
			return
		}

		valueSize := int(binary.LittleEndian.Uint32(it.currentBlock[it.blockOffset : it.blockOffset+4]))
		it.blockOffset += 4 + valueSize
	}

	// 如果没有找到，迭代器无效
	it.valid = false
}

// Next 移动到下一个数据点
func (it *SSTableIterator) Next() {
	// 如果当前迭代器无效，直接返回
	if !it.valid {
		return
	}

	// 移动到下一个条目
	it.readEntry()
}

// readEntry 读取当前条目
func (it *SSTableIterator) readEntry() {
	// 检查边界
	if it.blockOffset+8 > len(it.currentBlock) {
		it.valid = false
		return
	}

	// 解析键
	it.currentKey = int64(binary.LittleEndian.Uint64(it.currentBlock[it.blockOffset : it.blockOffset+8]))
	it.blockOffset += 8

	if it.blockOffset+4 > len(it.currentBlock) {
		it.valid = false
		return
	}

	// 解析值大小
	valueSize := int(binary.LittleEndian.Uint32(it.currentBlock[it.blockOffset : it.blockOffset+4]))
	it.blockOffset += 4

	if it.blockOffset+valueSize > len(it.currentBlock) {
		it.valid = false
		return
	}

	// 读取值
	it.currentValue = make([]byte, valueSize)
	copy(it.currentValue, it.currentBlock[it.blockOffset:it.blockOffset+valueSize])
	it.blockOffset += valueSize

	it.valid = true
}

// Valid 检查迭代器是否有效
func (it *SSTableIterator) Valid() bool {
	return it.valid
}

// Key 获取当前键
func (it *SSTableIterator) Key() int64 {
	if !it.valid {
		return 0
	}
	return it.currentKey
}

// Value 获取当前值
func (it *SSTableIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.currentValue
}

// Error 获取错误
func (it *SSTableIterator) Error() error {
	return it.err
}

// Close 关闭迭代器
func (it *SSTableIterator) Close() error {
	if it.reader != nil {
		err := it.reader.Close()
		it.reader = nil
		return err
	}
	return nil
}

// MergeIterator 用于合并多个迭代器
type MergeIterator struct {
	iterators []*SSTableIterator
	current   int
	valid     bool
}

// NewMergeIterator 创建新的合并迭代器
func NewMergeIterator(iterators []*SSTableIterator) *MergeIterator {
	return &MergeIterator{
		iterators: iterators,
		current:   -1,
		valid:     false,
	}
}

// SeekToFirst 定位到第一个数据点
func (it *MergeIterator) SeekToFirst() {
	// 初始化所有迭代器
	for _, iter := range it.iterators {
		iter.SeekToFirst()
	}

	// 查找最小的键
	it.findSmallest()
}

// Seek 定位到指定时间戳
func (it *MergeIterator) Seek(timestamp int64) {
	// 初始化所有迭代器
	for _, iter := range it.iterators {
		iter.Seek(timestamp)
	}

	// 查找最小的键
	it.findSmallest()
}

// Next 移动到下一个数据点
func (it *MergeIterator) Next() {
	// 如果当前迭代器无效，直接返回
	if !it.valid {
		return
	}

	// 移动当前迭代器
	it.iterators[it.current].Next()

	// 查找最小的键
	it.findSmallest()
}

// findSmallest 查找最小的键
func (it *MergeIterator) findSmallest() {
	it.current = -1
	it.valid = false

	var smallestKey int64 = math.MaxInt64

	for i, iter := range it.iterators {
		if iter.Valid() {
			key := iter.Key()
			if key < smallestKey {
				smallestKey = key
				it.current = i
				it.valid = true
			}
		}
	}
}

// Valid 检查迭代器是否有效
func (it *MergeIterator) Valid() bool {
	return it.valid
}

// Key 获取当前键
func (it *MergeIterator) Key() int64 {
	if !it.valid {
		return 0
	}
	return it.iterators[it.current].Key()
}

// Value 获取当前值
func (it *MergeIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.iterators[it.current].Value()
}

// Close 关闭迭代器
func (it *MergeIterator) Close() error {
	var lastErr error
	for _, iter := range it.iterators {
		if err := iter.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
