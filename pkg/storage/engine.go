package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/index"
	"ChronoGo/pkg/model"
)

// StorageEngine 表示时序存储引擎
type StorageEngine struct {
	dataDir     string                     // 数据目录
	walDir      string                     // WAL目录
	metaDir     string                     // 元数据目录
	databases   map[string]*model.Database // 数据库映射
	memTables   map[string]*MemTable       // 内存表映射 (db:table -> memtable)
	wal         *WAL                       // 预写日志
	indexMgr    index.IndexManager         // 索引管理器
	flushTicker *time.Ticker               // 刷盘定时器
	mu          sync.RWMutex               // 读写锁
	closing     chan struct{}              // 关闭信号
}

// QueryOptimizer 查询优化器
type QueryOptimizer struct {
	engine *StorageEngine
}

// TimeRange 表示时间范围
type TimeRange struct {
	Start int64
	End   int64
}

// NewStorageEngine 创建新的存储引擎
func NewStorageEngine(baseDir string) (*StorageEngine, error) {
	// 创建目录结构
	dataDir := filepath.Join(baseDir, "data")
	walDir := filepath.Join(baseDir, "wal")
	metaDir := filepath.Join(baseDir, "meta")

	for _, dir := range []string{dataDir, walDir, metaDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// 创建WAL
	wal, err := NewWAL(walDir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// 创建索引管理器
	indexMgr := index.NewIndexManager()

	engine := &StorageEngine{
		dataDir:   dataDir,
		walDir:    walDir,
		metaDir:   metaDir,
		databases: make(map[string]*model.Database),
		memTables: make(map[string]*MemTable),
		wal:       wal,
		indexMgr:  indexMgr,
		closing:   make(chan struct{}),
	}

	// 加载元数据
	if err := engine.loadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	// 从WAL恢复数据
	if err := engine.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// 启动后台刷盘任务
	engine.startBackgroundTasks()

	return engine, nil
}

// NewQueryOptimizer 创建查询优化器
func NewQueryOptimizer(engine *StorageEngine) *QueryOptimizer {
	return &QueryOptimizer{
		engine: engine,
	}
}

// loadMetadata 加载元数据
func (e *StorageEngine) loadMetadata() error {
	metadataPath := filepath.Join(e.dataDir, "metadata.json")

	// 检查元数据文件是否存在
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// 元数据文件不存在，初始化空数据库
		e.databases = make(map[string]*model.Database)
		return nil
	}

	// 读取元数据文件
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// 解析元数据
	var metadata struct {
		Databases map[string]*model.Database `json:"databases"`
	}

	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// 设置数据库
	e.databases = metadata.Databases

	// 重新创建索引
	ctx := context.Background()
	for dbName, db := range e.databases {
		for tableName, table := range db.Tables {
			for _, tagIndex := range table.TagIndexes {
				// 转换索引类型
				var idxType index.IndexType
				switch tagIndex.Type {
				case "inverted":
					idxType = index.IndexTypeInverted
				case "btree":
					idxType = index.IndexTypeBTree
				case "bitmap":
					idxType = index.IndexTypeBitmap
				case "hash":
					idxType = index.IndexTypeHash
				case "composite":
					idxType = index.IndexTypeComposite
				default:
					// 自动选择索引类型
					idxType = 0
				}

				// 创建索引选项
				options := index.IndexOptions{
					Type:                idxType,
					Name:                tagIndex.Name,
					Unique:              tagIndex.Unique,
					Fields:              tagIndex.Fields,
					CardinalityEstimate: tagIndex.CardinalityEstimate,
					SupportRange:        tagIndex.SupportRange,
					SupportPrefix:       tagIndex.SupportPrefix,
					SupportRegex:        tagIndex.SupportRegex,
				}

				// 创建索引
				idx, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
				if err != nil {
					return fmt.Errorf("failed to recreate index %s: %w", tagIndex.Name, err)
				}

				// 从现有数据填充索引
				if err := e.fillIndexFromExistingData(ctx, dbName, tableName, idx, tagIndex.Fields); err != nil {
					return fmt.Errorf("failed to fill index %s from existing data: %w", tagIndex.Name, err)
				}
			}
		}
	}

	return nil
}

// saveMetadata 保存元数据
func (e *StorageEngine) saveMetadata() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 准备元数据
	metadata := struct {
		Databases map[string]*model.Database `json:"databases"`
	}{
		Databases: e.databases,
	}

	// 序列化元数据
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// 写入元数据文件
	metadataPath := filepath.Join(e.dataDir, "metadata.json")
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// recoverFromWAL 从WAL恢复数据
func (e *StorageEngine) recoverFromWAL() error {
	entries, err := e.wal.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover WAL entries: %w", err)
	}

	for _, entry := range entries {
		switch entry.Type {
		case walOpInsert:
			var doc bson.D
			if err := bson.Unmarshal(entry.Data, &doc); err != nil {
				return fmt.Errorf("failed to unmarshal WAL entry: %w", err)
			}

			point, err := model.FromBSON(doc)
			if err != nil {
				return fmt.Errorf("failed to convert BSON to TimeSeriesPoint: %w", err)
			}

			// 获取或创建内存表
			memTable, err := e.getOrCreateMemTable(entry.Database, entry.Table)
			if err != nil {
				return fmt.Errorf("failed to get or create memtable: %w", err)
			}

			// 插入数据点
			if err := memTable.Put(point); err != nil {
				return fmt.Errorf("failed to insert data point: %w", err)
			}

		case walOpDelete:
			// TODO: 实现删除操作恢复
		}
	}

	return nil
}

// startBackgroundTasks 启动后台任务
func (e *StorageEngine) startBackgroundTasks() {
	e.flushTicker = time.NewTicker(1 * time.Minute)

	go func() {
		for {
			select {
			case <-e.flushTicker.C:
				e.flushMemTables()
			case <-e.closing:
				return
			}
		}
	}()
}

// flushMemTables 刷新内存表到磁盘
func (e *StorageEngine) flushMemTables() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, memTable := range e.memTables {
		if memTable.ShouldFlush() {
			// TODO: 实现将内存表刷新到磁盘的逻辑
			memTable.Clear()
		}
	}
}

// getOrCreateMemTable 获取或创建内存表
func (e *StorageEngine) getOrCreateMemTable(dbName, tableName string) (*MemTable, error) {
	key := dbName + ":" + tableName

	e.mu.Lock()
	defer e.mu.Unlock()

	if memTable, exists := e.memTables[key]; exists {
		return memTable, nil
	}

	// 创建新的内存表
	memTable := NewMemTable(64 * 1024 * 1024) // 64MB
	e.memTables[key] = memTable

	return memTable, nil
}

// CreateDatabase 创建数据库
func (e *StorageEngine) CreateDatabase(name string, retentionPolicy model.RetentionPolicy) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.databases[name]; exists {
		return fmt.Errorf("database %s already exists", name)
	}

	// 创建数据库目录
	dbDir := filepath.Join(e.dataDir, name)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// 创建数据库对象
	db := &model.Database{
		Name:            name,
		RetentionPolicy: retentionPolicy,
		Tables:          make(map[string]*model.Table),
	}

	e.databases[name] = db

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// CreateTable 创建表
func (e *StorageEngine) CreateTable(dbName, tableName string, schema model.Schema, tagIndexes []model.TagIndex) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	db, exists := e.databases[dbName]
	if !exists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	if _, exists := db.Tables[tableName]; exists {
		return fmt.Errorf("table %s already exists in database %s", tableName, dbName)
	}

	// 创建表目录
	tableDir := filepath.Join(e.dataDir, dbName, tableName)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// 创建表对象
	table := &model.Table{
		Name:       tableName,
		Database:   dbName,
		Schema:     schema,
		TagIndexes: tagIndexes,
	}

	db.Tables[tableName] = table

	// 创建索引
	ctx := context.Background()
	for _, tagIndex := range tagIndexes {
		// 转换索引类型
		var idxType index.IndexType
		switch tagIndex.Type {
		case "inverted":
			idxType = index.IndexTypeInverted
		case "btree":
			idxType = index.IndexTypeBTree
		case "bitmap":
			idxType = index.IndexTypeBitmap
		case "hash":
			idxType = index.IndexTypeHash
		case "composite":
			idxType = index.IndexTypeComposite
		default:
			// 自动选择索引类型
			idxType = 0
		}

		// 创建索引选项
		options := index.IndexOptions{
			Type:                idxType,
			Name:                tagIndex.Name,
			Unique:              tagIndex.Unique,
			Fields:              tagIndex.Fields,
			CardinalityEstimate: tagIndex.CardinalityEstimate,
			SupportRange:        tagIndex.SupportRange,
			SupportPrefix:       tagIndex.SupportPrefix,
			SupportRegex:        tagIndex.SupportRegex,
		}

		// 创建索引
		_, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
		if err != nil {
			return fmt.Errorf("failed to create index %s: %w", tagIndex.Name, err)
		}
	}

	// 创建时间索引
	if schema.TimeField != "" {
		timeIndexOptions := index.TimeIndexOptions{
			Name:      "time_idx",
			TimeField: schema.TimeField,
			Level:     index.TimeIndexLevelSecond,
			TimeUnit:  "ns", // 默认使用纳秒
		}
		// 创建时间索引实例
		_ = index.NewTimeIndex(timeIndexOptions)

		// 注册时间索引
		options := index.IndexOptions{
			Type:   index.IndexTypeBTree,
			Name:   "time_idx",
			Fields: []string{schema.TimeField},
		}
		_, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
		if err != nil {
			return fmt.Errorf("failed to create time index: %w", err)
		}
	}

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// InsertPoint 插入时序数据点
func (e *StorageEngine) InsertPoint(dbName, tableName string, point *model.TimeSeriesPoint) error {
	// 检查数据库和表是否存在
	e.mu.RLock()
	db, dbExists := e.databases[dbName]
	if !dbExists {
		e.mu.RUnlock()
		return fmt.Errorf("database %s does not exist", dbName)
	}

	table, tableExists := db.Tables[tableName]
	if !tableExists {
		e.mu.RUnlock()
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}
	e.mu.RUnlock()

	// 创建WAL条目
	doc := point.ToBSON()
	entry, err := CreateInsertEntry(dbName, tableName, point.Timestamp, doc)
	if err != nil {
		return fmt.Errorf("failed to create WAL entry: %w", err)
	}

	// 写入WAL
	if err := e.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// 获取内存表
	memTable, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get memtable: %w", err)
	}

	// 插入数据点
	if err := memTable.Put(point); err != nil {
		return fmt.Errorf("failed to insert data point: %w", err)
	}

	// 更新索引
	ctx := context.Background()

	// 生成唯一的时间序列ID
	seriesID := fmt.Sprintf("%s:%s:%d", dbName, tableName, point.Timestamp)

	// 更新标签索引
	for _, tagIndex := range table.TagIndexes {
		// 获取索引
		idx, err := e.indexMgr.GetIndex(ctx, dbName, tableName, tagIndex.Name)
		if err != nil {
			// 索引不存在，跳过
			continue
		}

		// 根据索引字段数量决定如何更新索引
		if len(tagIndex.Fields) == 1 {
			// 单字段索引
			field := tagIndex.Fields[0]
			if value, ok := point.Tags[field]; ok {
				// 更新索引
				if err := idx.Insert(ctx, value, seriesID); err != nil {
					return fmt.Errorf("failed to update index %s: %w", tagIndex.Name, err)
				}
			}
		} else {
			// 复合索引
			values := make([]string, len(tagIndex.Fields))
			for i, field := range tagIndex.Fields {
				if value, ok := point.Tags[field]; ok {
					values[i] = value
				} else {
					values[i] = "" // 字段不存在，使用空字符串
				}
			}
			// 更新索引
			if err := idx.Insert(ctx, values, seriesID); err != nil {
				return fmt.Errorf("failed to update index %s: %w", tagIndex.Name, err)
			}
		}
	}

	// 更新时间索引
	if table.Schema.TimeField != "" {
		// 获取时间索引
		timeIdx, err := e.indexMgr.GetIndex(ctx, dbName, tableName, "time_idx")
		if err == nil {
			// 更新时间索引
			if err := timeIdx.Insert(ctx, point.Timestamp, seriesID); err != nil {
				return fmt.Errorf("failed to update time index: %w", err)
			}
		}
	}

	return nil
}

// QueryByTimeRange 按时间范围查询
func (e *StorageEngine) QueryByTimeRange(dbName, tableName string, start, end int64) ([]*model.TimeSeriesPoint, error) {
	// 检查数据库和表是否存在
	e.mu.RLock()
	db, dbExists := e.databases[dbName]
	if !dbExists {
		e.mu.RUnlock()
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	_, tableExists := db.Tables[tableName]
	if !tableExists {
		e.mu.RUnlock()
		return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}
	e.mu.RUnlock()

	// 创建查询优化器
	optimizer := NewQueryOptimizer(e)

	// 创建时间范围
	timeRange := &TimeRange{
		Start: start,
		End:   end,
	}

	// 选择最佳索引
	ctx := context.Background()
	indexName, err := optimizer.SelectBestIndex(ctx, dbName, tableName, nil, timeRange)
	if err != nil {
		// 索引选择失败，使用全表扫描
		memTable, err := e.getOrCreateMemTable(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get memtable: %w", err)
		}
		return memTable.QueryByTimeRange(start, end), nil
	}

	// 获取选定的索引
	selectedIndex, err := e.indexMgr.GetIndex(ctx, dbName, tableName, indexName)
	if err != nil {
		// 索引获取失败，使用全表扫描
		memTable, err := e.getOrCreateMemTable(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get memtable: %w", err)
		}
		return memTable.QueryByTimeRange(start, end), nil
	}

	// 使用索引进行范围查询
	var seriesIDs []string
	if indexName == "time_idx" {
		// 时间索引
		seriesIDs, err = selectedIndex.SearchRange(ctx, start, end, true, true)
	} else {
		// 其他索引，可能需要额外的过滤
		// 这里简化处理，实际应该根据索引类型和字段进行适当的查询
		condition := index.IndexCondition{
			Field:    "timestamp",
			Operator: "between",
			Value:    []int64{start, end},
		}
		seriesIDs, err = selectedIndex.Search(ctx, condition)
	}

	if err != nil || len(seriesIDs) == 0 {
		// 索引查询失败或无结果，使用全表扫描
		memTable, err := e.getOrCreateMemTable(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get memtable: %w", err)
		}
		return memTable.QueryByTimeRange(start, end), nil
	}

	// 获取内存表
	memTable, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get memtable: %w", err)
	}

	// 使用索引结果查询
	result := make([]*model.TimeSeriesPoint, 0, len(seriesIDs))
	for _, id := range seriesIDs {
		// 从seriesID中提取时间戳
		parts := strings.Split(id, ":")
		if len(parts) != 3 {
			continue
		}

		// 尝试解析时间戳
		ts, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}

		// 查询内存表
		point, found := memTable.Get(ts)
		if found && point != nil {
			// 验证时间范围
			if point.Timestamp >= start && point.Timestamp <= end {
				result = append(result, point)
			}
		}
	}

	return result, nil
}

// QueryByTag 按标签查询
func (e *StorageEngine) QueryByTag(dbName, tableName, tagKey, tagValue string) ([]*model.TimeSeriesPoint, error) {
	// 检查数据库和表是否存在
	e.mu.RLock()
	db, dbExists := e.databases[dbName]
	if !dbExists {
		e.mu.RUnlock()
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	_, tableExists := db.Tables[tableName]
	if !tableExists {
		e.mu.RUnlock()
		return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}
	e.mu.RUnlock()

	// 创建查询优化器
	optimizer := NewQueryOptimizer(e)

	// 创建查询条件
	filters := map[string]interface{}{
		tagKey: tagValue,
	}

	// 选择最佳索引
	ctx := context.Background()
	indexName, err := optimizer.SelectBestIndex(ctx, dbName, tableName, filters, nil)
	if err != nil {
		// 索引选择失败，使用全表扫描
		memTable, err := e.getOrCreateMemTable(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get memtable: %w", err)
		}
		return memTable.QueryByTag(tagKey, tagValue), nil
	}

	// 获取选定的索引
	selectedIndex, err := e.indexMgr.GetIndex(ctx, dbName, tableName, indexName)
	if err != nil {
		// 索引获取失败，使用全表扫描
		memTable, err := e.getOrCreateMemTable(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get memtable: %w", err)
		}
		return memTable.QueryByTag(tagKey, tagValue), nil
	}

	// 创建查询条件
	condition := index.IndexCondition{
		Field:    tagKey,
		Operator: "=",
		Value:    tagValue,
	}

	// 使用索引进行查询
	seriesIDs, err := selectedIndex.Search(ctx, condition)
	if err != nil || len(seriesIDs) == 0 {
		// 索引查询失败或无结果，使用全表扫描
		memTable, err := e.getOrCreateMemTable(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get memtable: %w", err)
		}
		return memTable.QueryByTag(tagKey, tagValue), nil
	}

	// 获取内存表
	memTable, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get memtable: %w", err)
	}

	// 使用索引结果查询
	result := make([]*model.TimeSeriesPoint, 0, len(seriesIDs))
	for _, id := range seriesIDs {
		// 从seriesID中提取时间戳
		parts := strings.Split(id, ":")
		if len(parts) != 3 {
			continue
		}

		// 尝试解析时间戳
		ts, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}

		// 查询内存表
		point, found := memTable.Get(ts)
		if found && point != nil {
			// 验证标签值
			if value, ok := point.Tags[tagKey]; ok && value == tagValue {
				result = append(result, point)
			}
		}
	}

	return result, nil
}

// Close 关闭存储引擎
func (e *StorageEngine) Close() error {
	// 停止后台任务
	close(e.closing)
	if e.flushTicker != nil {
		e.flushTicker.Stop()
	}

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// 刷新所有内存表
	e.flushMemTables()

	// 关闭WAL
	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// 关闭索引管理器
	if err := e.indexMgr.Close(); err != nil {
		return fmt.Errorf("failed to close index manager: %w", err)
	}

	return nil
}

// ListDatabases 列出所有数据库
func (e *StorageEngine) ListDatabases() ([]*model.Database, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	databases := make([]*model.Database, 0, len(e.databases))
	for _, db := range e.databases {
		databases = append(databases, db)
	}
	return databases, nil
}

// DropDatabase 删除数据库
func (e *StorageEngine) DropDatabase(dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 检查数据库是否存在
	if _, exists := e.databases[dbName]; !exists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	// 删除数据库目录
	dbPath := filepath.Join(e.dataDir, dbName)
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to remove database directory: %w", err)
	}

	// 删除内存中的数据库对象
	delete(e.databases, dbName)

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// DropTable 删除表
func (e *StorageEngine) DropTable(dbName, tableName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 检查数据库是否存在
	db, exists := e.databases[dbName]
	if !exists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	// 检查表是否存在
	if _, exists := db.Tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 删除表目录
	tablePath := filepath.Join(e.dataDir, dbName, tableName)
	if err := os.RemoveAll(tablePath); err != nil {
		return fmt.Errorf("failed to remove table directory: %w", err)
	}

	// 删除内存中的表对象
	delete(db.Tables, tableName)

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// ListTables 列出数据库中的所有表
func (e *StorageEngine) ListTables(dbName string) ([]*model.Table, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 检查数据库是否存在
	db, exists := e.databases[dbName]
	if !exists {
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	tables := make([]*model.Table, 0, len(db.Tables))
	for _, table := range db.Tables {
		tables = append(tables, table)
	}
	return tables, nil
}

// CreateIndex 创建索引
func (e *StorageEngine) CreateIndex(dbName, tableName string, tagIndex model.TagIndex) error {
	// 检查数据库和表是否存在
	e.mu.Lock()
	defer e.mu.Unlock()

	db, dbExists := e.databases[dbName]
	if !dbExists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	table, tableExists := db.Tables[tableName]
	if !tableExists {
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 检查索引是否已存在
	for _, idx := range table.TagIndexes {
		if idx.Name == tagIndex.Name {
			return fmt.Errorf("index %s already exists", tagIndex.Name)
		}
	}

	// 添加到表的索引列表
	table.TagIndexes = append(table.TagIndexes, tagIndex)

	// 转换索引类型
	var idxType index.IndexType
	switch tagIndex.Type {
	case "inverted":
		idxType = index.IndexTypeInverted
	case "btree":
		idxType = index.IndexTypeBTree
	case "bitmap":
		idxType = index.IndexTypeBitmap
	case "hash":
		idxType = index.IndexTypeHash
	case "composite":
		idxType = index.IndexTypeComposite
	default:
		// 自动选择索引类型
		idxType = 0
	}

	// 创建索引选项
	options := index.IndexOptions{
		Type:                idxType,
		Name:                tagIndex.Name,
		Unique:              tagIndex.Unique,
		Fields:              tagIndex.Fields,
		CardinalityEstimate: tagIndex.CardinalityEstimate,
		SupportRange:        tagIndex.SupportRange,
		SupportPrefix:       tagIndex.SupportPrefix,
		SupportRegex:        tagIndex.SupportRegex,
	}

	// 创建索引
	ctx := context.Background()
	idx, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// 解锁以避免死锁，因为fillIndexFromExistingData会获取锁
	e.mu.Unlock()

	// 从现有数据填充索引
	err = e.fillIndexFromExistingData(ctx, dbName, tableName, idx, tagIndex.Fields)
	if err != nil {
		// 重新获取锁以便返回
		e.mu.Lock()
		return fmt.Errorf("failed to fill index from existing data: %w", err)
	}

	return nil
}

// DropIndex 删除索引
func (e *StorageEngine) DropIndex(dbName, tableName, indexName string) error {
	// 检查数据库和表是否存在
	e.mu.Lock()
	defer e.mu.Unlock()

	db, dbExists := e.databases[dbName]
	if !dbExists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	table, tableExists := db.Tables[tableName]
	if !tableExists {
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 从表的索引列表中删除
	found := false
	for i, idx := range table.TagIndexes {
		if idx.Name == indexName {
			// 删除索引
			table.TagIndexes = append(table.TagIndexes[:i], table.TagIndexes[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("index %s does not exist", indexName)
	}

	// 从索引管理器中删除
	ctx := context.Background()
	if err := e.indexMgr.DropIndex(ctx, dbName, tableName, indexName); err != nil {
		return fmt.Errorf("failed to drop index: %w", err)
	}

	// 保存元数据
	if err := e.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// RebuildIndex 重建索引
func (e *StorageEngine) RebuildIndex(dbName, tableName, indexName string) error {
	// 检查数据库和表是否存在
	e.mu.RLock()
	db, dbExists := e.databases[dbName]
	if !dbExists {
		e.mu.RUnlock()
		return fmt.Errorf("database %s does not exist", dbName)
	}

	table, tableExists := db.Tables[tableName]
	if !tableExists {
		e.mu.RUnlock()
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 查找索引
	var targetIndex *model.TagIndex
	for i, idx := range table.TagIndexes {
		if idx.Name == indexName {
			targetIndex = &table.TagIndexes[i]
			break
		}
	}
	e.mu.RUnlock()

	if targetIndex == nil {
		return fmt.Errorf("index %s does not exist", indexName)
	}

	// 重建索引
	ctx := context.Background()
	if err := e.indexMgr.RebuildIndex(ctx, dbName, tableName, indexName); err != nil {
		return fmt.Errorf("failed to rebuild index: %w", err)
	}

	// 获取内存表
	_, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get memtable: %w", err)
	}

	// 填充索引数据
	// TODO: 实现从现有数据填充索引

	return nil
}

// ListIndexes 列出表的所有索引
func (e *StorageEngine) ListIndexes(dbName, tableName string) ([]model.TagIndex, error) {
	// 检查数据库和表是否存在
	e.mu.RLock()
	defer e.mu.RUnlock()

	db, dbExists := e.databases[dbName]
	if !dbExists {
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	table, tableExists := db.Tables[tableName]
	if !tableExists {
		return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 返回索引列表的副本
	result := make([]model.TagIndex, len(table.TagIndexes))
	copy(result, table.TagIndexes)

	return result, nil
}

// GetIndexStats 获取索引统计信息
func (e *StorageEngine) GetIndexStats(dbName, tableName, indexName string) (*index.IndexStats, error) {
	// 检查数据库和表是否存在
	e.mu.RLock()
	db, dbExists := e.databases[dbName]
	if !dbExists {
		e.mu.RUnlock()
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	_, tableExists := db.Tables[tableName]
	if !tableExists {
		e.mu.RUnlock()
		return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}
	e.mu.RUnlock()

	// 获取索引统计信息
	ctx := context.Background()
	stats, err := e.indexMgr.GetStats(ctx, dbName, tableName, indexName)
	if err != nil {
		return nil, fmt.Errorf("failed to get index stats: %w", err)
	}

	return &stats, nil
}

// fillIndexFromExistingData 从现有数据填充索引
func (e *StorageEngine) fillIndexFromExistingData(ctx context.Context, dbName, tableName string, idx index.Index, fields []string) error {
	// 获取内存表
	memTable, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get memtable: %w", err)
	}

	// 获取表定义
	e.mu.RLock()
	db, ok := e.databases[dbName]
	if !ok {
		e.mu.RUnlock()
		return fmt.Errorf("database %s not found", dbName)
	}

	table, ok := db.Tables[tableName]
	if !ok {
		e.mu.RUnlock()
		return fmt.Errorf("table %s not found", tableName)
	}
	e.mu.RUnlock()

	// 获取所有数据点
	allPoints := memTable.GetAll()

	// 遍历所有数据点，更新索引
	for _, point := range allPoints {
		// 生成唯一的时间序列ID
		seriesID := fmt.Sprintf("%s:%s:%d", dbName, tableName, point.Timestamp)

		// 根据索引字段数量决定如何更新索引
		if len(fields) == 1 {
			// 单字段索引
			field := fields[0]

			// 检查字段是否是时间字段
			if field == table.Schema.TimeField {
				// 时间索引
				if err := idx.Insert(ctx, point.Timestamp, seriesID); err != nil {
					return fmt.Errorf("failed to update time index: %w", err)
				}
			} else if value, ok := point.Tags[field]; ok {
				// 标签索引
				if err := idx.Insert(ctx, value, seriesID); err != nil {
					return fmt.Errorf("failed to update index: %w", err)
				}
			}
		} else {
			// 复合索引
			values := make([]string, len(fields))
			for i, field := range fields {
				if value, ok := point.Tags[field]; ok {
					values[i] = value
				} else {
					values[i] = "" // 字段不存在，使用空字符串
				}
			}
			// 更新索引
			if err := idx.Insert(ctx, values, seriesID); err != nil {
				return fmt.Errorf("failed to update index: %w", err)
			}
		}
	}

	return nil
}

// SelectBestIndex 选择最佳索引
func (qo *QueryOptimizer) SelectBestIndex(ctx context.Context, dbName, tableName string, filters map[string]interface{}, timeRange *TimeRange) (string, error) {
	// 获取表的所有索引
	e := qo.engine
	e.mu.RLock()
	defer e.mu.RUnlock()

	db, ok := e.databases[dbName]
	if !ok {
		return "", fmt.Errorf("database %s not found", dbName)
	}

	table, ok := db.Tables[tableName]
	if !ok {
		return "", fmt.Errorf("table %s not found", tableName)
	}

	// 如果只有时间范围查询，使用时间索引
	if len(filters) == 0 && timeRange != nil {
		return "time_idx", nil
	}

	// 计算每个索引的得分
	type indexScore struct {
		name  string
		score float64
	}

	scores := make([]indexScore, 0, len(table.TagIndexes))

	for _, idx := range table.TagIndexes {
		score := 0.0

		// 检查索引字段是否匹配查询条件
		fieldsInQuery := 0
		for _, field := range idx.Fields {
			if _, ok := filters[field]; ok {
				fieldsInQuery++
			}
		}

		// 如果没有匹配的字段，跳过此索引
		if fieldsInQuery == 0 {
			continue
		}

		// 计算字段匹配率
		fieldMatchRatio := float64(fieldsInQuery) / float64(len(idx.Fields))

		// 计算查询条件覆盖率
		filterCoverageRatio := float64(fieldsInQuery) / float64(len(filters))

		// 基础分数 = 字段匹配率 * 查询条件覆盖率
		score = fieldMatchRatio * filterCoverageRatio

		// 根据索引类型调整分数
		switch idx.Type {
		case "btree":
			// B树索引适合范围查询
			if timeRange != nil {
				score *= 1.2
			}
		case "hash":
			// 哈希索引适合精确匹配
			if timeRange == nil {
				score *= 1.3
			}
		case "bitmap":
			// 位图索引适合低基数字段
			if idx.CardinalityEstimate > 0 && idx.CardinalityEstimate < 1000 {
				score *= 1.4
			}
		case "inverted":
			// 倒排索引适合文本搜索
			score *= 1.1
		}

		// 考虑索引的基数估计
		if idx.CardinalityEstimate > 0 {
			// 高基数索引更有选择性
			selectivity := math.Min(1.0, 10000.0/float64(idx.CardinalityEstimate))
			score *= (1.0 + selectivity)
		}

		// 添加到得分列表
		scores = append(scores, indexScore{
			name:  idx.Name,
			score: score,
		})
	}

	// 如果没有合适的索引，使用时间索引
	if len(scores) == 0 {
		return "time_idx", nil
	}

	// 选择得分最高的索引
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	return scores[0].name, nil
}
