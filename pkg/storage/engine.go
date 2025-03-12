package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/index"
	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/model"
	"ChronoGo/pkg/util"
)

// StorageEngine 表示时序存储引擎
type StorageEngine struct {
	dataDir           string                     // 数据目录
	walDir            string                     // WAL目录
	metaDir           string                     // 元数据目录
	coldDir           string                     // 冷数据目录
	databases         map[string]*model.Database // 数据库映射
	memTables         map[string]*MemTable       // 内存表映射 (db:table -> memtable) - 将逐步废弃
	memTableManager   *MemTableManager           // 内存表管理器 - 新增
	sstableManager    *SSTableManager            // SSTable 管理器 - 新增
	wal               *WAL                       // 预写日志
	asyncWAL          *AsyncWAL                  // 异步预写日志
	writeBuffer       *WriteBuffer               // 写缓冲区
	indexMgr          index.IndexManager         // 索引管理器
	asyncIndexUpdater *AsyncIndexUpdater         // 异步索引更新器
	flushTicker       *time.Ticker               // 刷盘定时器
	storageTiers      []model.StorageTier        // 存储层级
	mu                sync.RWMutex               // 读写锁（旧的锁，将逐步替换）
	lockManager       *LockManager               // 锁管理器
	shardedLockMgr    *ShardedLockManager        // 分片锁管理器 - 新增
	closing           chan struct{}              // 关闭信号
	pointPool         *model.TimeSeriesPointPool // 时序数据点对象池
	objectPools       *util.ObjectPools          // 对象池管理器 - 新增
	batchProcessor    *BatchProcessor            // 批处理器 - 新增
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
	// 创建目录
	dataDir := filepath.Join(baseDir, "data")
	walDir := filepath.Join(baseDir, "wal")
	metaDir := filepath.Join(baseDir, "meta")
	coldDir := filepath.Join(baseDir, "cold")

	// 确保目录存在
	dirs := []string{dataDir, walDir, metaDir, coldDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// 创建数据点对象池并预热
	pointPool := model.NewTimeSeriesPointPool()
	pointPool.Prewarm(1000) // 预热1000个对象

	// 预热WAL条目对象池
	walEntryPool.Prewarm(1000) // 预热1000个对象

	// 创建存储引擎
	engine := &StorageEngine{
		dataDir:        dataDir,
		walDir:         walDir,
		metaDir:        metaDir,
		coldDir:        coldDir,
		databases:      make(map[string]*model.Database),
		memTables:      make(map[string]*MemTable),
		lockManager:    NewLockManager(32, true),
		shardedLockMgr: NewShardedLockManager(32), // 创建分片锁管理器，32个分片
		closing:        make(chan struct{}),
		pointPool:      model.NewTimeSeriesPointPool(), // 初始化对象池
		objectPools:    util.NewObjectPools(),          // 初始化对象池管理器 - 新增
	}

	// 预热对象池
	engine.objectPools.PrewarmAll(1000)

	// 创建索引管理器
	indexMgr := index.NewIndexManager()
	engine.indexMgr = indexMgr

	// 创建WAL
	wal, err := NewWAL(walDir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}
	engine.wal = wal

	// 创建异步WAL
	// 参数: WAL, 队列大小, 批处理大小, 刷新间隔, 工作线程数
	asyncWAL := NewAsyncWAL(wal, 10000, 100, 100*time.Millisecond, 2)
	engine.asyncWAL = asyncWAL

	// 创建内存表管理器
	// 参数: 数据目录, 最大内存表大小, 最大只读内存表数量, 刷盘工作线程数, 存储引擎引用
	memTableManager := NewMemTableManager(dataDir, 64*1024*1024, 2, 2, engine)
	engine.memTableManager = memTableManager

	// 创建 SSTable 管理器
	// 参数: 数据目录, 配置选项, 存储引擎引用
	sstableDir := filepath.Join(dataDir, "sstable")
	sstableManager, err := NewSSTableManager(sstableDir, DefaultSSTableOptions(), engine)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable manager: %w", err)
	}
	engine.sstableManager = sstableManager

	// 加载元数据
	if err := engine.loadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	// 从WAL恢复
	if err := engine.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// 创建写缓冲区
	// 参数: 存储引擎, 批处理大小, 刷新间隔
	writeBuffer := NewWriteBuffer(engine, 100, 100*time.Millisecond)
	engine.writeBuffer = writeBuffer

	// 创建异步索引更新器
	// 参数: 索引管理器, 队列大小, 批处理大小, 刷新间隔, 工作线程数
	asyncIndexUpdater := NewAsyncIndexUpdater(indexMgr, 10000, 100, 100*time.Millisecond, 2)
	engine.asyncIndexUpdater = asyncIndexUpdater

	// 创建批处理器 - 新增
	engine.batchProcessor = NewBatchProcessor(engine, 1000, 100*time.Millisecond, runtime.NumCPU())

	// 启动后台任务
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
	// 读取元数据文件
	metadataPath := filepath.Join(e.dataDir, "metadata.json")
	logger.Printf("读取元数据文件: %s", metadataPath)

	// 检查文件是否存在
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		logger.Printf("元数据文件不存在，使用空数据库")
		return nil
	}

	// 读取文件内容
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// 解析元数据
	logger.Printf("解析元数据")
	var metadata struct {
		Databases map[string]map[string]interface{} `json:"databases"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// 加载数据库
	for dbName, dbMap := range metadata.Databases {
		logger.Printf("加载数据库: %s", dbName)

		// 创建数据库对象
		db := &model.Database{
			Name:   dbName,
			Tables: make(map[string]*model.Table),
		}

		// 解析保留策略
		if retentionData, ok := dbMap["retentionPolicy"].(map[string]interface{}); ok {
			if duration, ok := retentionData["Duration"].(float64); ok {
				db.RetentionPolicy.Duration = time.Duration(duration)
			}
			if precision, ok := retentionData["Precision"].(string); ok {
				db.RetentionPolicy.Precision = precision
			}
		}

		// 解析表
		if tablesData, ok := dbMap["tables"].(map[string]interface{}); ok {
			for tableName, tableData := range tablesData {
				logger.Printf("加载表: %s.%s", dbName, tableName)

				tableMap, ok := tableData.(map[string]interface{})
				if !ok {
					logger.Printf("表 %s.%s 格式不正确", dbName, tableName)
					continue
				}

				// 创建表对象
				table := &model.Table{
					Name:     tableName,
					Database: dbName,
				}

				// 解析模式
				if schemaData, ok := tableMap["schema"].(map[string]interface{}); ok {
					if timeField, ok := schemaData["TimeField"].(string); ok {
						table.Schema.TimeField = timeField
					}

					if tagFields, ok := schemaData["TagFields"].(map[string]interface{}); ok {
						table.Schema.TagFields = make(map[string]string)
						for k, v := range tagFields {
							if strVal, ok := v.(string); ok {
								table.Schema.TagFields[k] = strVal
							}
						}
					}

					if fields, ok := schemaData["Fields"].(map[string]interface{}); ok {
						table.Schema.Fields = make(map[string]string)
						for k, v := range fields {
							if strVal, ok := v.(string); ok {
								table.Schema.Fields[k] = strVal
							}
						}
					}
				}

				// 解析标签索引
				if tagIndexesData, ok := tableMap["tagIndexes"].([]interface{}); ok {
					for _, indexData := range tagIndexesData {
						indexMap, ok := indexData.(map[string]interface{})
						if !ok {
							continue
						}

						tagIndex := model.TagIndex{}

						if name, ok := indexMap["Name"].(string); ok {
							tagIndex.Name = name
						}

						if indexType, ok := indexMap["Type"].(string); ok {
							tagIndex.Type = indexType
						}

						if unique, ok := indexMap["Unique"].(bool); ok {
							tagIndex.Unique = unique
						}

						if fieldsData, ok := indexMap["Fields"].([]interface{}); ok {
							tagIndex.Fields = make([]string, 0, len(fieldsData))
							for _, field := range fieldsData {
								if strField, ok := field.(string); ok {
									tagIndex.Fields = append(tagIndex.Fields, strField)
								}
							}
						}

						table.TagIndexes = append(table.TagIndexes, tagIndex)
					}
				}

				// 添加表到数据库
				db.Tables[tableName] = table
			}
		}

		// 添加数据库到映射
		e.databases[dbName] = db
	}

	// 重新创建索引
	logger.Printf("重新创建索引")
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
				logger.Printf("创建索引: %s.%s.%s", dbName, tableName, tagIndex.Name)
				_, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
				if err != nil {
					logger.Printf("创建索引失败: %v", err)
					// 继续处理其他索引
				}
			}

			// 创建时间索引
			if table.Schema.TimeField != "" {
				logger.Printf("创建时间索引: %s.%s.time_idx", dbName, tableName)
				options := index.IndexOptions{
					Type:   index.IndexTypeBTree,
					Name:   "time_idx",
					Fields: []string{table.Schema.TimeField},
				}
				_, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
				if err != nil {
					logger.Printf("创建时间索引失败: %v", err)
					// 继续处理其他表
				}
			}
		}
	}

	logger.Printf("元数据加载完成")
	return nil
}

// saveMetadata 保存元数据
func (e *StorageEngine) saveMetadata(alreadyLocked ...bool) error {
	logger.Printf("开始保存元数据")

	// 检查是否已经持有锁
	isAlreadyLocked := false
	if len(alreadyLocked) > 0 && alreadyLocked[0] {
		isAlreadyLocked = true
		logger.Printf("saveMetadata: 调用者已持有锁，跳过加锁")
	}

	// 如果没有持有锁，则获取全局读锁
	var unlock func()
	if !isAlreadyLocked {
		unlock = e.lockManager.LockGlobal(false)
		defer func() {
			unlock()
			logger.Printf("saveMetadata: 解锁完成")
		}()
	}

	// 创建可序列化的数据库映射
	serializableDatabases := make(map[string]interface{})
	for dbName, db := range e.databases {
		logger.Printf("处理数据库: %s", dbName)

		// 创建可序列化的表映射
		serializableTables := make(map[string]interface{})
		for tableName, table := range db.Tables {
			logger.Printf("处理表: %s.%s", dbName, tableName)

			// 创建可序列化的表
			serializableTable := struct {
				Name       string           `json:"name"`
				Database   string           `json:"database"`
				Schema     model.Schema     `json:"schema"`
				TagIndexes []model.TagIndex `json:"tagIndexes"`
			}{
				Name:       table.Name,
				Database:   table.Database,
				Schema:     table.Schema,
				TagIndexes: table.TagIndexes,
			}
			serializableTables[tableName] = serializableTable
		}

		// 创建可序列化的数据库
		serializableDB := struct {
			Name            string                 `json:"name"`
			RetentionPolicy model.RetentionPolicy  `json:"retentionPolicy"`
			Tables          map[string]interface{} `json:"tables"`
		}{
			Name:            db.Name,
			RetentionPolicy: db.RetentionPolicy,
			Tables:          serializableTables,
		}
		serializableDatabases[dbName] = serializableDB
	}

	// 准备元数据
	metadata := struct {
		Databases map[string]interface{} `json:"databases"`
	}{
		Databases: serializableDatabases,
	}

	// 序列化元数据
	logger.Printf("序列化元数据")
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		logger.Printf("序列化元数据失败: %v", err)
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	logger.Printf("序列化元数据成功，大小: %d 字节", len(data))

	// 写入元数据文件
	metadataPath := filepath.Join(e.dataDir, "metadata.json")
	logger.Printf("写入元数据文件: %s", metadataPath)

	// 确保目录存在
	metadataDir := filepath.Dir(metadataPath)
	logger.Printf("确保目录存在: %s", metadataDir)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		logger.Printf("创建元数据目录失败: %v", err)
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}
	logger.Printf("目录已存在或创建成功")

	// 检查目录是否可写
	testFile := filepath.Join(metadataDir, "test.tmp")
	logger.Printf("检查目录是否可写: %s", testFile)
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		logger.Printf("目录不可写: %v", err)
		return fmt.Errorf("directory not writable: %w", err)
	}
	os.Remove(testFile)
	logger.Printf("目录可写")

	// 写入元数据文件
	logger.Printf("写入元数据文件内容: %d 字节", len(data))
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		logger.Printf("写入元数据文件失败: %v", err)
		return fmt.Errorf("failed to write metadata file: %w", err)
	}
	logger.Printf("写入元数据文件成功")

	// 验证文件是否成功写入
	logger.Printf("验证元数据文件是否成功写入")
	if _, err := os.Stat(metadataPath); err != nil {
		logger.Printf("验证元数据文件失败: %v", err)
		return fmt.Errorf("failed to verify metadata file: %w", err)
	}
	logger.Printf("验证元数据文件成功")

	logger.Printf("元数据保存成功")
	return nil
}

// recoverFromWAL 从WAL恢复数据
func (e *StorageEngine) recoverFromWAL() error {
	// 创建一个带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 创建一个通道来接收恢复结果
	resultChan := make(chan error, 1)

	// 在后台执行WAL恢复过程
	go func() {
		startTime := time.Now()
		logger.Printf("Starting WAL recovery")

		entries, err := e.wal.Recover()
		if err != nil {
			resultChan <- fmt.Errorf("failed to recover WAL entries: %w", err)
			return
		}

		logger.Printf("Recovered %d WAL entries in %v", len(entries), time.Since(startTime))

		// 处理WAL条目
		for _, entry := range entries {
			select {
			case <-ctx.Done():
				// 超时，停止处理
				logger.Printf("WAL recovery timed out, processed %d entries", len(entries))
				resultChan <- nil
				return
			default:
				// 继续处理
				switch entry.Type {
				case walOpInsert:
					var doc bson.D
					if err := bson.Unmarshal(entry.Data, &doc); err != nil {
						resultChan <- fmt.Errorf("failed to unmarshal WAL entry: %w", err)
						return
					}

					point, err := model.FromBSON(doc)
					if err != nil {
						resultChan <- fmt.Errorf("failed to convert BSON to TimeSeriesPoint: %w", err)
						return
					}

					// 使用内存表管理器插入数据点
					if err := e.memTableManager.Put(entry.Database, entry.Table, point); err != nil {
						resultChan <- fmt.Errorf("failed to insert data point: %w", err)
						return
					}

				case walOpDelete:
					// TODO: 实现删除操作恢复
				}
			}
		}

		logger.Printf("WAL recovery completed in %v", time.Since(startTime))
		resultChan <- nil
	}()

	// 等待恢复完成或超时
	select {
	case err := <-resultChan:
		return err
	case <-ctx.Done():
		logger.Printf("WAL recovery timed out after 3 seconds, continuing with partial recovery")
		return nil
	}
}

// startBackgroundTasks 启动后台任务
func (e *StorageEngine) startBackgroundTasks() {
	// 注意：不再需要单独的刷盘定时器，因为 MemTableManager 会自动处理刷盘
	// 但我们保留这个定时器以兼容旧代码，直到完全迁移到 MemTableManager
	e.flushTicker = time.NewTicker(10 * time.Second)     // 每10秒刷新一次内存表
	retentionTicker := time.NewTicker(1 * time.Hour)     // 每小时检查一次数据保留
	downsampleTicker := time.NewTicker(30 * time.Minute) // 每30分钟执行一次降采样
	tierMigrationTicker := time.NewTicker(2 * time.Hour) // 每2小时执行一次数据迁移
	walCompactionTicker := time.NewTicker(1 * time.Hour) // 每小时执行一次WAL压缩

	go func() {
		for {
			select {
			case <-e.flushTicker.C:
				// 兼容旧代码，刷新旧的内存表
				e.flushMemTables()
			case <-retentionTicker.C:
				e.enforceRetentionPolicies()
			case <-downsampleTicker.C:
				e.performDownsampling()
			case <-tierMigrationTicker.C:
				e.migrateDataBetweenTiers()
			case <-walCompactionTicker.C:
				// 执行WAL压缩
				if e.wal != nil {
					if err := e.wal.Checkpoint(); err != nil {
						logger.Printf("WAL压缩失败: %v", err)
					}
				}
			case <-e.closing:
				retentionTicker.Stop()
				downsampleTicker.Stop()
				tierMigrationTicker.Stop()
				walCompactionTicker.Stop()
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
// 注意：此方法将逐步废弃，请使用 memTableManager
func (e *StorageEngine) getOrCreateMemTable(dbName, tableName string) (*MemTable, error) {
	key := dbName + ":" + tableName

	// 获取全局写锁
	unlock := e.lockManager.LockGlobal(true)
	defer unlock()

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
	logger.Printf("开始创建数据库: %s\n", name)

	// 获取全局写锁
	unlock := e.lockManager.LockGlobal(true)
	defer func() {
		unlock()
		logger.Printf("CreateDatabase: 解锁完成\n")
	}()

	if _, exists := e.databases[name]; exists {
		logger.Printf("数据库 %s 已存在\n", name)
		return fmt.Errorf("database %s already exists", name)
	}

	// 创建数据库目录
	dbDir := filepath.Join(e.dataDir, name)
	logger.Printf("创建数据库目录: %s\n", dbDir)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		logger.Printf("创建数据库目录失败: %v\n", err)
		return fmt.Errorf("failed to create database directory: %w", err)
	}
	logger.Printf("数据库目录创建成功\n")

	// 创建数据库对象
	db := &model.Database{
		Name:            name,
		RetentionPolicy: retentionPolicy,
		Tables:          make(map[string]*model.Table),
	}

	logger.Printf("添加数据库 %s 到映射\n", name)
	e.databases[name] = db
	logger.Printf("数据库添加到映射成功\n")

	// 保存元数据
	logger.Printf("保存数据库 %s 的元数据\n", name)
	if err := e.saveMetadata(true); err != nil {
		logger.Printf("保存数据库 %s 的元数据失败: %v\n", name, err)
		// 回滚操作
		delete(e.databases, name)
		return fmt.Errorf("failed to save metadata: %w", err)
	}
	logger.Printf("保存数据库 %s 的元数据成功\n", name)

	logger.Printf("数据库 %s 创建成功\n", name)
	return nil
}

// CreateTable 创建表
func (e *StorageEngine) CreateTable(dbName, tableName string, schema model.Schema, tagIndexes []model.TagIndex) error {
	logger.Printf("开始创建表 %s.%s\n", dbName, tableName)

	// 获取数据库写锁
	unlock := e.lockManager.LockDatabase(dbName, true)
	defer func() {
		unlock()
		logger.Printf("CreateTable: 解锁完成\n")
	}()

	db, exists := e.databases[dbName]
	if !exists {
		logger.Printf("数据库 %s 不存在\n", dbName)
		return fmt.Errorf("database %s does not exist", dbName)
	}

	if _, exists := db.Tables[tableName]; exists {
		logger.Printf("表 %s 已存在于数据库 %s 中\n", tableName, dbName)
		return fmt.Errorf("table %s already exists in database %s", tableName, dbName)
	}

	// 创建表目录
	tableDir := filepath.Join(e.dataDir, dbName, tableName)
	logger.Printf("创建表目录: %s\n", tableDir)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		logger.Printf("创建表目录失败: %v\n", err)
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// 创建表对象
	table := &model.Table{
		Name:       tableName,
		Database:   dbName,
		Schema:     schema,
		TagIndexes: tagIndexes,
	}

	logger.Printf("添加表 %s 到数据库 %s\n", tableName, dbName)
	db.Tables[tableName] = table

	// 创建索引
	if tagIndexes != nil && len(tagIndexes) > 0 {
		logger.Printf("开始创建 %d 个标签索引\n", len(tagIndexes))
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

			logger.Printf("创建索引 %s\n", tagIndex.Name)
			// 创建索引
			_, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
			if err != nil {
				logger.Printf("创建索引 %s 失败: %v\n", tagIndex.Name, err)
				return fmt.Errorf("failed to create index %s: %w", tagIndex.Name, err)
			}
		}
	} else {
		logger.Printf("没有标签索引需要创建\n")
	}

	// 创建时间索引
	if schema.TimeField != "" {
		logger.Printf("开始创建时间索引，时间字段: %s\n", schema.TimeField)
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

		ctx := context.Background()
		logger.Printf("注册时间索引\n")
		_, err := e.indexMgr.CreateIndex(ctx, dbName, tableName, options)
		if err != nil {
			logger.Printf("创建时间索引失败: %v\n", err)
			return fmt.Errorf("failed to create time index: %w", err)
		}
	} else {
		logger.Printf("没有时间字段，跳过创建时间索引\n")
	}

	// 保存元数据
	logger.Printf("保存元数据\n")
	if err := e.saveMetadata(true); err != nil {
		logger.Printf("保存元数据失败: %v\n", err)
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	logger.Printf("表 %s.%s 创建成功\n", dbName, tableName)
	return nil
}

// InsertPoint 插入时序数据点
func (e *StorageEngine) InsertPoint(dbName, tableName string, point *model.TimeSeriesPoint) error {
	// 使用写缓冲区
	if e.writeBuffer != nil {
		return e.writeBuffer.Add(dbName, tableName, point)
	}

	// 如果写缓冲区不可用，则使用同步方式插入
	return e.InsertPointSync(dbName, tableName, point)
}

// InsertPointSync 同步插入时序数据点
func (e *StorageEngine) InsertPointSync(dbName, tableName string, point *model.TimeSeriesPoint) error {
	// 获取表写锁
	unlock := e.lockManager.LockTable(dbName, tableName, true)
	defer unlock()

	// 检查数据库和表是否存在
	db, dbExists := e.databases[dbName]
	if !dbExists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	table, tableExists := db.Tables[tableName]
	if !tableExists {
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 创建WAL条目
	doc := point.ToBSON()
	entry, err := CreateInsertEntry(dbName, tableName, point.Timestamp, doc)
	if err != nil {
		return fmt.Errorf("failed to create WAL entry: %w", err)
	}

	// 使用异步WAL
	if e.asyncWAL != nil {
		if err := e.asyncWAL.Write(entry); err != nil {
			return fmt.Errorf("failed to write to async WAL: %w", err)
		}
	} else {
		// 如果异步WAL不可用，则使用同步WAL
		if e.wal != nil {
			// 创建WAL条目
			entry := &WALEntry{
				Type:      walOpInsert,
				Database:  dbName,
				Table:     tableName,
				Timestamp: point.Timestamp,
			}

			// 将数据点转换为BSON并序列化
			doc := point.ToBSON()
			data, err := bson.Marshal(doc)
			if err != nil {
				return err
			}
			entry.Data = data

			if e.asyncWAL != nil {
				e.asyncWAL.Write(entry)
			} else {
				if err := e.wal.Write(entry); err != nil {
					return err
				}
			}
		}
	}

	// 使用内存表管理器插入数据点
	if err := e.memTableManager.Put(dbName, tableName, point); err != nil {
		return fmt.Errorf("failed to insert data point: %w", err)
	}

	// 生成唯一的时间序列ID
	seriesID := fmt.Sprintf("%s:%s:%d", dbName, tableName, point.Timestamp)

	// 使用异步索引更新器
	if e.asyncIndexUpdater != nil {
		// 更新索引
		for _, idx := range table.TagIndexes {
			// 获取索引键
			var indexKey interface{}
			if len(idx.Fields) == 1 {
				// 单字段索引
				indexKey = point.Tags[idx.Fields[0]]
			} else {
				// 复合索引
				values := make([]string, len(idx.Fields))
				for i, field := range idx.Fields {
					values[i] = point.Tags[field]
				}
				indexKey = values
			}

			// 异步更新索引
			if err := e.asyncIndexUpdater.UpdateIndex(dbName, tableName, idx.Name, indexKey, seriesID); err != nil {
				logger.Printf("Failed to update index %s: %v", idx.Name, err)
			}
		}
	}

	return nil
}

// QueryByTimeRange 按时间范围查询时序数据点
// 注意：调用者必须在使用完结果后调用ReleasePoints方法将对象放回池中，否则会导致内存泄漏
func (e *StorageEngine) QueryByTimeRange(dbName, tableName string, start, end int64) ([]*model.TimeSeriesPoint, error) {
	// 获取表读锁
	unlock := e.lockManager.LockTable(dbName, tableName, false)
	defer unlock()

	// 检查数据库和表是否存在
	db, dbExists := e.databases[dbName]
	if !dbExists {
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	_, tableExists := db.Tables[tableName]
	if !tableExists {
		return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 从内存表查询
	memPoints := e.memTableManager.QueryByTimeRange(dbName, tableName, start, end)

	// 从 SSTable 查询
	var sstablePoints []*model.TimeSeriesPoint
	if e.sstableManager != nil {
		// 创建时间范围
		timeRange := TimeRange{
			Start: start,
			End:   end,
		}

		// 从 SSTable 查询
		var err error
		sstablePoints, err = e.sstableManager.Query(dbName, tableName, timeRange, nil)
		if err != nil {
			logger.Printf("从 SSTable 查询失败: %v", err)
			// 即使查询失败，我们仍然返回内存表中的结果
		}
	}

	// 合并结果
	if len(sstablePoints) > 0 {
		// 合并内存表和 SSTable 的结果
		allPoints := append(memPoints, sstablePoints...)

		// 按时间戳排序
		sort.Slice(allPoints, func(i, j int) bool {
			return allPoints[i].Timestamp < allPoints[j].Timestamp
		})

		// 去重
		if len(allPoints) > 1 {
			// 使用双指针法去重
			j := 0
			for i := 1; i < len(allPoints); i++ {
				if allPoints[i].Timestamp != allPoints[j].Timestamp {
					j++
					if i != j {
						allPoints[j] = allPoints[i]
					}
				}
			}
			allPoints = allPoints[:j+1]
		}

		return allPoints, nil
	}

	// 如果没有 SSTable 结果，直接返回内存表中的结果
	return memPoints, nil
}

// QueryByTag 按标签查询时序数据点
// 注意：调用者必须在使用完结果后调用ReleasePoints方法将对象放回池中，否则会导致内存泄漏
func (e *StorageEngine) QueryByTag(dbName, tableName, tagKey, tagValue string) ([]*model.TimeSeriesPoint, error) {
	// 获取表读锁
	unlock := e.lockManager.LockTable(dbName, tableName, false)
	defer unlock()

	// 检查数据库和表是否存在
	db, dbExists := e.databases[dbName]
	if !dbExists {
		return nil, fmt.Errorf("database %s does not exist", dbName)
	}

	_, tableExists := db.Tables[tableName]
	if !tableExists {
		return nil, fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 从内存表查询
	memPoints := e.memTableManager.QueryByTag(dbName, tableName, tagKey, tagValue)

	// 从 SSTable 查询
	var sstablePoints []*model.TimeSeriesPoint
	if e.sstableManager != nil {
		// 创建标签过滤条件
		tags := map[string]string{
			tagKey: tagValue,
		}

		// 创建时间范围（全范围）
		timeRange := TimeRange{
			Start: 0,
			End:   math.MaxInt64,
		}

		// 从 SSTable 查询
		var err error
		sstablePoints, err = e.sstableManager.Query(dbName, tableName, timeRange, tags)
		if err != nil {
			logger.Printf("从 SSTable 查询标签失败: %v", err)
			// 即使查询失败，我们仍然返回内存表中的结果
		}
	}

	// 合并结果
	if len(sstablePoints) > 0 {
		// 合并内存表和 SSTable 的结果
		allPoints := append(memPoints, sstablePoints...)

		// 按时间戳排序
		sort.Slice(allPoints, func(i, j int) bool {
			return allPoints[i].Timestamp < allPoints[j].Timestamp
		})

		// 去重
		if len(allPoints) > 1 {
			// 使用双指针法去重
			j := 0
			for i := 1; i < len(allPoints); i++ {
				if allPoints[i].Timestamp != allPoints[j].Timestamp {
					j++
					if i != j {
						allPoints[j] = allPoints[i]
					}
				}
			}
			allPoints = allPoints[:j+1]
		}

		return allPoints, nil
	}

	// 如果没有 SSTable 结果，直接返回内存表中的结果
	return memPoints, nil
}

// Close 关闭存储引擎
func (e *StorageEngine) Close() error {
	logger.Printf("正在关闭存储引擎...")
	startTime := time.Now()

	// 发送关闭信号
	close(e.closing)

	// 停止刷盘定时器
	if e.flushTicker != nil {
		e.flushTicker.Stop()
	}

	// 关闭内存表管理器
	logger.Printf("关闭内存表管理器...")
	if err := e.memTableManager.Close(); err != nil {
		logger.Printf("关闭内存表管理器失败: %v", err)
	}

	// 关闭 SSTable 管理器
	if e.sstableManager != nil {
		logger.Printf("关闭 SSTable 管理器...")
		if e.sstableManager.bgCompactor != nil {
			e.sstableManager.bgCompactor.Stop()
		}
	}

	// 执行WAL检查点和压缩
	logger.Printf("执行WAL检查点和压缩...")
	if err := e.wal.Checkpoint(); err != nil {
		logger.Printf("WAL检查点失败: %v", err)
	}

	// 执行额外的WAL检查点
	if e.wal != nil {
		if err := e.wal.Checkpoint(); err != nil {
			logger.Printf("关闭时WAL压缩失败: %v", err)
		}
	}

	// 关闭写缓冲区
	if e.writeBuffer != nil {
		logger.Printf("关闭写缓冲区...")
		if err := e.writeBuffer.Close(); err != nil {
			logger.Printf("关闭写缓冲区失败: %v", err)
		}
	}

	// 关闭异步索引更新器
	if e.asyncIndexUpdater != nil {
		logger.Printf("关闭异步索引更新器...")
		if err := e.asyncIndexUpdater.Close(); err != nil {
			logger.Printf("关闭异步索引更新器失败: %v", err)
		}
	}

	// 关闭异步WAL
	if e.asyncWAL != nil {
		logger.Printf("关闭异步WAL...")
		if err := e.asyncWAL.Close(); err != nil {
			logger.Printf("关闭异步WAL失败: %v", err)
		}
	}

	// 关闭WAL
	if e.wal != nil {
		logger.Printf("关闭WAL...")
		if err := e.wal.Close(); err != nil {
			logger.Printf("关闭WAL失败: %v", err)
		}
	}

	// 保存元数据
	logger.Printf("保存元数据...")
	if err := e.saveMetadata(); err != nil {
		logger.Printf("保存元数据失败: %v", err)
	}

	// 关闭批处理器
	if e.batchProcessor != nil {
		err := e.batchProcessor.Close()
		if err != nil {
			log.Printf("Failed to close batch processor: %v", err)
		}
	}

	logger.Printf("存储引擎关闭完成，耗时: %v", time.Since(startTime))
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
	logger.Printf("开始删除数据库: %s\n", dbName)

	// 获取全局写锁
	unlock := e.lockManager.LockGlobal(true)
	defer func() {
		unlock()
		logger.Printf("DropDatabase: 解锁完成\n")
	}()

	// 检查数据库是否存在
	if _, exists := e.databases[dbName]; !exists {
		logger.Printf("数据库 %s 不存在\n", dbName)
		return fmt.Errorf("database %s does not exist", dbName)
	}
	logger.Printf("数据库 %s 存在，准备删除\n", dbName)

	// 删除数据库目录
	dbPath := filepath.Join(e.dataDir, dbName)
	logger.Printf("删除数据库目录: %s\n", dbPath)
	if err := os.RemoveAll(dbPath); err != nil {
		logger.Printf("删除数据库目录失败: %v\n", err)
		return fmt.Errorf("failed to remove database directory: %w", err)
	}
	logger.Printf("数据库目录删除成功\n")

	// 删除内存中的数据库对象
	logger.Printf("从内存中删除数据库对象\n")
	delete(e.databases, dbName)
	logger.Printf("内存中的数据库对象删除成功\n")

	// 删除相关的内存表
	logger.Printf("删除相关的内存表\n")
	prefix := dbName + ":"
	for key := range e.memTables {
		if strings.HasPrefix(key, prefix) {
			delete(e.memTables, key)
		}
	}
	logger.Printf("内存表删除成功\n")

	// 移除数据库相关的所有锁
	e.lockManager.RemoveDatabaseLocks(dbName)
	logger.Printf("数据库相关的锁已移除\n")

	// 保存更新后的元数据
	logger.Printf("保存更新后的元数据\n")
	if err := e.saveMetadata(true); err != nil {
		logger.Printf("保存元数据失败: %v\n", err)
		return fmt.Errorf("failed to save metadata: %w", err)
	}
	logger.Printf("元数据保存成功\n")

	logger.Printf("数据库 %s 删除成功\n", dbName)
	return nil
}

// DropTable 删除表
func (e *StorageEngine) DropTable(dbName, tableName string) error {
	logger.Printf("开始删除表: %s.%s\n", dbName, tableName)

	// 获取数据库写锁
	unlock := e.lockManager.LockDatabase(dbName, true)
	defer func() {
		unlock()
		logger.Printf("DropTable: 解锁完成\n")
	}()

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

	// 删除内存表
	memTableKey := dbName + ":" + tableName
	if _, exists := e.memTables[memTableKey]; exists {
		delete(e.memTables, memTableKey)
	}

	// 移除表锁
	e.lockManager.RemoveTableLock(dbName, tableName)

	// 保存更新后的元数据
	if err := e.saveMetadata(true); err != nil {
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

// enforceRetentionPolicies 执行数据保留策略
func (e *StorageEngine) enforceRetentionPolicies() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	for dbName, db := range e.databases {
		// 跳过没有保留策略的数据库
		if db.RetentionPolicy.Duration <= 0 {
			continue
		}

		// 计算保留边界时间
		retentionBoundary := now.Add(-db.RetentionPolicy.Duration)
		retentionTimestamp := retentionBoundary.UnixNano()

		// 对每个表执行保留策略
		for tableName := range db.Tables {
			go func(dbName, tableName string, retentionTimestamp int64) {
				if err := e.purgeExpiredData(dbName, tableName, retentionTimestamp); err != nil {
					logger.Printf("Error purging expired data for %s.%s: %v\n", dbName, tableName, err)
				}
			}(dbName, tableName, retentionTimestamp)
		}
	}
}

// purgeExpiredData 清理过期数据
func (e *StorageEngine) purgeExpiredData(dbName, tableName string, retentionTimestamp int64) error {
	// 获取表的磁盘存储路径
	tablePath := filepath.Join(e.dataDir, dbName, tableName)

	// 获取所有数据文件
	files, err := os.ReadDir(tablePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 目录不存在，无需清理
		}
		return fmt.Errorf("failed to read table directory: %w", err)
	}

	// 遍历数据文件，删除过期的文件
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过子目录
		}

		// 解析文件名，获取时间范围
		// 假设文件名格式为: data_<startTime>_<endTime>.tsdb
		parts := strings.Split(file.Name(), "_")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".tsdb") {
			continue // 跳过不符合命名规则的文件
		}

		// 只解析结束时间，因为我们只需要比较结束时间与时间边界
		fileEndTime, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue // 跳过无法解析的文件
		}

		// 如果文件的结束时间早于保留边界，则删除该文件
		if fileEndTime < retentionTimestamp {
			filePath := filepath.Join(tablePath, file.Name())
			if err := os.Remove(filePath); err != nil {
				return fmt.Errorf("failed to remove expired data file %s: %w", filePath, err)
			}
		}
	}

	return nil
}

// performDownsampling 执行数据降采样
func (e *StorageEngine) performDownsampling() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	for dbName, db := range e.databases {
		// 跳过没有降采样策略的数据库
		if len(db.RetentionPolicy.Downsample) == 0 {
			continue
		}

		// 对每个表执行降采样
		for tableName, table := range db.Tables {
			go func(dbName string, tableName string, table *model.Table, policies []model.DownsamplePolicy) {
				for _, policy := range policies {
					// 计算需要降采样的时间范围
					endTime := now.Add(-time.Hour)                  // 留出1小时缓冲，避免处理最新数据
					startTime := endTime.Add(-policy.Interval * 10) // 处理10个降采样间隔的数据

					if err := e.downsampleData(dbName, tableName, table, policy, startTime, endTime); err != nil {
						logger.Printf("Error downsampling data for %s.%s: %v\n", dbName, tableName, err)
					}
				}
			}(dbName, tableName, table, db.RetentionPolicy.Downsample)
		}
	}
}

// downsampleData 对指定时间范围的数据进行降采样
func (e *StorageEngine) downsampleData(dbName, tableName string, table *model.Table, policy model.DownsamplePolicy, startTime, endTime time.Time) error {
	// 确定目标表名
	targetTable := tableName
	if policy.Destination != "" {
		targetTable = policy.Destination
	}

	// 检查降采样目标表是否存在，不存在则创建
	if policy.Destination != "" {
		e.mu.RLock()
		db := e.databases[dbName]
		_, exists := db.Tables[targetTable]
		e.mu.RUnlock()

		if !exists {
			// 创建与源表相同结构的目标表
			if err := e.CreateTable(dbName, targetTable, table.Schema, table.TagIndexes); err != nil {
				return fmt.Errorf("failed to create downsample target table: %w", err)
			}
		}
	}

	// 查询原始数据
	points, err := e.QueryByTimeRange(dbName, tableName, startTime.UnixNano(), endTime.UnixNano())
	if err != nil {
		return fmt.Errorf("failed to query data for downsampling: %w", err)
	}

	if len(points) == 0 {
		return nil // 没有数据需要降采样
	}

	// 按降采样间隔分组
	groups := make(map[int64][]*model.TimeSeriesPoint)
	for _, point := range points {
		// 计算时间戳所在的降采样间隔
		interval := point.Timestamp / policy.Interval.Nanoseconds()
		intervalStart := interval * policy.Interval.Nanoseconds()
		groups[intervalStart] = append(groups[intervalStart], point)
	}

	// 对每个时间间隔进行降采样
	for intervalStart, intervalPoints := range groups {
		// 按标签分组
		tagGroups := make(map[string][]*model.TimeSeriesPoint)
		for _, point := range intervalPoints {
			// 生成标签键
			tagKey := e.generateTagKey(point.Tags)
			tagGroups[tagKey] = append(tagGroups[tagKey], point)
		}

		// 对每个标签组执行降采样
		for _, tagPoints := range tagGroups {
			downsampledPoint, err := e.applyDownsampleFunction(tagPoints, policy.Function, intervalStart)
			if err != nil {
				return fmt.Errorf("failed to apply downsample function: %w", err)
			}

			// 插入降采样后的数据点
			if err := e.InsertPoint(dbName, targetTable, downsampledPoint); err != nil {
				return fmt.Errorf("failed to insert downsampled point: %w", err)
			}
		}
	}

	return nil
}

// generateTagKey 生成标签键
func (e *StorageEngine) generateTagKey(tags map[string]string) string {
	// 按键排序
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 构建标签键
	var builder strings.Builder
	for i, k := range keys {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(tags[k])
	}
	return builder.String()
}

// applyDownsampleFunction 应用降采样函数
func (e *StorageEngine) applyDownsampleFunction(points []*model.TimeSeriesPoint, function string, intervalStart int64) (*model.TimeSeriesPoint, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("no points to downsample")
	}

	// 使用第一个点的标签
	result := &model.TimeSeriesPoint{
		Timestamp: intervalStart,
		Tags:      make(map[string]string),
		Fields:    make(map[string]interface{}),
	}

	// 复制标签
	for k, v := range points[0].Tags {
		result.Tags[k] = v
	}

	// 收集所有字段
	fieldValues := make(map[string][]float64)
	for _, point := range points {
		for field, value := range point.Fields {
			// 尝试将值转换为float64
			var floatVal float64
			switch v := value.(type) {
			case float64:
				floatVal = v
			case float32:
				floatVal = float64(v)
			case int:
				floatVal = float64(v)
			case int64:
				floatVal = float64(v)
			default:
				// 非数值类型字段不参与降采样
				continue
			}
			fieldValues[field] = append(fieldValues[field], floatVal)
		}
	}

	// 对每个字段应用降采样函数
	for field, values := range fieldValues {
		if len(values) == 0 {
			continue
		}

		switch function {
		case "avg":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			result.Fields[field] = sum / float64(len(values))
		case "sum":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			result.Fields[field] = sum
		case "min":
			min := values[0]
			for _, v := range values {
				if v < min {
					min = v
				}
			}
			result.Fields[field] = min
		case "max":
			max := values[0]
			for _, v := range values {
				if v > max {
					max = v
				}
			}
			result.Fields[field] = max
		case "count":
			result.Fields[field] = float64(len(values))
		case "first":
			result.Fields[field] = values[0]
		case "last":
			result.Fields[field] = values[len(values)-1]
		default:
			return nil, fmt.Errorf("unsupported downsample function: %s", function)
		}
	}

	return result, nil
}

// migrateDataBetweenTiers 在不同存储层级之间迁移数据
func (e *StorageEngine) migrateDataBetweenTiers() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	for dbName, db := range e.databases {
		for tableName := range db.Tables {
			go func(dbName, tableName string) {
				if err := e.migrateDatabaseTableData(dbName, tableName, now); err != nil {
					logger.Printf("Error migrating data for %s.%s: %v\n", dbName, tableName, err)
				}
			}(dbName, tableName)
		}
	}
}

// migrateDatabaseTableData 迁移指定数据库表的数据
func (e *StorageEngine) migrateDatabaseTableData(dbName, tableName string, now time.Time) error {
	// 按照优先级排序存储层级
	tiers := make([]model.StorageTier, len(e.storageTiers))
	copy(tiers, e.storageTiers)
	sort.Slice(tiers, func(i, j int) bool {
		return tiers[i].Priority < tiers[j].Priority
	})

	// 对每个存储层级执行迁移
	for i, tier := range tiers {
		if i == len(tiers)-1 {
			break // 最后一个层级不需要迁移
		}

		// 计算当前层级的时间边界
		tierBoundary := now.Add(-tier.MaxAge)
		nextTier := tiers[i+1]

		// 迁移超过当前层级最大存储时间的数据到下一个层级
		if err := e.migrateDataToNextTier(dbName, tableName, tier, nextTier, tierBoundary); err != nil {
			return fmt.Errorf("failed to migrate data from %s to %s: %w", tier.Name, nextTier.Name, err)
		}
	}

	return nil
}

// migrateDataToNextTier 将数据从一个存储层级迁移到下一个层级
func (e *StorageEngine) migrateDataToNextTier(dbName, tableName string, currentTier, nextTier model.StorageTier, timeBoundary time.Time) error {
	// 获取当前层级的数据目录
	var sourceDir string
	if currentTier.Name == "hot" {
		sourceDir = filepath.Join(e.dataDir, dbName, tableName)
	} else {
		sourceDir = filepath.Join(e.dataDir, currentTier.Name, dbName, tableName)
	}

	// 获取目标层级的数据目录
	var targetDir string
	if nextTier.Name == "cold" {
		targetDir = filepath.Join(e.coldDir, dbName, tableName)
	} else {
		targetDir = filepath.Join(e.dataDir, nextTier.Name, dbName, tableName)
	}

	// 确保目标目录存在
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// 获取源目录中的所有数据文件
	files, err := os.ReadDir(sourceDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 源目录不存在，无需迁移
		}
		return fmt.Errorf("failed to read source directory: %w", err)
	}

	// 遍历数据文件，迁移符合条件的文件
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过子目录
		}

		// 解析文件名，获取时间范围
		// 假设文件名格式为: data_<startTime>_<endTime>.tsdb
		parts := strings.Split(file.Name(), "_")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".tsdb") {
			continue // 跳过不符合命名规则的文件
		}

		// 解析结束时间
		endTimeStr := strings.TrimSuffix(parts[2], ".tsdb")
		fileEndTime, err := strconv.ParseInt(endTimeStr, 10, 64)
		if err != nil {
			continue // 跳过无法解析的文件
		}

		// 如果文件的结束时间早于时间边界，则迁移该文件
		if fileEndTime < timeBoundary.UnixNano() {
			sourceFile := filepath.Join(sourceDir, file.Name())
			targetFile := filepath.Join(targetDir, file.Name())

			// 如果目标层级需要压缩，且当前层级不压缩
			if nextTier.Compress && !currentTier.Compress {
				// 读取源文件
				data, err := os.ReadFile(sourceFile)
				if err != nil {
					return fmt.Errorf("failed to read source file: %w", err)
				}

				// 压缩数据
				compressedData, err := e.compressData(data)
				if err != nil {
					return fmt.Errorf("failed to compress data: %w", err)
				}

				// 写入目标文件
				if err := os.WriteFile(targetFile+".gz", compressedData, 0644); err != nil {
					return fmt.Errorf("failed to write compressed file: %w", err)
				}
			} else {
				// 直接复制文件
				if err := e.copyFile(sourceFile, targetFile); err != nil {
					return fmt.Errorf("failed to copy file: %w", err)
				}
			}

			// 删除源文件
			if err := os.Remove(sourceFile); err != nil {
				return fmt.Errorf("failed to remove source file: %w", err)
			}
		}
	}

	return nil
}

// compressData 压缩数据
func (e *StorageEngine) compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	if _, err := gzWriter.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write to gzip writer: %w", err)
	}

	if err := gzWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return buf.Bytes(), nil
}

// copyFile 复制文件
func (e *StorageEngine) copyFile(src, dst string) error {
	// 尝试使用零拷贝技术
	err := util.CopyFileWithSendfile(src, dst)
	if err == nil {
		return nil
	}

	// 如果零拷贝失败，回退到标准复制
	logger.Printf("Sendfile failed, falling back to standard copy: %v", err)

	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}

	return nil
}

// queryTierByTimeRange 查询指定存储层级的数据
func (e *StorageEngine) queryTierByTimeRange(dbName, tableName, tierName string, start, end int64) ([]*model.TimeSeriesPoint, error) {
	var tierDir string
	if tierName == "hot" {
		tierDir = filepath.Join(e.dataDir, dbName, tableName)
	} else if tierName == "cold" {
		tierDir = filepath.Join(e.coldDir, dbName, tableName)
	} else {
		tierDir = filepath.Join(e.dataDir, tierName, dbName, tableName)
	}

	// 检查目录是否存在
	if _, err := os.Stat(tierDir); os.IsNotExist(err) {
		return nil, nil // 目录不存在，返回空结果
	}

	// 获取目录中的所有数据文件
	files, err := os.ReadDir(tierDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read tier directory: %w", err)
	}

	var points []*model.TimeSeriesPoint
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过子目录
		}

		// 解析文件名，获取时间范围
		// 假设文件名格式为: data_<startTime>_<endTime>.tsdb 或 data_<startTime>_<endTime>.tsdb.gz
		fileName := file.Name()
		isCompressed := strings.HasSuffix(fileName, ".gz")
		if isCompressed {
			fileName = strings.TrimSuffix(fileName, ".gz")
		}

		parts := strings.Split(fileName, "_")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".tsdb") {
			continue // 跳过不符合命名规则的文件
		}

		// 解析文件的时间范围
		startTimeStr := parts[1]
		endTimeStr := strings.TrimSuffix(parts[2], ".tsdb")

		fileStartTime, err := strconv.ParseInt(startTimeStr, 10, 64)
		if err != nil {
			continue // 跳过无法解析的文件
		}

		fileEndTime, err := strconv.ParseInt(endTimeStr, 10, 64)
		if err != nil {
			continue // 跳过无法解析的文件
		}

		// 检查文件的时间范围是否与查询范围重叠
		if fileEndTime < start || fileStartTime > end {
			continue // 文件的时间范围与查询范围不重叠，跳过
		}

		// 读取文件内容
		filePath := filepath.Join(tierDir, file.Name())
		var fileData []byte
		if isCompressed {
			// 读取并解压缩文件
			compressedData, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read compressed file: %w", err)
			}

			fileData, err = e.decompressData(compressedData)
			if err != nil {
				return nil, fmt.Errorf("failed to decompress file: %w", err)
			}
		} else {
			// 直接读取文件
			var err error
			fileData, err = os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read file: %w", err)
			}
		}

		// 解析文件内容，提取数据点
		filePoints, err := e.parseDataFile(fileData, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to parse data file: %w", err)
		}

		points = append(points, filePoints...)
	}

	return points, nil
}

// decompressData 解压缩数据
func (e *StorageEngine) decompressData(compressedData []byte) ([]byte, error) {
	gzReader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, gzReader); err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	return buf.Bytes(), nil
}

// parseDataFile 解析数据文件
func (e *StorageEngine) parseDataFile(data []byte, startTime, endTime int64) ([]*model.TimeSeriesPoint, error) {
	// 这里应该实现从二进制数据解析时序数据点的逻辑
	// 为简化示例，这里假设数据是JSON格式的
	var points []*model.TimeSeriesPoint
	if err := json.Unmarshal(data, &points); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	// 过滤出符合时间范围的数据点
	var result []*model.TimeSeriesPoint
	for _, point := range points {
		if point.Timestamp >= startTime && point.Timestamp <= endTime {
			result = append(result, point)
		}
	}

	return result, nil
}

// GetDatabaseSize 获取数据库大小（字节）
func (e *StorageEngine) GetDatabaseSize(dbName string) (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	db, exists := e.databases[dbName]
	if !exists {
		return 0, fmt.Errorf("database %s not found", dbName)
	}

	var totalSize int64
	for tableName := range db.Tables {
		tableSize, err := e.GetTableSize(dbName, tableName)
		if err != nil {
			return 0, err
		}
		totalSize += tableSize
	}

	return totalSize, nil
}

// GetTableSize 获取表大小（字节）
func (e *StorageEngine) GetTableSize(dbName, tableName string) (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	db, exists := e.databases[dbName]
	if !exists {
		return 0, fmt.Errorf("database %s not found", dbName)
	}

	// 检查表是否存在
	if _, exists := db.Tables[tableName]; !exists {
		return 0, fmt.Errorf("table %s not found in database %s", tableName, dbName)
	}

	// 计算表数据文件大小
	tableDataPath := filepath.Join(e.dataDir, dbName, tableName)
	size, err := getDirSize(tableDataPath)
	if err != nil {
		// 如果目录不存在，可能是新表，返回0
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	return size, nil
}

// getDirSize 获取目录大小（字节）
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// ClearTable 清空表中的所有数据
func (e *StorageEngine) ClearTable(dbName, tableName string) error {
	logger.Printf("开始清空表: %s.%s\n", dbName, tableName)

	// 第一阶段：获取数据库锁和表锁，检查数据库和表是否存在
	unlock := e.lockManager.LockMultiWithMaps(false, map[string]bool{dbName: true}, map[string]bool{dbName + ":" + tableName: true})

	// 检查数据库是否存在
	db, exists := e.databases[dbName]
	if !exists {
		unlock()
		logger.Printf("数据库 %s 不存在\n", dbName)
		return fmt.Errorf("database %s does not exist", dbName)
	}

	// 检查表是否存在
	table, exists := db.Tables[tableName]
	if !exists {
		unlock()
		logger.Printf("表 %s 在数据库 %s 中不存在\n", tableName, dbName)
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}
	logger.Printf("表 %s.%s 存在，准备清空\n", dbName, tableName)

	// 获取表的数据目录
	tableDir := filepath.Join(e.dataDir, dbName, tableName)
	logger.Printf("表数据目录: %s\n", tableDir)

	// 获取表中的数据文件
	dataFiles, err := filepath.Glob(filepath.Join(tableDir, "*.data"))
	if err != nil {
		unlock()
		logger.Printf("获取数据文件失败: %v\n", err)
		return fmt.Errorf("failed to get data files: %w", err)
	}
	logger.Printf("找到 %d 个数据文件\n", len(dataFiles))

	// 删除所有数据文件
	for _, file := range dataFiles {
		logger.Printf("删除数据文件: %s\n", file)
		if err := os.Remove(file); err != nil {
			unlock()
			logger.Printf("删除数据文件失败: %v\n", err)
			return fmt.Errorf("failed to remove data file %s: %w", file, err)
		}
	}
	logger.Printf("所有数据文件删除成功\n")

	// 清空内存表
	memTableKey := dbName + ":" + tableName
	if memTable, exists := e.memTables[memTableKey]; exists {
		logger.Printf("清空内存表\n")
		memTable.Clear()
		logger.Printf("内存表清空成功\n")
	}

	// 保存需要重建的索引
	var tagIndexes []model.TagIndex
	if len(table.TagIndexes) > 0 {
		tagIndexes = make([]model.TagIndex, len(table.TagIndexes))
		copy(tagIndexes, table.TagIndexes)
	}

	// 释放锁
	unlock()
	logger.Printf("ClearTable: 第一阶段完成，释放锁\n")

	// 第二阶段：重建索引（不需要锁）
	if len(tagIndexes) > 0 {
		logger.Printf("重建索引\n")
		for _, tagIndex := range tagIndexes {
			logger.Printf("重建索引: %s\n", tagIndex.Name)
			if err := e.RebuildIndex(dbName, tableName, tagIndex.Name); err != nil {
				logger.Printf("重建索引失败: %v\n", err)
				return fmt.Errorf("failed to rebuild index %s: %w", tagIndex.Name, err)
			}
		}
		logger.Printf("所有索引重建成功\n")
	}

	logger.Printf("表 %s.%s 清空成功\n", dbName, tableName)
	return nil
}

// InsertPoints 批量插入时序数据点
func (e *StorageEngine) InsertPoints(dbName, tableName string, points []*model.TimeSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}

	// 使用写缓冲区
	if e.writeBuffer != nil {
		// 批量添加到写缓冲区
		for _, point := range points {
			if err := e.writeBuffer.Add(dbName, tableName, point); err != nil {
				return fmt.Errorf("failed to add point to write buffer: %w", err)
			}
		}
		return nil
	}

	// 如果写缓冲区不可用，则使用同步方式插入
	return e.InsertPointsSync(dbName, tableName, points)
}

// InsertPointsSync 同步插入多个数据点
func (e *StorageEngine) InsertPointsSync(dbName, tableName string, points []*model.TimeSeriesPoint) error {
	// 获取数据库
	e.mu.RLock()
	db, dbExists := e.databases[dbName]
	e.mu.RUnlock()

	if !dbExists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	// 获取表
	e.mu.RLock()
	table, tableExists := db.Tables[tableName]
	e.mu.RUnlock()

	if !tableExists {
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// 获取或创建内存表
	key := dbName + ":" + tableName
	e.mu.Lock()
	memTable, exists := e.memTables[key]
	if !exists {
		// 创建新的内存表
		memTable = NewMemTable(64 * 1024 * 1024) // 64MB
		e.memTables[key] = memTable
	}
	e.mu.Unlock()

	// 预分配足够大小的缓冲区，减少内存重新分配
	docBuffers := make([][]byte, len(points))

	// 并行序列化数据点
	var wg sync.WaitGroup
	var mu sync.Mutex
	var serializationErr error

	// 使用工作池限制并发数
	workerCount := runtime.NumCPU()
	if workerCount > 4 {
		workerCount = 4 // 最多4个工作线程
	}

	// 创建工作通道
	type workItem struct {
		index int
		point *model.TimeSeriesPoint
	}
	workChan := make(chan workItem, len(points))

	// 提交工作
	for i, point := range points {
		workChan <- workItem{index: i, point: point}
	}
	close(workChan)

	// 启动工作线程
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()

			for work := range workChan {
				// 使用 MarshalBSON 方法，它内部使用对象池
				data, err := work.point.MarshalBSON()
				if err != nil {
					mu.Lock()
					serializationErr = err
					mu.Unlock()
					continue
				}

				mu.Lock()
				docBuffers[work.index] = data
				mu.Unlock()
			}
		}()
	}

	// 等待所有序列化完成
	wg.Wait()

	// 检查序列化错误
	if serializationErr != nil {
		return serializationErr
	}

	for i, point := range points {
		// 写入WAL
		if e.wal != nil {
			// 创建WAL条目
			entry := &WALEntry{
				Type:      walOpInsert,
				Database:  dbName,
				Table:     tableName,
				Timestamp: point.Timestamp,
			}

			// 使用已序列化的数据
			entry.Data = docBuffers[i]

			if e.asyncWAL != nil {
				e.asyncWAL.Write(entry)
			} else {
				if err := e.wal.Write(entry); err != nil {
					return err
				}
			}
		}

		// 写入内存表
		e.mu.Lock()

		// 使用已序列化的数据
		memTable.data.Insert(point.Timestamp, docBuffers[i])

		// 更新大小
		memTable.size++
		e.mu.Unlock()

		// 更新索引
		if e.asyncIndexUpdater != nil {
			// 使用BatchUpdateIndex方法更新索引
			seriesID := fmt.Sprintf("%s:%s:%d", dbName, tableName, point.Timestamp)
			err := e.asyncIndexUpdater.BatchUpdateIndex(dbName, tableName, point, seriesID, table.TagIndexes, "timestamp")
			if err != nil {
				log.Printf("Failed to update index: %v", err)
			}
		}
	}

	// 检查是否需要刷盘
	if memTable.ShouldFlush() {
		go func() {
			e.mu.Lock()
			defer e.mu.Unlock()

			// 创建新的内存表
			newMemTable := NewMemTable(64 * 1024 * 1024) // 64MB

			// 替换旧的内存表
			key := dbName + ":" + tableName
			oldMemTable := e.memTables[key]
			e.memTables[key] = newMemTable

			// 异步刷盘
			go func(mt *MemTable) {
				// Create a MemTableInfo for the flush operation
				memTableInfo := &MemTableInfo{
					DBName:   dbName,
					Table:    tableName,
					MemTable: mt,
				}

				if err := e.memTableManager.flushMemTable(memTableInfo); err != nil {
					log.Printf("Failed to flush memtable: %v", err)
				}
			}(oldMemTable)
		}()
	}

	return nil
}

// WritePoint 写入单个数据点
func (e *StorageEngine) WritePoint(dbName, tableName string, point *model.TimeSeriesPoint) error {
	// 使用批处理器处理单点写入
	if e.batchProcessor != nil {
		return e.batchProcessor.Add(dbName, tableName, point)
	}

	// 如果批处理器不可用，使用同步写入
	return e.InsertPointSync(dbName, tableName, point)
}

// WriteBatch 写入批量数据点
func (e *StorageEngine) WriteBatch(dbName, tableName string, points []*model.TimeSeriesPoint) error {
	// 使用批处理器处理批量写入
	if e.batchProcessor != nil {
		return e.batchProcessor.AddBatch(dbName, tableName, points)
	}

	// 如果批处理器不可用，使用同步写入
	return e.InsertPointsSync(dbName, tableName, points)
}

// FlushBatchProcessor 刷新批处理器
func (e *StorageEngine) FlushBatchProcessor() error {
	if e.batchProcessor != nil {
		return e.batchProcessor.Flush()
	}
	return nil
}
