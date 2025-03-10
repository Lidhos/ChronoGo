package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

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
	flushTicker *time.Ticker               // 刷盘定时器
	mu          sync.RWMutex               // 读写锁
	closing     chan struct{}              // 关闭信号
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

	engine := &StorageEngine{
		dataDir:   dataDir,
		walDir:    walDir,
		metaDir:   metaDir,
		databases: make(map[string]*model.Database),
		memTables: make(map[string]*MemTable),
		wal:       wal,
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

// loadMetadata 加载元数据
func (e *StorageEngine) loadMetadata() error {
	// TODO: 实现元数据加载
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

	// TODO: 持久化元数据

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

	// TODO: 持久化元数据

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

	_, tableExists := db.Tables[tableName]
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

	// 获取内存表
	memTable, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get memtable: %w", err)
	}

	// 从内存表查询
	result := memTable.QueryByTimeRange(start, end)

	// TODO: 从磁盘文件查询并合并结果

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

	// 获取内存表
	memTable, err := e.getOrCreateMemTable(dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get memtable: %w", err)
	}

	// 从内存表查询
	result := memTable.QueryByTag(tagKey, tagValue)

	// TODO: 从磁盘文件查询并合并结果

	return result, nil
}

// Close 关闭存储引擎
func (e *StorageEngine) Close() error {
	// 停止后台任务
	close(e.closing)
	if e.flushTicker != nil {
		e.flushTicker.Stop()
	}

	// 刷新所有内存表
	e.flushMemTables()

	// 关闭WAL
	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
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

	// 删除数据库中的所有表
	for tableName := range e.databases[dbName].Tables {
		// 删除内存表
		delete(e.memTables, dbName+":"+tableName)

		// TODO: 删除磁盘文件
	}

	// 删除数据库
	delete(e.databases, dbName)

	// TODO: 删除元数据文件

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

	// 删除内存表
	delete(e.memTables, dbName+":"+tableName)

	// 删除表定义
	delete(db.Tables, tableName)

	// TODO: 删除磁盘文件

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
