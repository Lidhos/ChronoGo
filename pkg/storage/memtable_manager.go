package storage

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/model"
)

// MemTableState 表示内存表的状态
type MemTableState int

const (
	// MemTableMutable 表示可写的内存表
	MemTableMutable MemTableState = iota
	// MemTableImmutable 表示只读的内存表，等待刷盘
	MemTableImmutable
	// MemTableFlushing 表示正在刷盘的内存表
	MemTableFlushing
)

// MemTableInfo 表示内存表信息
type MemTableInfo struct {
	MemTable *MemTable     // 内存表
	State    MemTableState // 状态
	DBName   string        // 数据库名
	Table    string        // 表名
	CreateAt time.Time     // 创建时间
	UpdateAt time.Time     // 最后更新时间
	FlushAt  time.Time     // 最后刷盘时间
}

// MemTableManager 管理多个内存表
type MemTableManager struct {
	// 数据目录
	dataDir string
	// 内存表映射 (db:table -> [mutable, immutable1, immutable2, ...])
	memTables map[string][]*MemTableInfo
	// 最大内存表大小
	maxTableSize int64
	// 最大内存表数量
	maxImmutableTables int
	// 互斥锁
	mu sync.RWMutex
	// 刷盘通道
	flushChan chan *MemTableInfo
	// 刷盘工作线程数
	flushWorkers int
	// 等待组
	wg sync.WaitGroup
	// 关闭信号
	closing chan struct{}
	// 存储引擎引用（用于刷盘）
	engine *StorageEngine
}

// NewMemTableManager 创建新的内存表管理器
func NewMemTableManager(dataDir string, maxTableSize int64, maxImmutableTables, flushWorkers int, engine *StorageEngine) *MemTableManager {
	if maxTableSize <= 0 {
		maxTableSize = 64 * 1024 * 1024 // 默认 64MB
	}
	if maxImmutableTables <= 0 {
		maxImmutableTables = 2 // 默认最多 2 个只读内存表
	}
	if flushWorkers <= 0 {
		flushWorkers = 2 // 默认 2 个刷盘工作线程
	}

	mtm := &MemTableManager{
		dataDir:            dataDir,
		memTables:          make(map[string][]*MemTableInfo),
		maxTableSize:       maxTableSize,
		maxImmutableTables: maxImmutableTables,
		flushChan:          make(chan *MemTableInfo, maxImmutableTables*10), // 缓冲区大小为最大只读表数量的 10 倍
		flushWorkers:       flushWorkers,
		closing:            make(chan struct{}),
		engine:             engine,
	}

	// 启动刷盘工作线程
	mtm.startFlushWorkers()

	return mtm
}

// startFlushWorkers 启动刷盘工作线程
func (mtm *MemTableManager) startFlushWorkers() {
	for i := 0; i < mtm.flushWorkers; i++ {
		mtm.wg.Add(1)
		go mtm.flushWorker(i)
	}
}

// flushWorker 刷盘工作线程
func (mtm *MemTableManager) flushWorker(id int) {
	defer mtm.wg.Done()

	logger.Printf("MemTableManager: 刷盘工作线程 %d 启动", id)

	for {
		select {
		case <-mtm.closing:
			logger.Printf("MemTableManager: 刷盘工作线程 %d 退出", id)
			return
		case memTableInfo := <-mtm.flushChan:
			// 更新状态为正在刷盘
			mtm.mu.Lock()
			memTableInfo.State = MemTableFlushing
			mtm.mu.Unlock()

			// 执行刷盘操作
			startTime := time.Now()
			logger.Printf("MemTableManager: 开始刷盘 %s:%s (大小: %d)",
				memTableInfo.DBName, memTableInfo.Table, memTableInfo.MemTable.Size())

			if err := mtm.flushMemTable(memTableInfo); err != nil {
				logger.Printf("MemTableManager: 刷盘失败 %s:%s: %v",
					memTableInfo.DBName, memTableInfo.Table, err)
			} else {
				logger.Printf("MemTableManager: 刷盘成功 %s:%s, 耗时: %v",
					memTableInfo.DBName, memTableInfo.Table, time.Since(startTime))
			}

			// 刷盘完成后，从内存表列表中移除
			mtm.mu.Lock()
			key := memTableInfo.DBName + ":" + memTableInfo.Table
			if tables, ok := mtm.memTables[key]; ok {
				// 查找并移除该内存表
				for i, t := range tables {
					if t == memTableInfo {
						// 移除元素
						mtm.memTables[key] = append(tables[:i], tables[i+1:]...)
						break
					}
				}
			}
			mtm.mu.Unlock()
		}
	}
}

// flushMemTable 将内存表刷盘到SSTable文件
func (mtm *MemTableManager) flushMemTable(memTableInfo *MemTableInfo) error {
	// 检查内存表是否为空
	if memTableInfo.MemTable.Size() == 0 {
		return nil // 没有数据，不需要刷盘
	}

	// 使用 SSTableManager 创建 SSTable 文件
	if mtm.engine.sstableManager != nil {
		// 使用 SSTableManager 将内存表刷盘到 SSTable
		fileName, err := mtm.engine.sstableManager.CreateSSTableFromMemTable(
			memTableInfo.MemTable,
			memTableInfo.DBName,
			memTableInfo.Table,
		)
		if err != nil {
			return fmt.Errorf("failed to create SSTable from memtable: %w", err)
		}

		logger.Printf("MemTableManager: 成功将内存表刷盘到 SSTable 文件: %s", fileName)

		// 更新刷盘时间
		memTableInfo.FlushAt = time.Now()

		return nil
	}

	// 如果 SSTableManager 不可用，使用旧的刷盘方式
	// 获取所有数据点
	points := memTableInfo.MemTable.GetAll()
	if len(points) == 0 {
		return nil // 没有数据，不需要刷盘
	}

	// 创建表数据文件
	tableDataFile, err := NewTableDataFile(mtm.dataDir, memTableInfo.DBName, memTableInfo.Table)
	if err != nil {
		return fmt.Errorf("failed to create table data file: %w", err)
	}
	defer tableDataFile.Close()

	// 写入数据点
	if err := tableDataFile.WritePoints(points); err != nil {
		return fmt.Errorf("failed to write points: %w", err)
	}

	// 更新刷盘时间
	memTableInfo.FlushAt = time.Now()

	return nil
}

// GetOrCreateMutableTable 获取或创建可写内存表
func (mtm *MemTableManager) GetOrCreateMutableTable(dbName, tableName string) (*MemTable, error) {
	key := dbName + ":" + tableName

	mtm.mu.Lock()
	defer mtm.mu.Unlock()

	// 检查是否已有内存表
	if tables, ok := mtm.memTables[key]; ok && len(tables) > 0 {
		// 第一个应该是可写的内存表
		if tables[0].State == MemTableMutable {
			// 检查是否需要转换为只读
			if tables[0].MemTable.Size() >= mtm.maxTableSize {
				// 转换为只读
				tables[0].State = MemTableImmutable
				tables[0].UpdateAt = time.Now()

				// 检查只读表数量是否超过限制
				immutableCount := 0
				for _, t := range tables {
					if t.State == MemTableImmutable {
						immutableCount++
					}
				}

				// 如果只读表数量超过限制，触发刷盘
				if immutableCount >= mtm.maxImmutableTables {
					// 找到最旧的只读表
					var oldestTable *MemTableInfo
					for _, t := range tables {
						if t.State == MemTableImmutable {
							if oldestTable == nil || t.UpdateAt.Before(oldestTable.UpdateAt) {
								oldestTable = t
							}
						}
					}

					// 将最旧的只读表加入刷盘队列
					if oldestTable != nil {
						mtm.flushChan <- oldestTable
					}
				}

				// 创建新的可写内存表
				newTable := NewMemTable(mtm.maxTableSize)
				newTableInfo := &MemTableInfo{
					MemTable: newTable,
					State:    MemTableMutable,
					DBName:   dbName,
					Table:    tableName,
					CreateAt: time.Now(),
					UpdateAt: time.Now(),
				}

				// 将新表添加到列表头部
				mtm.memTables[key] = append([]*MemTableInfo{newTableInfo}, tables...)

				return newTable, nil
			}

			// 可写表未满，直接返回
			return tables[0].MemTable, nil
		}

		// 第一个表不是可写的，创建新的可写表
		newTable := NewMemTable(mtm.maxTableSize)
		newTableInfo := &MemTableInfo{
			MemTable: newTable,
			State:    MemTableMutable,
			DBName:   dbName,
			Table:    tableName,
			CreateAt: time.Now(),
			UpdateAt: time.Now(),
		}

		// 将新表添加到列表头部
		mtm.memTables[key] = append([]*MemTableInfo{newTableInfo}, tables...)

		return newTable, nil
	}

	// 没有内存表，创建新的
	newTable := NewMemTable(mtm.maxTableSize)
	newTableInfo := &MemTableInfo{
		MemTable: newTable,
		State:    MemTableMutable,
		DBName:   dbName,
		Table:    tableName,
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
	}

	// 创建新的列表
	mtm.memTables[key] = []*MemTableInfo{newTableInfo}

	return newTable, nil
}

// Put 插入数据点到内存表
func (mtm *MemTableManager) Put(dbName, tableName string, point *model.TimeSeriesPoint) error {
	// 获取或创建可写内存表
	memTable, err := mtm.GetOrCreateMutableTable(dbName, tableName)
	if err != nil {
		return err
	}

	// 插入数据点
	return memTable.Put(point)
}

// PutBatch 批量插入数据点到内存表
func (mtm *MemTableManager) PutBatch(dbName, tableName string, points []*model.TimeSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}

	// 获取或创建可写内存表
	memTable, err := mtm.GetOrCreateMutableTable(dbName, tableName)
	if err != nil {
		return err
	}

	// 批量插入数据点
	return memTable.PutBatch(points)
}

// QueryByTimeRange 按时间范围查询
func (mtm *MemTableManager) QueryByTimeRange(dbName, tableName string, start, end int64) []*model.TimeSeriesPoint {
	key := dbName + ":" + tableName

	mtm.mu.RLock()
	tables, ok := mtm.memTables[key]
	mtm.mu.RUnlock()

	if !ok || len(tables) == 0 {
		return nil
	}

	// 合并所有内存表的查询结果
	var result []*model.TimeSeriesPoint
	for _, tableInfo := range tables {
		points := tableInfo.MemTable.QueryByTimeRange(start, end)
		result = append(result, points...)
	}

	// 按时间戳排序
	if len(result) > 1 {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Timestamp < result[j].Timestamp
		})
	}

	return result
}

// QueryByTag 按标签查询
func (mtm *MemTableManager) QueryByTag(dbName, tableName string, tagKey, tagValue string) []*model.TimeSeriesPoint {
	key := dbName + ":" + tableName

	mtm.mu.RLock()
	tables, ok := mtm.memTables[key]
	mtm.mu.RUnlock()

	if !ok || len(tables) == 0 {
		return nil
	}

	// 合并所有内存表的查询结果
	var result []*model.TimeSeriesPoint
	for _, tableInfo := range tables {
		points := tableInfo.MemTable.QueryByTag(tagKey, tagValue)
		result = append(result, points...)
	}

	// 按时间戳排序
	if len(result) > 1 {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Timestamp < result[j].Timestamp
		})
	}

	return result
}

// FlushAll 刷盘所有内存表
func (mtm *MemTableManager) FlushAll() error {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()

	logger.Printf("MemTableManager: 开始刷盘所有内存表")
	startTime := time.Now()

	// 收集所有内存表
	var allTables []*MemTableInfo
	for _, tables := range mtm.memTables {
		for _, tableInfo := range tables {
			// 只处理可写和只读的内存表，跳过正在刷盘的
			if tableInfo.State != MemTableFlushing {
				// 将状态更新为只读
				tableInfo.State = MemTableImmutable
				allTables = append(allTables, tableInfo)
			}
		}
	}

	// 将所有内存表加入刷盘队列
	for _, tableInfo := range allTables {
		mtm.flushChan <- tableInfo
	}

	logger.Printf("MemTableManager: 已将 %d 个内存表加入刷盘队列，耗时: %v",
		len(allTables), time.Since(startTime))

	return nil
}

// Close 关闭内存表管理器
func (mtm *MemTableManager) Close() error {
	logger.Printf("MemTableManager: 开始关闭")
	startTime := time.Now()

	// 刷盘所有内存表
	if err := mtm.FlushAll(); err != nil {
		logger.Printf("MemTableManager: 刷盘所有内存表失败: %v", err)
	}

	// 发送关闭信号
	close(mtm.closing)

	// 等待所有工作线程退出
	mtm.wg.Wait()

	logger.Printf("MemTableManager: 关闭完成，耗时: %v", time.Since(startTime))
	return nil
}

// GetStats 获取内存表管理器统计信息
func (mtm *MemTableManager) GetStats() map[string]interface{} {
	mtm.mu.RLock()
	defer mtm.mu.RUnlock()

	stats := make(map[string]interface{})
	tableStats := make(map[string]interface{})

	var totalSize int64
	var totalTables, mutableTables, immutableTables, flushingTables int

	for key, tables := range mtm.memTables {
		tableInfo := make(map[string]interface{})
		var tableSize int64
		var tableMutable, tableImmutable, tableFlushing int

		for _, t := range tables {
			tableSize += t.MemTable.Size()
			switch t.State {
			case MemTableMutable:
				tableMutable++
				mutableTables++
			case MemTableImmutable:
				tableImmutable++
				immutableTables++
			case MemTableFlushing:
				tableFlushing++
				flushingTables++
			}
		}

		totalSize += tableSize
		totalTables += len(tables)

		tableInfo["size"] = tableSize
		tableInfo["count"] = len(tables)
		tableInfo["mutable"] = tableMutable
		tableInfo["immutable"] = tableImmutable
		tableInfo["flushing"] = tableFlushing

		tableStats[key] = tableInfo
	}

	stats["tables"] = tableStats
	stats["totalSize"] = totalSize
	stats["totalTables"] = totalTables
	stats["mutableTables"] = mutableTables
	stats["immutableTables"] = immutableTables
	stats["flushingTables"] = flushingTables
	stats["maxTableSize"] = mtm.maxTableSize
	stats["maxImmutableTables"] = mtm.maxImmutableTables
	stats["flushWorkers"] = mtm.flushWorkers
	stats["flushQueueSize"] = len(mtm.flushChan)
	stats["flushQueueCapacity"] = cap(mtm.flushChan)

	return stats
}
