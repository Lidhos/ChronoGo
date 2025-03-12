package storage

import (
	"ChronoGo/pkg/model"
	"ChronoGo/pkg/util"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// BatchProcessor 批处理器
type BatchProcessor struct {
	engine        *StorageEngine
	batchSize     int               // 批处理大小
	flushInterval time.Duration     // 刷新间隔
	batchPool     *BatchPool        // 批处理对象池
	workerPool    *util.WorkerPool  // 工作线程池
	mu            sync.Mutex        // 互斥锁
	batches       map[string]*Batch // 当前批次 (db:table -> batch)
	flushTicker   *time.Ticker      // 刷新定时器
	closing       chan struct{}     // 关闭信号
	objectPools   *util.ObjectPools // 对象池
}

// Batch 表示一个批处理
type Batch struct {
	dbName    string                   // 数据库名
	tableName string                   // 表名
	points    []*model.TimeSeriesPoint // 数据点
	size      int                      // 当前大小
	capacity  int                      // 容量
	mu        sync.Mutex               // 互斥锁
}

// BatchPool 批处理对象池
type BatchPool struct {
	pool *util.ObjectPool
}

// NewBatchPool 创建新的批处理对象池
func NewBatchPool(batchSize int) *BatchPool {
	return &BatchPool{
		pool: util.NewObjectPool(
			func() interface{} {
				return &Batch{
					points:   make([]*model.TimeSeriesPoint, 0, batchSize),
					capacity: batchSize,
				}
			},
			func(obj interface{}) {
				batch := obj.(*Batch)
				batch.dbName = ""
				batch.tableName = ""
				batch.points = batch.points[:0]
				batch.size = 0
			},
		),
	}
}

// Get 获取批处理
func (p *BatchPool) Get(dbName, tableName string) *Batch {
	batch := p.pool.Get().(*Batch)
	batch.dbName = dbName
	batch.tableName = tableName
	return batch
}

// Put 放回批处理
func (p *BatchPool) Put(batch *Batch) {
	if batch == nil {
		return
	}
	p.pool.Put(batch)
}

// NewBatchProcessor 创建新的批处理器
func NewBatchProcessor(engine *StorageEngine, batchSize int, flushInterval time.Duration, workerCount int) *BatchProcessor {
	if batchSize <= 0 {
		batchSize = 1000 // 默认批处理大小
	}

	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond // 默认刷新间隔
	}

	if workerCount <= 0 {
		workerCount = 4 // 默认工作线程数
	}

	processor := &BatchProcessor{
		engine:        engine,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		batchPool:     NewBatchPool(batchSize),
		workerPool:    util.NewWorkerPool(workerCount),
		batches:       make(map[string]*Batch),
		flushTicker:   time.NewTicker(flushInterval),
		closing:       make(chan struct{}),
		objectPools:   engine.objectPools,
	}

	// 启动后台刷新协程
	go processor.backgroundFlush()

	return processor
}

// Add 添加数据点到批处理
func (p *BatchProcessor) Add(dbName, tableName string, point *model.TimeSeriesPoint) error {
	key := dbName + ":" + tableName

	p.mu.Lock()
	batch, ok := p.batches[key]
	if !ok {
		batch = p.batchPool.Get(dbName, tableName)
		p.batches[key] = batch
	}
	p.mu.Unlock()

	batch.mu.Lock()
	defer batch.mu.Unlock()

	batch.points = append(batch.points, point)
	batch.size++

	// 如果达到批处理大小，则刷新
	if batch.size >= p.batchSize {
		return p.flushBatch(key, batch)
	}

	return nil
}

// AddBatch 添加多个数据点到批处理
func (p *BatchProcessor) AddBatch(dbName, tableName string, points []*model.TimeSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}

	// 如果点数超过批处理大小，直接处理
	if len(points) >= p.batchSize {
		// 对点进行排序（按时间戳）
		sortedPoints := make([]*model.TimeSeriesPoint, len(points))
		copy(sortedPoints, points)
		sort.Slice(sortedPoints, func(i, j int) bool {
			return sortedPoints[i].Timestamp < sortedPoints[j].Timestamp
		})

		// 直接处理批量点
		return p.processBatch(dbName, tableName, sortedPoints)
	}

	// 否则添加到现有批处理
	key := dbName + ":" + tableName

	p.mu.Lock()
	batch, ok := p.batches[key]
	if !ok {
		batch = p.batchPool.Get(dbName, tableName)
		p.batches[key] = batch
	}
	p.mu.Unlock()

	batch.mu.Lock()
	defer batch.mu.Unlock()

	// 检查是否需要扩容
	requiredCapacity := batch.size + len(points)
	if requiredCapacity > batch.capacity {
		newCapacity := batch.capacity * 2
		if newCapacity < requiredCapacity {
			newCapacity = requiredCapacity
		}

		newPoints := make([]*model.TimeSeriesPoint, len(batch.points), newCapacity)
		copy(newPoints, batch.points)
		batch.points = newPoints
		batch.capacity = newCapacity
	}

	// 添加点
	batch.points = append(batch.points, points...)
	batch.size += len(points)

	// 如果达到批处理大小，则刷新
	if batch.size >= p.batchSize {
		return p.flushBatch(key, batch)
	}

	return nil
}

// Flush 刷新所有批处理
func (p *BatchProcessor) Flush() error {
	p.mu.Lock()
	batches := p.batches
	p.batches = make(map[string]*Batch)
	p.mu.Unlock()

	var lastErr error
	var wg sync.WaitGroup

	// 并行处理每个批次
	for key, batch := range batches {
		wg.Add(1)

		// 捕获变量
		currentKey := key
		currentBatch := batch

		// 提交到工作线程池
		p.workerPool.Submit(func() {
			defer wg.Done()

			err := p.flushBatch(currentKey, currentBatch)
			if err != nil {
				lastErr = err
			}
		})
	}

	// 等待所有批次处理完成
	wg.Wait()

	return lastErr
}

// flushBatch 刷新单个批处理
func (p *BatchProcessor) flushBatch(key string, batch *Batch) error {
	batch.mu.Lock()
	points := batch.points
	dbName := batch.dbName
	tableName := batch.tableName

	// 重置批处理
	batch.points = make([]*model.TimeSeriesPoint, 0, batch.capacity)
	batch.size = 0
	batch.mu.Unlock()

	// 从映射中移除批处理
	p.mu.Lock()
	delete(p.batches, key)
	p.mu.Unlock()

	// 处理批处理
	err := p.processBatch(dbName, tableName, points)

	// 放回对象池
	p.batchPool.Put(batch)

	return err
}

// processBatch 处理批处理
func (p *BatchProcessor) processBatch(dbName, tableName string, points []*model.TimeSeriesPoint) error {
	if len(points) == 0 {
		return nil
	}

	// 获取数据库和表
	db, exists := p.engine.databases[dbName]
	if !exists {
		return fmt.Errorf("database not found: %s", dbName)
	}

	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table not found: %s.%s", dbName, tableName)
	}

	// 获取或创建内存表
	key := dbName + ":" + tableName
	p.engine.mu.Lock()
	memTable, exists := p.engine.memTables[key]
	if !exists {
		// 创建新的内存表
		memTable = NewMemTable(10 * 1024 * 1024) // 默认10MB
		p.engine.memTables[key] = memTable
	}
	p.engine.mu.Unlock()

	// 批量写入内存表
	err := memTable.PutBatch(points)
	if err != nil {
		return err
	}

	// 写入WAL
	if p.engine.wal != nil {
		for _, point := range points {
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

			// 写入WAL
			if p.engine.asyncWAL != nil {
				p.engine.asyncWAL.Write(entry)
			} else {
				if err := p.engine.wal.Write(entry); err != nil {
					return err
				}
			}
		}
	}

	// 更新索引
	if p.engine.asyncIndexUpdater != nil {
		for _, point := range points {
			// 使用BatchUpdateIndex方法更新索引
			seriesID := fmt.Sprintf("%s:%s:%d", dbName, tableName, point.Timestamp)
			err := p.engine.asyncIndexUpdater.BatchUpdateIndex(dbName, tableName, point, seriesID, table.TagIndexes, "timestamp")
			if err != nil {
				log.Printf("Failed to update index: %v", err)
			}
		}
	}

	// 检查是否需要刷盘
	if memTable.ShouldFlush() {
		go func() {
			p.engine.mu.Lock()

			// 创建新的内存表
			newMemTable := NewMemTable(10 * 1024 * 1024) // 默认10MB

			// 替换旧的内存表
			oldMemTable := p.engine.memTables[key]
			p.engine.memTables[key] = newMemTable
			p.engine.mu.Unlock()

			// 异步刷盘
			go func(mt *MemTable) {
				// 获取所有数据点
				points := mt.GetAll()
				if len(points) == 0 {
					return // 没有数据，不需要刷盘
				}

				// 如果有MemTableManager，使用它来刷盘
				if p.engine.memTableManager != nil {
					// 创建临时MemTableInfo
					memTableInfo := &MemTableInfo{
						DBName:   dbName,
						Table:    tableName,
						MemTable: mt,
						CreateAt: time.Now(),
					}

					// 使用MemTableManager刷盘
					if err := p.engine.memTableManager.flushMemTable(memTableInfo); err != nil {
						log.Printf("Failed to flush memtable: %v", err)
					}
				} else {
					// 否则使用旧的方式刷盘
					// 创建表数据文件
					tableDataFile, err := NewTableDataFile(p.engine.dataDir, dbName, tableName)
					if err != nil {
						log.Printf("Failed to create table data file: %v", err)
						return
					}
					defer tableDataFile.Close()

					// 写入数据点
					if err := tableDataFile.WritePoints(points); err != nil {
						log.Printf("Failed to write points: %v", err)
					}
				}

				// 清空内存表
				mt.Clear()
			}(oldMemTable)
		}()
	}

	return nil
}

// backgroundFlush 后台刷新协程
func (p *BatchProcessor) backgroundFlush() {
	for {
		select {
		case <-p.flushTicker.C:
			// 定时刷新
			_ = p.Flush()
		case <-p.closing:
			// 关闭信号
			p.flushTicker.Stop()
			_ = p.Flush()
			return
		}
	}
}

// Close 关闭批处理器
func (p *BatchProcessor) Close() error {
	close(p.closing)
	return p.Flush()
}
