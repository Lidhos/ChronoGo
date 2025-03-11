package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"ChronoGo/pkg/index"
	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/model"
)

// IndexUpdateEntry 表示一个索引更新条目
type IndexUpdateEntry struct {
	Database  string
	Table     string
	IndexName string
	Key       interface{}
	SeriesID  string
}

// IndexUpdateEntryPool 是IndexUpdateEntry对象池
type IndexUpdateEntryPool struct {
	pool sync.Pool
}

// NewIndexUpdateEntryPool 创建一个新的IndexUpdateEntry对象池
func NewIndexUpdateEntryPool() *IndexUpdateEntryPool {
	return &IndexUpdateEntryPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &IndexUpdateEntry{}
			},
		},
	}
}

// Get 从池中获取一个IndexUpdateEntry
func (p *IndexUpdateEntryPool) Get() *IndexUpdateEntry {
	return p.pool.Get().(*IndexUpdateEntry)
}

// Put 将IndexUpdateEntry放回池中
func (p *IndexUpdateEntryPool) Put(entry *IndexUpdateEntry) {
	// 清空对象，避免内存泄漏
	entry.Database = ""
	entry.Table = ""
	entry.IndexName = ""
	entry.Key = nil
	entry.SeriesID = ""
	p.pool.Put(entry)
}

// AsyncIndexUpdater 表示异步索引更新器
type AsyncIndexUpdater struct {
	indexMgr       index.IndexManager     // 索引管理器
	entryChan      chan *IndexUpdateEntry // 条目通道
	batchSize      int                    // 批处理大小
	flushInterval  time.Duration          // 刷新间隔
	workers        int                    // 工作线程数
	wg             sync.WaitGroup         // 等待组
	ctx            context.Context        // 上下文
	cancel         context.CancelFunc     // 取消函数
	mu             sync.Mutex             // 互斥锁
	buffer         []*IndexUpdateEntry    // 条目缓冲区
	updateCounters map[string]int         // 更新计数器
	entryPool      *IndexUpdateEntryPool  // 对象池
}

// NewAsyncIndexUpdater 创建新的异步索引更新器
func NewAsyncIndexUpdater(indexMgr index.IndexManager, queueSize int, batchSize int, flushInterval time.Duration, workers int) *AsyncIndexUpdater {
	ctx, cancel := context.WithCancel(context.Background())

	updater := &AsyncIndexUpdater{
		indexMgr:       indexMgr,
		entryChan:      make(chan *IndexUpdateEntry, queueSize),
		batchSize:      batchSize,
		flushInterval:  flushInterval,
		workers:        workers,
		ctx:            ctx,
		cancel:         cancel,
		buffer:         make([]*IndexUpdateEntry, 0, batchSize),
		updateCounters: make(map[string]int),
		entryPool:      NewIndexUpdateEntryPool(),
	}

	// 启动工作线程
	for i := 0; i < workers; i++ {
		updater.wg.Add(1)
		go updater.worker(i)
	}

	// 启动定期刷新线程
	updater.wg.Add(1)
	go updater.periodicFlusher()

	logger.Printf("AsyncIndexUpdater: 已启动 %d 个工作线程", workers)
	return updater
}

// UpdateIndex 异步更新索引
func (au *AsyncIndexUpdater) UpdateIndex(dbName, tableName, indexName string, key interface{}, seriesID string) error {
	// 从对象池获取条目
	entry := au.entryPool.Get()
	entry.Database = dbName
	entry.Table = tableName
	entry.IndexName = indexName
	entry.Key = key
	entry.SeriesID = seriesID

	// 尝试发送到通道，如果通道已满则阻塞
	select {
	case au.entryChan <- entry:
		return nil
	case <-au.ctx.Done():
		// 如果上下文已取消，将条目放回池中
		au.entryPool.Put(entry)
		return fmt.Errorf("async index updater is closed")
	}
}

// BatchUpdateIndex 批量异步更新索引
func (au *AsyncIndexUpdater) BatchUpdateIndex(dbName, tableName string, point *model.TimeSeriesPoint, seriesID string, tagIndexes []model.TagIndex, timeField string) error {
	// 创建一个批量更新的条目列表
	entries := make([]*IndexUpdateEntry, 0, len(tagIndexes)+1) // +1 for time index

	// 处理标签索引
	for _, tagIndex := range tagIndexes {
		// 根据索引字段数量决定如何更新索引
		if len(tagIndex.Fields) == 1 {
			// 单字段索引
			field := tagIndex.Fields[0]
			if value, ok := point.Tags[field]; ok {
				// 从对象池获取条目
				entry := au.entryPool.Get()
				entry.Database = dbName
				entry.Table = tableName
				entry.IndexName = tagIndex.Name
				entry.Key = value
				entry.SeriesID = seriesID
				entries = append(entries, entry)
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

			// 从对象池获取条目
			entry := au.entryPool.Get()
			entry.Database = dbName
			entry.Table = tableName
			entry.IndexName = tagIndex.Name
			entry.Key = values
			entry.SeriesID = seriesID
			entries = append(entries, entry)
		}
	}

	// 处理时间索引
	if timeField != "" {
		// 从对象池获取条目
		entry := au.entryPool.Get()
		entry.Database = dbName
		entry.Table = tableName
		entry.IndexName = "time_idx"
		entry.Key = point.Timestamp
		entry.SeriesID = seriesID
		entries = append(entries, entry)
	}

	// 批量发送到通道
	for _, entry := range entries {
		select {
		case au.entryChan <- entry:
			// 成功发送
		case <-au.ctx.Done():
			// 如果上下文已取消，将剩余条目放回池中
			for _, e := range entries {
				au.entryPool.Put(e)
			}
			return fmt.Errorf("async index updater is closed")
		}
	}

	return nil
}

// BatchUpdateIndexForPoints 为多个数据点批量更新索引
func (au *AsyncIndexUpdater) BatchUpdateIndexForPoints(dbName, tableName string, points []*model.TimeSeriesPoint, seriesIDs []string, tagIndexes []model.TagIndex, timeField string) error {
	if len(points) == 0 || len(points) != len(seriesIDs) {
		return fmt.Errorf("invalid points or seriesIDs")
	}

	// 创建索引条目映射
	entriesMap := make(map[string][]*IndexUpdateEntry)

	// 为每个点创建索引更新条目
	for i, point := range points {
		seriesID := seriesIDs[i]

		// 处理标签索引
		for _, tagIndex := range tagIndexes {
			indexKey := fmt.Sprintf("%s:%s:%s", dbName, tableName, tagIndex.Name)

			// 根据索引字段数量决定如何更新索引
			if len(tagIndex.Fields) == 1 {
				// 单字段索引
				field := tagIndex.Fields[0]
				if value, ok := point.Tags[field]; ok {
					// 从对象池获取条目
					entry := au.entryPool.Get()
					entry.Database = dbName
					entry.Table = tableName
					entry.IndexName = tagIndex.Name
					entry.Key = value
					entry.SeriesID = seriesID
					entriesMap[indexKey] = append(entriesMap[indexKey], entry)
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

				// 从对象池获取条目
				entry := au.entryPool.Get()
				entry.Database = dbName
				entry.Table = tableName
				entry.IndexName = tagIndex.Name
				entry.Key = values
				entry.SeriesID = seriesID
				entriesMap[indexKey] = append(entriesMap[indexKey], entry)
			}
		}

		// 处理时间索引
		if timeField != "" {
			timeIndexKey := fmt.Sprintf("%s:%s:time_idx", dbName, tableName)

			// 从对象池获取条目
			entry := au.entryPool.Get()
			entry.Database = dbName
			entry.Table = tableName
			entry.IndexName = "time_idx"
			entry.Key = point.Timestamp
			entry.SeriesID = seriesID
			entriesMap[timeIndexKey] = append(entriesMap[timeIndexKey], entry)
		}
	}

	// 合并相同键的更新
	for indexKey, entries := range entriesMap {
		// 创建一个映射来存储每个键的最新值
		keyMap := make(map[interface{}]string)
		keyOrder := make([]interface{}, 0, len(entries))
		keyExists := make(map[interface{}]bool)

		// 遍历所有条目，保留每个键的最新值
		for _, entry := range entries {
			if !keyExists[entry.Key] {
				keyOrder = append(keyOrder, entry.Key)
				keyExists[entry.Key] = true
			}
			keyMap[entry.Key] = entry.SeriesID
		}

		// 创建合并后的条目
		for _, key := range keyOrder {
			// 从对象池获取条目
			entry := au.entryPool.Get()

			// 解析索引键
			parts := strings.Split(indexKey, ":")
			if len(parts) != 3 {
				au.entryPool.Put(entry)
				continue
			}

			entry.Database = parts[0]
			entry.Table = parts[1]
			entry.IndexName = parts[2]
			entry.Key = key
			entry.SeriesID = keyMap[key]

			// 发送到通道
			select {
			case au.entryChan <- entry:
				// 成功发送
			case <-au.ctx.Done():
				// 如果上下文已取消，将条目放回池中
				au.entryPool.Put(entry)
				// 释放所有已分配但未发送的条目
				for _, e := range entries {
					au.entryPool.Put(e)
				}
				return fmt.Errorf("async index updater is closed")
			}
		}

		// 释放原始条目
		for _, entry := range entries {
			au.entryPool.Put(entry)
		}
	}

	return nil
}

// worker 工作线程函数
func (au *AsyncIndexUpdater) worker(id int) {
	defer au.wg.Done()
	logger.Printf("AsyncIndexUpdater: 工作线程 %d 已启动", id)

	for {
		select {
		case entry := <-au.entryChan:
			// 处理索引更新条目
			au.mu.Lock()
			au.buffer = append(au.buffer, entry)

			// 如果达到批处理大小，则刷新缓冲区
			if len(au.buffer) >= au.batchSize {
				bufferCopy := make([]*IndexUpdateEntry, len(au.buffer))
				copy(bufferCopy, au.buffer)
				au.buffer = au.buffer[:0]
				au.mu.Unlock()

				// 批量更新索引
				au.processBatch(bufferCopy)

				// 将条目放回池中
				for _, e := range bufferCopy {
					au.entryPool.Put(e)
				}
			} else {
				au.mu.Unlock()
			}
		case <-au.ctx.Done():
			logger.Printf("AsyncIndexUpdater: 工作线程 %d 正在关闭", id)
			return
		}
	}
}

// periodicFlusher 定期刷新线程
func (au *AsyncIndexUpdater) periodicFlusher() {
	defer au.wg.Done()
	logger.Printf("AsyncIndexUpdater: 定期刷新线程已启动，间隔 %v", au.flushInterval)

	ticker := time.NewTicker(au.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			au.mu.Lock()
			if len(au.buffer) > 0 {
				bufferCopy := make([]*IndexUpdateEntry, len(au.buffer))
				copy(bufferCopy, au.buffer)
				au.buffer = au.buffer[:0]
				au.mu.Unlock()

				// 批量更新索引
				au.processBatch(bufferCopy)

				// 将条目放回池中
				for _, e := range bufferCopy {
					au.entryPool.Put(e)
				}
			} else {
				au.mu.Unlock()
			}
		case <-au.ctx.Done():
			logger.Printf("AsyncIndexUpdater: 定期刷新线程正在关闭")
			return
		}
	}
}

// processBatch 处理一批索引更新条目
func (au *AsyncIndexUpdater) processBatch(entries []*IndexUpdateEntry) {
	if len(entries) == 0 {
		return
	}

	// 按索引分组并合并相同键的更新
	indexGroups := make(map[string]map[interface{}]string)
	indexKeys := make(map[string][]interface{})

	// 第一步：按索引分组并合并相同键的更新
	for _, entry := range entries {
		key := fmt.Sprintf("%s:%s:%s", entry.Database, entry.Table, entry.IndexName)

		// 如果索引组不存在，创建它
		if _, exists := indexGroups[key]; !exists {
			indexGroups[key] = make(map[interface{}]string)
			indexKeys[key] = make([]interface{}, 0)
		}

		// 如果键不存在，添加到键列表中
		if _, exists := indexGroups[key][entry.Key]; !exists {
			indexKeys[key] = append(indexKeys[key], entry.Key)
		}

		// 更新或添加键值对（使用最新的SeriesID）
		indexGroups[key][entry.Key] = entry.SeriesID
	}

	// 批量更新每个索引
	ctx := context.Background()
	for key, group := range indexGroups {
		// 解析键
		parts := strings.Split(key, ":")
		if len(parts) != 3 {
			logger.Printf("AsyncIndexUpdater: 无效的索引键 %s", key)
			continue
		}

		dbName := parts[0]
		tableName := parts[1]
		indexName := parts[2]

		// 获取索引
		idx, err := au.indexMgr.GetIndex(ctx, dbName, tableName, indexName)
		if err != nil {
			logger.Printf("AsyncIndexUpdater: 获取索引 %s 失败: %v", key, err)
			continue
		}

		// 批量更新索引
		keyCount := len(group)
		logger.Printf("AsyncIndexUpdater: 批量更新索引 %s，%d 个唯一键", key, keyCount)

		// 记录更新计数
		au.mu.Lock()
		au.updateCounters[key] += keyCount
		au.mu.Unlock()

		// 按照原始顺序更新索引，确保一致性
		for _, k := range indexKeys[key] {
			seriesID := group[k]
			if err := idx.Insert(ctx, k, seriesID); err != nil {
				logger.Printf("AsyncIndexUpdater: 更新索引 %s 失败: %v", key, err)
			}
		}
	}
}

// GetUpdateStats 获取更新统计信息
func (au *AsyncIndexUpdater) GetUpdateStats() map[string]int {
	au.mu.Lock()
	defer au.mu.Unlock()

	// 创建副本
	stats := make(map[string]int)
	for k, v := range au.updateCounters {
		stats[k] = v
	}

	return stats
}

// Sync 同步所有待处理的索引更新
func (au *AsyncIndexUpdater) Sync() error {
	logger.Printf("AsyncIndexUpdater: 同步所有待处理的索引更新")

	// 处理缓冲区中的条目
	au.mu.Lock()
	if len(au.buffer) > 0 {
		bufferCopy := make([]*IndexUpdateEntry, len(au.buffer))
		copy(bufferCopy, au.buffer)
		au.buffer = au.buffer[:0]
		au.mu.Unlock()

		// 批量更新索引
		au.processBatch(bufferCopy)

		// 将条目放回池中
		for _, e := range bufferCopy {
			au.entryPool.Put(e)
		}
	} else {
		au.mu.Unlock()
	}

	// 等待通道中的所有条目被处理
	for {
		if len(au.entryChan) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

// Close 关闭异步索引更新器
func (au *AsyncIndexUpdater) Close() error {
	logger.Printf("AsyncIndexUpdater: 正在关闭")

	// 取消上下文
	au.cancel()

	// 同步所有待处理的索引更新
	au.Sync()

	// 等待所有工作线程退出
	au.wg.Wait()

	logger.Printf("AsyncIndexUpdater: 已关闭")
	return nil
}
