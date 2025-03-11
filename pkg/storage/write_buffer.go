package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/model"
)

// WriteBufferKey 表示写缓冲区的键
type WriteBufferKey struct {
	Database string
	Table    string
}

// WriteBuffer 表示写缓冲区
type WriteBuffer struct {
	engine        *StorageEngine                              // 存储引擎
	buffers       map[WriteBufferKey][]*model.TimeSeriesPoint // 缓冲区映射
	mu            sync.Mutex                                  // 互斥锁
	batchSize     int                                         // 批处理大小
	flushInterval time.Duration                               // 刷新间隔
	ctx           context.Context                             // 上下文
	cancel        context.CancelFunc                          // 取消函数
	wg            sync.WaitGroup                              // 等待组
}

// NewWriteBuffer 创建新的写缓冲区
func NewWriteBuffer(engine *StorageEngine, batchSize int, flushInterval time.Duration) *WriteBuffer {
	ctx, cancel := context.WithCancel(context.Background())

	wb := &WriteBuffer{
		engine:        engine,
		buffers:       make(map[WriteBufferKey][]*model.TimeSeriesPoint),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		ctx:           ctx,
		cancel:        cancel,
	}

	// 启动定期刷新线程
	wb.wg.Add(1)
	go wb.periodicFlusher()

	return wb
}

// Add 添加数据点到缓冲区
func (wb *WriteBuffer) Add(dbName, tableName string, point *model.TimeSeriesPoint) error {
	key := WriteBufferKey{
		Database: dbName,
		Table:    tableName,
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 添加到缓冲区
	wb.buffers[key] = append(wb.buffers[key], point)

	// 如果达到批处理大小，触发异步刷新
	if len(wb.buffers[key]) >= wb.batchSize {
		// 创建一个副本用于刷新
		pointsToFlush := make([]*model.TimeSeriesPoint, len(wb.buffers[key]))
		copy(pointsToFlush, wb.buffers[key])

		// 清空缓冲区
		delete(wb.buffers, key)

		// 异步刷新
		go func(k WriteBufferKey, pts []*model.TimeSeriesPoint) {
			if err := wb.engine.InsertPointsSync(k.Database, k.Table, pts); err != nil {
				logger.Printf("WriteBuffer: 异步刷新失败: %v", err)
			}
		}(key, pointsToFlush)
	}

	return nil
}

// periodicFlusher 定期刷新线程
func (wb *WriteBuffer) periodicFlusher() {
	defer wb.wg.Done()

	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	logger.Printf("WriteBuffer: 定期刷新线程启动，间隔 %v", wb.flushInterval)

	for {
		select {
		case <-wb.ctx.Done():
			logger.Printf("WriteBuffer: 定期刷新线程退出")
			return
		case <-ticker.C:
			wb.FlushAll()
		}
	}
}

// flushBuffer 刷新指定键的缓冲区
// 调用者必须持有锁
func (wb *WriteBuffer) flushBuffer(key WriteBufferKey) error {
	points := wb.buffers[key]
	if len(points) == 0 {
		return nil
	}

	logger.Printf("WriteBuffer: 刷新 %s.%s 的 %d 个数据点", key.Database, key.Table, len(points))

	// 批量插入数据点，直接调用同步插入方法避免循环调用
	err := wb.engine.InsertPointsSync(key.Database, key.Table, points)
	if err != nil {
		return fmt.Errorf("failed to insert points: %w", err)
	}

	// 清空缓冲区
	delete(wb.buffers, key)

	return nil
}

// FlushAll 刷新所有缓冲区
func (wb *WriteBuffer) FlushAll() {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	for key := range wb.buffers {
		if err := wb.flushBuffer(key); err != nil {
			logger.Printf("WriteBuffer: 刷新 %s.%s 失败: %v", key.Database, key.Table, err)
		}
	}
}

// Close 关闭写缓冲区
func (wb *WriteBuffer) Close() error {
	// 取消上下文
	wb.cancel()

	// 等待所有线程退出
	wb.wg.Wait()

	// 刷新所有缓冲区
	wb.FlushAll()

	return nil
}
