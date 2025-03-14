package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ChronoGo/pkg/logger"
)

// AsyncWAL 表示异步预写日志
type AsyncWAL struct {
	wal           *WAL               // 底层WAL
	entryChan     chan *WALEntry     // 条目通道
	batchSize     int                // 批处理大小
	flushInterval time.Duration      // 刷新间隔
	wg            sync.WaitGroup     // 等待组
	ctx           context.Context    // 上下文
	cancel        context.CancelFunc // 取消函数
	mu            sync.Mutex         // 互斥锁
	buffer        []*WALEntry        // 条目缓冲区
}

// NewAsyncWAL 创建新的异步WAL
func NewAsyncWAL(wal *WAL, queueSize int, batchSize int, flushInterval time.Duration, workers int) *AsyncWAL {
	ctx, cancel := context.WithCancel(context.Background())

	asyncWAL := &AsyncWAL{
		wal:           wal,
		entryChan:     make(chan *WALEntry, queueSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		ctx:           ctx,
		cancel:        cancel,
		buffer:        make([]*WALEntry, 0, batchSize),
	}

	// 启动工作线程
	for i := 0; i < workers; i++ {
		asyncWAL.wg.Add(1)
		go asyncWAL.worker(i)
	}

	// 启动定时刷新线程
	asyncWAL.wg.Add(1)
	go asyncWAL.periodicFlusher()

	return asyncWAL
}

// Write 异步写入WAL条目
func (aw *AsyncWAL) Write(entry *WALEntry) error {
	select {
	case aw.entryChan <- entry:
		return nil
	default:
		// 通道已满，直接写入WAL
		logger.Printf("AsyncWAL: 通道已满，直接写入WAL")
		return aw.wal.Write(entry)
	}
}

// worker WAL工作线程
func (aw *AsyncWAL) worker(id int) {
	defer aw.wg.Done()

	logger.Printf("AsyncWAL: 工作线程 %d 启动", id)

	for {
		select {
		case <-aw.ctx.Done():
			logger.Printf("AsyncWAL: 工作线程 %d 退出", id)
			return
		case entry := <-aw.entryChan:
			aw.mu.Lock()
			aw.buffer = append(aw.buffer, entry)

			// 如果缓冲区达到批处理大小，刷新到WAL
			if len(aw.buffer) >= aw.batchSize {
				if err := aw.flushBuffer(); err != nil {
					logger.Printf("AsyncWAL: 刷新缓冲区失败: %v", err)
				}
			}
			aw.mu.Unlock()
		}
	}
}

// periodicFlusher 定期刷新线程
func (aw *AsyncWAL) periodicFlusher() {
	defer aw.wg.Done()

	ticker := time.NewTicker(aw.flushInterval)
	defer ticker.Stop()

	logger.Printf("AsyncWAL: 定期刷新线程启动，间隔 %v", aw.flushInterval)

	for {
		select {
		case <-aw.ctx.Done():
			logger.Printf("AsyncWAL: 定期刷新线程退出")
			return
		case <-ticker.C:
			aw.mu.Lock()
			if len(aw.buffer) > 0 {
				if err := aw.flushBuffer(); err != nil {
					logger.Printf("AsyncWAL: 定期刷新失败: %v", err)
				}
			}
			aw.mu.Unlock()
		}
	}
}

// flushBuffer 刷新缓冲区到WAL
// 调用者必须持有锁
func (aw *AsyncWAL) flushBuffer() error {
	if len(aw.buffer) == 0 {
		return nil
	}

	logger.Printf("AsyncWAL: 刷新 %d 个条目到WAL", len(aw.buffer))

	// 逐个写入WAL
	for _, entry := range aw.buffer {
		if err := aw.wal.Write(entry); err != nil {
			return fmt.Errorf("failed to write entry to WAL: %w", err)
		}

		// 将条目放回对象池
		aw.wal.ReleaseEntry(entry)
	}

	// 清空缓冲区
	aw.buffer = aw.buffer[:0]

	return nil
}

// Sync 同步WAL
func (aw *AsyncWAL) Sync() error {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	// 刷新缓冲区
	if err := aw.flushBuffer(); err != nil {
		return err
	}

	// 同步底层WAL
	return aw.wal.Sync()
}

// Close 关闭异步WAL
func (aw *AsyncWAL) Close() error {
	logger.Printf("AsyncWAL: 开始关闭")

	// 取消上下文
	aw.cancel()

	// 等待所有工作线程退出
	done := make(chan struct{})
	go func() {
		aw.wg.Wait()
		close(done)
	}()

	// 等待工作线程退出，最多等待5秒
	select {
	case <-done:
		logger.Printf("AsyncWAL: 所有工作线程已退出")
	case <-time.After(5 * time.Second):
		logger.Printf("AsyncWAL: 等待工作线程退出超时")
	}

	// 刷新剩余条目
	aw.mu.Lock()
	entriesCount := len(aw.buffer)
	if entriesCount > 0 {
		logger.Printf("AsyncWAL: 关闭前刷新剩余 %d 个条目", entriesCount)
		if err := aw.flushBuffer(); err != nil {
			aw.mu.Unlock()
			return fmt.Errorf("failed to flush buffer during close: %w", err)
		}
	}
	aw.mu.Unlock()

	// 确保数据已写入磁盘
	if err := aw.wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL during close: %w", err)
	}

	logger.Printf("AsyncWAL: 关闭完成")
	return nil
}

// Recover 从WAL恢复
func (aw *AsyncWAL) Recover() ([]*WALEntry, error) {
	// 直接使用底层WAL的恢复功能
	return aw.wal.Recover()
}

// WriteBatch 批量异步写入WAL条目
func (aw *AsyncWAL) WriteBatch(entries []*WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// 如果只有一个条目，使用单条写入
	if len(entries) == 1 {
		return aw.Write(entries[0])
	}

	// 批量写入
	aw.mu.Lock()
	defer aw.mu.Unlock()

	// 预分配足够的空间
	if cap(aw.buffer)-len(aw.buffer) < len(entries) {
		// 需要扩容
		newBuffer := make([]*WALEntry, len(aw.buffer), len(aw.buffer)+len(entries))
		copy(newBuffer, aw.buffer)
		aw.buffer = newBuffer
	}

	// 添加所有条目到缓冲区
	aw.buffer = append(aw.buffer, entries...)

	// 如果缓冲区达到批处理大小，刷新到WAL
	if len(aw.buffer) >= aw.batchSize {
		if err := aw.flushBuffer(); err != nil {
			logger.Printf("AsyncWAL: 批量刷新缓冲区失败: %v", err)
			return err
		}
	}

	return nil
}
