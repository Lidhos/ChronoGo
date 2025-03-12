package util

import (
	"sync"
	"sync/atomic"
)

// WorkerPool 工作线程池
type WorkerPool struct {
	workers    int            // 工作线程数
	tasks      chan func()    // 任务队列
	wg         sync.WaitGroup // 等待组
	running    int32          // 运行状态
	queueDepth int            // 队列深度
}

// NewWorkerPool 创建新的工作线程池
func NewWorkerPool(workers int) *WorkerPool {
	if workers <= 0 {
		workers = 4 // 默认工作线程数
	}

	queueDepth := workers * 100 // 队列深度为工作线程数的100倍

	pool := &WorkerPool{
		workers:    workers,
		tasks:      make(chan func(), queueDepth),
		queueDepth: queueDepth,
	}

	// 启动工作线程
	pool.start()

	return pool
}

// start 启动工作线程池
func (p *WorkerPool) start() {
	atomic.StoreInt32(&p.running, 1)

	// 启动工作线程
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker 工作线程
func (p *WorkerPool) worker() {
	defer p.wg.Done()

	for task := range p.tasks {
		if task == nil {
			return
		}

		// 执行任务
		task()
	}
}

// Submit 提交任务
func (p *WorkerPool) Submit(task func()) bool {
	if atomic.LoadInt32(&p.running) == 0 || task == nil {
		return false
	}

	// 提交任务到队列
	p.tasks <- task
	return true
}

// Stop 停止工作线程池
func (p *WorkerPool) Stop() {
	if !atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		return
	}

	// 关闭任务队列
	close(p.tasks)

	// 等待所有工作线程退出
	p.wg.Wait()
}

// Resize 调整工作线程池大小
func (p *WorkerPool) Resize(workers int) {
	if workers <= 0 || workers == p.workers {
		return
	}

	// 停止当前工作线程池
	p.Stop()

	// 更新工作线程数
	p.workers = workers
	p.queueDepth = workers * 100
	p.tasks = make(chan func(), p.queueDepth)

	// 重新启动工作线程池
	p.start()
}
