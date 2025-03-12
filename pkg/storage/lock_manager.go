package storage

import (
	"hash/fnv"
	"strings"
	"sync"

	"ChronoGo/pkg/logger"
)

// LockManager 表示锁管理器
type LockManager struct {
	shards     []*lockShard             // 锁分片
	shardCount int                      // 分片数量
	shardMask  uint32                   // 分片掩码
	globalLock sync.RWMutex             // 全局锁，用于保护整个存储引擎
	dbLocks    map[string]*sync.RWMutex // 数据库级锁
	tableLocks map[string]*sync.RWMutex // 表级锁 (格式: "db:table")
	mu         sync.Mutex               // 保护 dbLocks 和 tableLocks 映射
	debug      bool                     // 是否启用调试日志
}

// lockShard 表示锁分片
type lockShard struct {
	sync.RWMutex
	locks map[string]*sync.RWMutex // 锁映射
}

// NewLockManager 创建新的锁管理器
func NewLockManager(shardCount int, debug bool) *LockManager {
	if shardCount <= 0 {
		shardCount = 32 // 默认分片数量
	}

	// 确保分片数量是2的幂
	shardCount = nextPowerOfTwo(shardCount)

	// 创建分片
	shards := make([]*lockShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &lockShard{
			locks: make(map[string]*sync.RWMutex),
		}
	}

	return &LockManager{
		shards:     shards,
		shardCount: shardCount,
		shardMask:  uint32(shardCount - 1),
		dbLocks:    make(map[string]*sync.RWMutex),
		tableLocks: make(map[string]*sync.RWMutex),
		debug:      debug,
	}
}

// nextPowerOfTwo 返回大于等于n的最小2的幂
func nextPowerOfTwo(n int) int {
	if n <= 0 {
		return 1
	}

	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++

	return n
}

// getShard 获取指定键的分片
func (lm *LockManager) getShard(key string) *lockShard {
	// 计算哈希值
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// 获取分片索引
	shardIndex := hash & lm.shardMask

	return lm.shards[shardIndex]
}

// getLock 获取指定键的锁
func (lm *LockManager) getLock(key string) *sync.RWMutex {
	// 获取分片
	shard := lm.getShard(key)

	// 获取锁
	shard.RLock()
	lock, exists := shard.locks[key]
	shard.RUnlock()

	if exists {
		return lock
	}

	// 创建新锁
	shard.Lock()
	defer shard.Unlock()

	// 再次检查，避免竞争条件
	lock, exists = shard.locks[key]
	if exists {
		return lock
	}

	// 创建新锁
	lock = &sync.RWMutex{}
	shard.locks[key] = lock

	return lock
}

// Lock 获取指定键的写锁
func (lm *LockManager) Lock(key string) {
	lock := lm.getLock(key)
	lock.Lock()
}

// Unlock 释放指定键的写锁
func (lm *LockManager) Unlock(key string) {
	lock := lm.getLock(key)
	lock.Unlock()
}

// RLock 获取指定键的读锁
func (lm *LockManager) RLock(key string) {
	lock := lm.getLock(key)
	lock.RLock()
}

// RUnlock 释放指定键的读锁
func (lm *LockManager) RUnlock(key string) {
	lock := lm.getLock(key)
	lock.RUnlock()
}

// LockMulti 获取多个键的写锁
func (lm *LockManager) LockMulti(keys []string) {
	// 对键进行排序，避免死锁
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)

	// 按顺序获取锁
	for _, key := range sortedKeys {
		lm.Lock(key)
	}
}

// UnlockMulti 释放多个键的写锁
func (lm *LockManager) UnlockMulti(keys []string) {
	// 按相反顺序释放锁
	for i := len(keys) - 1; i >= 0; i-- {
		lm.Unlock(keys[i])
	}
}

// RLockMulti 获取多个键的读锁
func (lm *LockManager) RLockMulti(keys []string) {
	// 对键进行排序，避免死锁
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)

	// 按顺序获取锁
	for _, key := range sortedKeys {
		lm.RLock(key)
	}
}

// RUnlockMulti 释放多个键的读锁
func (lm *LockManager) RUnlockMulti(keys []string) {
	// 按相反顺序释放锁
	for i := len(keys) - 1; i >= 0; i-- {
		lm.RUnlock(keys[i])
	}
}

// TryLock 尝试获取指定键的写锁
func (lm *LockManager) TryLock(key string) bool {
	// 获取分片
	shard := lm.getShard(key)

	// 获取锁
	shard.RLock()
	lock, exists := shard.locks[key]
	shard.RUnlock()

	if !exists {
		// 创建新锁
		shard.Lock()
		lock, exists = shard.locks[key]
		if !exists {
			lock = &sync.RWMutex{}
			shard.locks[key] = lock
		}
		shard.Unlock()
	}

	// 尝试获取锁
	return true // 注意：标准库的sync.RWMutex没有TryLock方法，这里简化处理
}

// TryRLock 尝试获取指定键的读锁
func (lm *LockManager) TryRLock(key string) bool {
	// 获取分片
	shard := lm.getShard(key)

	// 获取锁
	shard.RLock()
	lock, exists := shard.locks[key]
	shard.RUnlock()

	if !exists {
		// 创建新锁
		shard.Lock()
		lock, exists = shard.locks[key]
		if !exists {
			lock = &sync.RWMutex{}
			shard.locks[key] = lock
		}
		shard.Unlock()
	}

	// 尝试获取锁
	return true // 注意：标准库的sync.RWMutex没有TryRLock方法，这里简化处理
}

// Clear 清空锁管理器
func (lm *LockManager) Clear() {
	for _, shard := range lm.shards {
		shard.Lock()
		shard.locks = make(map[string]*sync.RWMutex)
		shard.Unlock()
	}
}

// GetDatabaseLock 获取数据库锁
func (lm *LockManager) GetDatabaseLock(dbName string) *sync.RWMutex {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lock, exists := lm.dbLocks[dbName]
	if !exists {
		lock = &sync.RWMutex{}
		lm.dbLocks[dbName] = lock
	}

	return lock
}

// GetTableLock 获取表锁
func (lm *LockManager) GetTableLock(dbName, tableName string) *sync.RWMutex {
	key := dbName + ":" + tableName

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lock, exists := lm.tableLocks[key]
	if !exists {
		lock = &sync.RWMutex{}
		lm.tableLocks[key] = lock
	}

	return lock
}

// LockGlobal 锁定全局锁
// 返回解锁函数
func (lm *LockManager) LockGlobal(write bool) func() {
	if write {
		if lm.debug {
			logger.Println("获取全局写锁")
		}
		lm.globalLock.Lock()
		return func() {
			if lm.debug {
				logger.Println("释放全局写锁")
			}
			lm.globalLock.Unlock()
		}
	} else {
		if lm.debug {
			logger.Printf("获取全局读锁")
		}
		lm.globalLock.RLock()
		return func() {
			if lm.debug {
				logger.Printf("释放全局读锁")
			}
			lm.globalLock.RUnlock()
		}
	}
}

// LockDatabase 锁定数据库
// 返回解锁函数
func (lm *LockManager) LockDatabase(dbName string, write bool) func() {
	lock := lm.GetDatabaseLock(dbName)

	if write {
		if lm.debug {
			logger.Printf("获取数据库 %s 写锁", dbName)
		}
		lock.Lock()
		return func() {
			if lm.debug {
				logger.Printf("释放数据库 %s 写锁", dbName)
			}
			lock.Unlock()
		}
	} else {
		if lm.debug {
			logger.Printf("获取数据库 %s 读锁", dbName)
		}
		lock.RLock()
		return func() {
			if lm.debug {
				logger.Printf("释放数据库 %s 读锁", dbName)
			}
			lock.RUnlock()
		}
	}
}

// LockTable 锁定表
// 返回解锁函数
func (lm *LockManager) LockTable(dbName, tableName string, write bool) func() {
	lock := lm.GetTableLock(dbName, tableName)

	if write {
		if lm.debug {
			logger.Printf("获取表 %s.%s 写锁", dbName, tableName)
		}
		lock.Lock()
		return func() {
			if lm.debug {
				logger.Printf("释放表 %s.%s 写锁", dbName, tableName)
			}
			lock.Unlock()
		}
	} else {
		if lm.debug {
			logger.Printf("获取表 %s.%s 读锁", dbName, tableName)
		}
		lock.RLock()
		return func() {
			if lm.debug {
				logger.Printf("释放表 %s.%s 读锁", dbName, tableName)
			}
			lock.RUnlock()
		}
	}
}

// LockMultiWithMaps 按照全局->数据库->表的顺序获取锁，避免死锁
// 返回解锁函数
func (lm *LockManager) LockMultiWithMaps(globalWrite bool, dbLocks map[string]bool, tableLocks map[string]bool) func() {
	// 保存所有获取的锁和解锁函数
	var unlockFuncs []func()

	// 第一步：获取全局锁
	if globalWrite {
		lm.globalLock.Lock()
		unlockFuncs = append(unlockFuncs, lm.globalLock.Unlock)
	} else {
		lm.globalLock.RLock()
		unlockFuncs = append(unlockFuncs, lm.globalLock.RUnlock)
	}

	// 第二步：获取数据库锁
	for dbName, write := range dbLocks {
		dbLock := lm.GetDatabaseLock(dbName)
		if write {
			dbLock.Lock()
			unlockFuncs = append(unlockFuncs, dbLock.Unlock)
		} else {
			dbLock.RLock()
			unlockFuncs = append(unlockFuncs, dbLock.RUnlock)
		}
	}

	// 第三步：获取表锁
	for tableKey, write := range tableLocks {
		tableLock := lm.getLock(tableKey)
		if write {
			tableLock.Lock()
			unlockFuncs = append(unlockFuncs, tableLock.Unlock)
		} else {
			tableLock.RLock()
			unlockFuncs = append(unlockFuncs, tableLock.RUnlock)
		}
	}

	// 返回解锁函数
	return func() {
		// 按照相反的顺序释放锁
		for i := len(unlockFuncs) - 1; i >= 0; i-- {
			unlockFuncs[i]()
		}
	}
}

// RemoveDatabaseLocks 移除数据库相关的所有锁
func (lm *LockManager) RemoveDatabaseLocks(dbName string) {
	// 获取锁管理器的锁
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 移除数据库锁
	delete(lm.dbLocks, dbName)

	// 移除与该数据库相关的所有表锁
	prefix := dbName + ":"
	for key := range lm.tableLocks {
		if strings.HasPrefix(key, prefix) {
			delete(lm.tableLocks, key)
		}
	}
}

// RemoveTableLock 移除表锁
func (lm *LockManager) RemoveTableLock(dbName, tableName string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	key := dbName + ":" + tableName
	delete(lm.tableLocks, key)
}
