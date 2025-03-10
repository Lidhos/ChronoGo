package storage

import (
	"strings"
	"sync"

	"ChronoGo/pkg/logger"
)

// LockManager 管理存储引擎的锁
type LockManager struct {
	globalLock sync.RWMutex             // 全局锁，用于保护整个存储引擎
	dbLocks    map[string]*sync.RWMutex // 数据库级锁
	tableLocks map[string]*sync.RWMutex // 表级锁 (格式: "db:table")
	mu         sync.Mutex               // 保护 dbLocks 和 tableLocks 映射
	debug      bool                     // 是否启用调试日志
}

// NewLockManager 创建一个新的锁管理器
func NewLockManager(debug bool) *LockManager {
	return &LockManager{
		dbLocks:    make(map[string]*sync.RWMutex),
		tableLocks: make(map[string]*sync.RWMutex),
		debug:      debug,
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

// LockMulti 锁定多个资源
// 按照全局->数据库->表的顺序获取锁，避免死锁
// 返回解锁函数
func (lm *LockManager) LockMulti(globalWrite bool, dbLocks map[string]bool, tableLocks map[string]bool) func() {
	// 保存所有获取的锁和解锁函数
	var unlockFuncs []func()

	// 获取全局锁
	if globalWrite {
		unlockGlobal := lm.LockGlobal(true)
		unlockFuncs = append(unlockFuncs, unlockGlobal)
	} else if len(dbLocks) > 0 || len(tableLocks) > 0 {
		unlockGlobal := lm.LockGlobal(false)
		unlockFuncs = append(unlockFuncs, unlockGlobal)
	}

	// 获取数据库锁
	for dbName, write := range dbLocks {
		unlockDB := lm.LockDatabase(dbName, write)
		unlockFuncs = append(unlockFuncs, unlockDB)
	}

	// 获取表锁
	for tableKey, write := range tableLocks {
		parts := strings.Split(tableKey, ":")
		if len(parts) == 2 {
			unlockTable := lm.LockTable(parts[0], parts[1], write)
			unlockFuncs = append(unlockFuncs, unlockTable)
		}
	}

	// 返回解锁函数，按照获取锁的相反顺序释放锁
	return func() {
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
