package storage

// LockTable 获取表锁
func (e *StorageEngine) LockTable(dbName, tableName string) {
	key := dbName + ":" + tableName
	e.shardedLockMgr.Lock(key)
}

// UnlockTable 释放表锁
func (e *StorageEngine) UnlockTable(dbName, tableName string) {
	key := dbName + ":" + tableName
	e.shardedLockMgr.Unlock(key)
}

// RLockTable 获取表读锁
func (e *StorageEngine) RLockTable(dbName, tableName string) {
	key := dbName + ":" + tableName
	e.shardedLockMgr.RLock(key)
}

// RUnlockTable 释放表读锁
func (e *StorageEngine) RUnlockTable(dbName, tableName string) {
	key := dbName + ":" + tableName
	e.shardedLockMgr.RUnlock(key)
}

// LockDatabase 获取数据库锁
func (e *StorageEngine) LockDatabase(dbName string) {
	e.shardedLockMgr.Lock(dbName)
}

// UnlockDatabase 释放数据库锁
func (e *StorageEngine) UnlockDatabase(dbName string) {
	e.shardedLockMgr.Unlock(dbName)
}

// RLockDatabase 获取数据库读锁
func (e *StorageEngine) RLockDatabase(dbName string) {
	e.shardedLockMgr.RLock(dbName)
}

// RUnlockDatabase 释放数据库读锁
func (e *StorageEngine) RUnlockDatabase(dbName string) {
	e.shardedLockMgr.RUnlock(dbName)
}

// LockMultiTables 获取多个表的锁
func (e *StorageEngine) LockMultiTables(tables []string) {
	e.shardedLockMgr.LockMulti(tables)
}

// UnlockMultiTables 释放多个表的锁
func (e *StorageEngine) UnlockMultiTables(tables []string) {
	e.shardedLockMgr.UnlockMulti(tables)
}

// RLockMultiTables 获取多个表的读锁
func (e *StorageEngine) RLockMultiTables(tables []string) {
	e.shardedLockMgr.RLockMulti(tables)
}

// RUnlockMultiTables 释放多个表的读锁
func (e *StorageEngine) RUnlockMultiTables(tables []string) {
	e.shardedLockMgr.RUnlockMulti(tables)
}

// LockMultiDatabases 获取多个数据库的锁
func (e *StorageEngine) LockMultiDatabases(databases []string) {
	e.shardedLockMgr.LockMulti(databases)
}

// UnlockMultiDatabases 释放多个数据库的锁
func (e *StorageEngine) UnlockMultiDatabases(databases []string) {
	e.shardedLockMgr.UnlockMulti(databases)
}

// RLockMultiDatabases 获取多个数据库的读锁
func (e *StorageEngine) RLockMultiDatabases(databases []string) {
	e.shardedLockMgr.RLockMulti(databases)
}

// RUnlockMultiDatabases 释放多个数据库的读锁
func (e *StorageEngine) RUnlockMultiDatabases(databases []string) {
	e.shardedLockMgr.RUnlockMulti(databases)
}
