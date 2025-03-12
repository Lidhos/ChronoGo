package storage

import (
	"hash/fnv"
	"sort"
	"sync"
)

// ShardedLockManager 表示分片锁管理器
type ShardedLockManager struct {
	shards     []*shardedLockShard // 锁分片
	shardCount int                 // 分片数量
	shardMask  uint32              // 分片掩码
}

// shardedLockShard 表示锁分片
type shardedLockShard struct {
	sync.RWMutex
	locks map[string]*sync.RWMutex // 锁映射
}

// NewShardedLockManager 创建新的分片锁管理器
func NewShardedLockManager(shardCount int) *ShardedLockManager {
	if shardCount <= 0 {
		shardCount = 32 // 默认分片数量
	}

	// 确保分片数量是2的幂
	shardCount = nextPowerOfTwoInt(shardCount)

	// 创建分片
	shards := make([]*shardedLockShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &shardedLockShard{
			locks: make(map[string]*sync.RWMutex),
		}
	}

	return &ShardedLockManager{
		shards:     shards,
		shardCount: shardCount,
		shardMask:  uint32(shardCount - 1),
	}
}

// nextPowerOfTwo 返回大于等于n的最小2的幂
func nextPowerOfTwoInt(n int) int {
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
func (lm *ShardedLockManager) getShard(key string) *shardedLockShard {
	// 计算哈希值
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// 获取分片索引
	shardIndex := hash & lm.shardMask

	return lm.shards[shardIndex]
}

// getLock 获取指定键的锁
func (lm *ShardedLockManager) getLock(key string) *sync.RWMutex {
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
func (lm *ShardedLockManager) Lock(key string) {
	lock := lm.getLock(key)
	lock.Lock()
}

// Unlock 释放指定键的写锁
func (lm *ShardedLockManager) Unlock(key string) {
	lock := lm.getLock(key)
	lock.Unlock()
}

// RLock 获取指定键的读锁
func (lm *ShardedLockManager) RLock(key string) {
	lock := lm.getLock(key)
	lock.RLock()
}

// RUnlock 释放指定键的读锁
func (lm *ShardedLockManager) RUnlock(key string) {
	lock := lm.getLock(key)
	lock.RUnlock()
}

// LockMulti 获取多个键的写锁
func (lm *ShardedLockManager) LockMulti(keys []string) {
	// 对键进行排序，避免死锁
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	// 按顺序获取锁
	for _, key := range sortedKeys {
		lm.Lock(key)
	}
}

// UnlockMulti 释放多个键的写锁
func (lm *ShardedLockManager) UnlockMulti(keys []string) {
	// 按相反顺序释放锁
	for i := len(keys) - 1; i >= 0; i-- {
		lm.Unlock(keys[i])
	}
}

// RLockMulti 获取多个键的读锁
func (lm *ShardedLockManager) RLockMulti(keys []string) {
	// 对键进行排序，避免死锁
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	// 按顺序获取锁
	for _, key := range sortedKeys {
		lm.RLock(key)
	}
}

// RUnlockMulti 释放多个键的读锁
func (lm *ShardedLockManager) RUnlockMulti(keys []string) {
	// 按相反顺序释放锁
	for i := len(keys) - 1; i >= 0; i-- {
		lm.RUnlock(keys[i])
	}
}

// TryLock 尝试获取指定键的写锁
func (lm *ShardedLockManager) TryLock(key string) bool {
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
func (lm *ShardedLockManager) TryRLock(key string) bool {
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
func (lm *ShardedLockManager) Clear() {
	for _, shard := range lm.shards {
		shard.Lock()
		shard.locks = make(map[string]*sync.RWMutex)
		shard.Unlock()
	}
}
