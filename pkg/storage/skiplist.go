package storage

import (
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    = 32   // 跳表最大层数
	probability = 0.25 // 层级提升概率
)

// SkipNode 表示跳表节点
type SkipNode struct {
	key      int64       // 时间戳作为键
	value    []byte      // BSON编码的数据点
	forward  []*SkipNode // 前向指针数组
	backward *SkipNode   // 后向指针
}

// SkipList 表示针对时序数据优化的跳表
type SkipList struct {
	head     *SkipNode    // 头节点
	tail     *SkipNode    // 尾节点
	maxLevel int          // 当前最大层级
	length   int64        // 节点数量
	mu       sync.RWMutex // 读写锁
	rand     *rand.Rand   // 随机数生成器
}

// NewSkipList 创建新的跳表
func NewSkipList() *SkipList {
	head := &SkipNode{
		forward: make([]*SkipNode, maxLevel),
	}

	return &SkipList{
		head:     head,
		maxLevel: 1,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel 随机生成层级
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rand.Float64() < probability {
		level++
	}
	return level
}

// Insert 插入新节点
func (sl *SkipList) Insert(key int64, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 查找插入位置
	update := make([]*SkipNode, maxLevel)
	current := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	// 获取随机层级
	level := sl.randomLevel()
	if level > sl.maxLevel {
		for i := sl.maxLevel; i < level; i++ {
			update[i] = sl.head
		}
		sl.maxLevel = level
	}

	// 创建新节点
	newNode := &SkipNode{
		key:     key,
		value:   value,
		forward: make([]*SkipNode, level),
	}

	// 更新前向指针
	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	// 更新后向指针
	if update[0] != sl.head {
		newNode.backward = update[0]
	}
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	} else {
		sl.tail = newNode
	}

	sl.length++
}

// Get 获取指定键的值
func (sl *SkipList) Get(key int64) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	node := sl.findNode(key)
	if node != nil && node.key == key {
		return node.value, true
	}
	return nil, false
}

// findNode 查找节点
func (sl *SkipList) findNode(key int64) *SkipNode {
	current := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil && current.key == key {
		return current
	}

	return nil
}

// Delete 删除指定键的节点
func (sl *SkipList) Delete(key int64) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*SkipNode, maxLevel)
	current := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && current.key == key {
		// 更新前向指针
		for i := 0; i < sl.maxLevel; i++ {
			if update[i].forward[i] != current {
				break
			}
			update[i].forward[i] = current.forward[i]
		}

		// 更新后向指针
		if current.forward[0] != nil {
			current.forward[0].backward = current.backward
		} else {
			sl.tail = current.backward
		}

		// 更新最大层级
		for sl.maxLevel > 1 && sl.head.forward[sl.maxLevel-1] == nil {
			sl.maxLevel--
		}

		sl.length--
		return true
	}
	return false
}

// Range 范围查询
func (sl *SkipList) Range(start, end int64) [][]byte {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var result [][]byte

	// 找到起始节点
	current := sl.head
	for i := sl.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < start {
			current = current.forward[i]
		}
	}

	// 移动到第一个大于等于start的节点
	current = current.forward[0]

	// 收集范围内的所有节点
	for current != nil && current.key <= end {
		result = append(result, current.value)
		current = current.forward[0]
	}

	return result
}

// Len 返回跳表长度
func (sl *SkipList) Len() int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}

// Clear 清空跳表
func (sl *SkipList) Clear() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	sl.head = &SkipNode{
		forward: make([]*SkipNode, maxLevel),
	}
	sl.tail = nil
	sl.maxLevel = 1
	sl.length = 0
}
