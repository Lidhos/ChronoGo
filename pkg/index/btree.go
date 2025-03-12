package index

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// BTreeDegree B+树的度（每个节点的最大子节点数）
const BTreeDegree = 32

// BTreeNode B+树节点
type BTreeNode struct {
	// 是否是叶子节点
	isLeaf bool
	// 键（按顺序排列）
	keys []string
	// 子节点（非叶子节点）
	children []*BTreeNode
	// 值（叶子节点）
	values []SeriesIDSet
	// 下一个叶子节点（用于范围查询）
	next *BTreeNode
}

// newBTreeNode 创建新的B+树节点
func newBTreeNode(isLeaf bool) *BTreeNode {
	return &BTreeNode{
		isLeaf:   isLeaf,
		keys:     make([]string, 0, BTreeDegree-1),
		children: make([]*BTreeNode, 0, BTreeDegree),
		values:   make([]SeriesIDSet, 0, BTreeDegree-1),
	}
}

// BTreeIndex B+树索引实现
type BTreeIndex struct {
	// 索引名称
	name string
	// 索引字段
	fields []string
	// 是否唯一索引
	unique bool
	// 根节点
	root *BTreeNode
	// 统计信息
	stats IndexStats
	// 互斥锁
	mu sync.RWMutex
}

// NewBTreeIndex 创建新的B+树索引
func NewBTreeIndex(name string, fields []string, unique bool) *BTreeIndex {
	return &BTreeIndex{
		name:   name,
		fields: fields,
		unique: unique,
		root:   newBTreeNode(true), // 初始时只有一个叶子节点
		stats: IndexStats{
			Name:           name,
			Type:           IndexTypeBTree,
			LastUpdateTime: time.Now().UnixNano(),
		},
	}
}

// Name 返回索引名称
func (idx *BTreeIndex) Name() string {
	return idx.name
}

// Type 返回索引类型
func (idx *BTreeIndex) Type() IndexType {
	return IndexTypeBTree
}

// Fields 返回索引字段
func (idx *BTreeIndex) Fields() []string {
	return idx.fields
}

// findLeaf 查找包含键的叶子节点
func (idx *BTreeIndex) findLeaf(key string) *BTreeNode {
	node := idx.root
	for !node.isLeaf {
		i := sort.SearchStrings(node.keys, key)
		if i < len(node.keys) && node.keys[i] == key {
			i++
		}
		node = node.children[i]
	}
	return node
}

// findKey 在叶子节点中查找键的位置
func findKey(node *BTreeNode, key string) int {
	return sort.SearchStrings(node.keys, key)
}

// Insert 插入索引项
func (idx *BTreeIndex) Insert(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为字符串
	strKey, ok := key.(string)
	if !ok {
		return fmt.Errorf("key must be string, got %T", key)
	}

	// 查找叶子节点
	leaf := idx.findLeaf(strKey)
	i := findKey(leaf, strKey)

	// 检查键是否已存在
	if i < len(leaf.keys) && leaf.keys[i] == strKey {
		// 键已存在
		if idx.unique && leaf.values[i].Size() > 0 {
			return fmt.Errorf("duplicate key %s for unique index %s", strKey, idx.name)
		}
		// 添加时间序列ID
		leaf.values[i].Add(SeriesID(seriesID))
	} else {
		// 键不存在，插入新键
		leaf.keys = append(leaf.keys, "")
		leaf.values = append(leaf.values, nil)
		// 移动元素
		copy(leaf.keys[i+1:], leaf.keys[i:])
		copy(leaf.values[i+1:], leaf.values[i:])
		// 插入新键和值
		leaf.keys[i] = strKey
		leaf.values[i] = NewSeriesIDSet()
		leaf.values[i].Add(SeriesID(seriesID))

		// 检查是否需要分裂
		if len(leaf.keys) >= BTreeDegree-1 {
			idx.splitLeaf(leaf)
		}
	}

	// 更新统计信息
	idx.stats.ItemCount++
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// splitLeaf 分裂叶子节点
func (idx *BTreeIndex) splitLeaf(leaf *BTreeNode) {
	// 创建新的叶子节点
	newLeaf := newBTreeNode(true)

	// 分裂点
	mid := BTreeDegree / 2

	// 将一半的键和值移动到新节点
	newLeaf.keys = append(newLeaf.keys, leaf.keys[mid:]...)
	newLeaf.values = append(newLeaf.values, leaf.values[mid:]...)

	// 更新原节点
	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]

	// 更新链表
	newLeaf.next = leaf.next
	leaf.next = newLeaf

	// 如果是根节点，创建新的根
	if leaf == idx.root {
		newRoot := newBTreeNode(false)
		newRoot.keys = append(newRoot.keys, newLeaf.keys[0])
		newRoot.children = append(newRoot.children, leaf, newLeaf)
		idx.root = newRoot
	} else {
		// 否则，将新节点插入到父节点
		idx.insertIntoParent(leaf, newLeaf.keys[0], newLeaf)
	}
}

// insertIntoParent 将键和子节点插入到父节点
func (idx *BTreeIndex) insertIntoParent(left *BTreeNode, key string, right *BTreeNode) {
	// 查找父节点
	parent := idx.findParent(idx.root, left)

	// 在父节点中查找插入位置
	i := 0
	for i < len(parent.keys) && parent.keys[i] <= key {
		i++
	}

	// 插入键
	parent.keys = append(parent.keys, "")
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = key

	// 插入子节点
	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = right

	// 检查是否需要分裂
	if len(parent.keys) >= BTreeDegree-1 {
		idx.splitInternal(parent)
	}
}

// splitInternal 分裂内部节点
func (idx *BTreeIndex) splitInternal(node *BTreeNode) {
	// 创建新的内部节点
	newNode := newBTreeNode(false)

	// 分裂点
	mid := BTreeDegree / 2

	// 将一半的键和子节点移动到新节点
	newNode.keys = append(newNode.keys, node.keys[mid+1:]...)
	newNode.children = append(newNode.children, node.children[mid+1:]...)

	// 提升中间键
	midKey := node.keys[mid]

	// 更新原节点
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]

	// 如果是根节点，创建新的根
	if node == idx.root {
		newRoot := newBTreeNode(false)
		newRoot.keys = append(newRoot.keys, midKey)
		newRoot.children = append(newRoot.children, node, newNode)
		idx.root = newRoot
	} else {
		// 否则，将新节点插入到父节点
		idx.insertIntoParent(node, midKey, newNode)
	}
}

// findParent 查找节点的父节点
func (idx *BTreeIndex) findParent(root, child *BTreeNode) *BTreeNode {
	if root.isLeaf || root.children[0].isLeaf {
		return nil
	}

	// 检查直接子节点
	for i := 0; i < len(root.children); i++ {
		if root.children[i] == child {
			return root
		}
	}

	// 递归查找
	for i := 0; i < len(root.children); i++ {
		if parent := idx.findParent(root.children[i], child); parent != nil {
			return parent
		}
	}

	return nil
}

// Remove 删除索引项
func (idx *BTreeIndex) Remove(ctx context.Context, key interface{}, seriesID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 将key转换为字符串
	strKey, ok := key.(string)
	if !ok {
		return fmt.Errorf("key must be string, got %T", key)
	}

	// 查找叶子节点
	leaf := idx.findLeaf(strKey)
	i := findKey(leaf, strKey)

	// 检查键是否存在
	if i < len(leaf.keys) && leaf.keys[i] == strKey {
		// 删除时间序列ID
		leaf.values[i].Remove(SeriesID(seriesID))

		// 如果集合为空，删除键
		if leaf.values[i].Size() == 0 {
			// 删除键和值
			leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
			leaf.values = append(leaf.values[:i], leaf.values[i+1:]...)

			// 更新统计信息
			idx.stats.ItemCount--
			idx.stats.LastUpdateTime = time.Now().UnixNano()

			// 注意：为简化实现，这里不处理节点合并
			// 在实际应用中，应该处理节点合并以保持树的平衡
		}
	}

	return nil
}

// Update 更新索引项
func (idx *BTreeIndex) Update(ctx context.Context, oldKey, newKey interface{}, seriesID string) error {
	// 先删除旧的，再插入新的
	if err := idx.Remove(ctx, oldKey, seriesID); err != nil {
		return err
	}
	return idx.Insert(ctx, newKey, seriesID)
}

// Search 搜索索引
func (idx *BTreeIndex) Search(ctx context.Context, condition IndexCondition) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 记录查询开始时间
	startTime := time.Now().UnixNano()

	var result SeriesIDSet

	switch condition.Operator {
	case "=", "==":
		// 精确匹配
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for = operator, got %T", condition.Value)
		}

		// 查找叶子节点
		leaf := idx.findLeaf(strValue)
		i := findKey(leaf, strValue)

		// 检查键是否存在
		if i < len(leaf.keys) && leaf.keys[i] == strValue {
			result = leaf.values[i]
		} else {
			result = NewSeriesIDSet()
		}
	case "IN":
		// IN操作符
		values, ok := condition.Value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("value must be []interface{} for IN operator, got %T", condition.Value)
		}

		result = NewSeriesIDSet()
		for _, val := range values {
			strVal, ok := val.(string)
			if !ok {
				continue
			}

			// 查找叶子节点
			leaf := idx.findLeaf(strVal)
			i := findKey(leaf, strVal)

			// 检查键是否存在
			if i < len(leaf.keys) && leaf.keys[i] == strVal {
				result = result.Union(leaf.values[i])
			}
		}
	case "LIKE":
		// 前缀匹配
		strValue, ok := condition.Value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be string for LIKE operator, got %T", condition.Value)
		}

		// 移除通配符
		pattern := strings.ReplaceAll(strValue, "%", "")

		// 遍历所有叶子节点
		result = NewSeriesIDSet()
		leaf := idx.findLeaf("")
		for leaf != nil {
			for i, key := range leaf.keys {
				if strings.Contains(key, pattern) {
					result = result.Union(leaf.values[i])
				}
			}
			leaf = leaf.next
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s for btree index", condition.Operator)
	}

	// 更新统计信息
	idx.stats.QueryCount++
	if result.Size() > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)

	return result.ToSlice(), nil
}

// SearchRange 范围搜索
func (idx *BTreeIndex) SearchRange(ctx context.Context, startKey, endKey interface{}, includeStart, includeEnd bool) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 记录查询开始时间
	startTime := time.Now().UnixNano()

	// 将key转换为字符串
	strStartKey, ok := startKey.(string)
	if !ok {
		return nil, fmt.Errorf("startKey must be string, got %T", startKey)
	}
	strEndKey, ok := endKey.(string)
	if !ok {
		return nil, fmt.Errorf("endKey must be string, got %T", endKey)
	}

	// 查找起始叶子节点
	leaf := idx.findLeaf(strStartKey)
	i := findKey(leaf, strStartKey)

	// 如果不包含起始键，移动到下一个键
	if !includeStart && i < len(leaf.keys) && leaf.keys[i] == strStartKey {
		i++
	}

	// 收集范围内的所有时间序列ID
	result := NewSeriesIDSet()
	for leaf != nil {
		for ; i < len(leaf.keys); i++ {
			// 检查是否超过结束键
			if leaf.keys[i] > strEndKey || (leaf.keys[i] == strEndKey && !includeEnd) {
				// 更新统计信息
				idx.stats.QueryCount++
				if result.Size() > 0 {
					idx.stats.HitCount++
				}
				idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)
				return result.ToSlice(), nil
			}
			result = result.Union(leaf.values[i])
		}
		leaf = leaf.next
		i = 0
	}

	// 更新统计信息
	idx.stats.QueryCount++
	if result.Size() > 0 {
		idx.stats.HitCount++
	}
	idx.stats.AvgQueryTimeNs = (idx.stats.AvgQueryTimeNs*idx.stats.QueryCount + time.Now().UnixNano() - startTime) / (idx.stats.QueryCount + 1)

	return result.ToSlice(), nil
}

// Clear 清空索引
func (idx *BTreeIndex) Clear(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 创建新的根节点
	idx.root = newBTreeNode(true)

	// 更新统计信息
	idx.stats.ItemCount = 0
	idx.stats.LastUpdateTime = time.Now().UnixNano()

	return nil
}

// Stats 返回索引统计信息
func (idx *BTreeIndex) Stats(ctx context.Context) (IndexStats, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return IndexStats{}, ctx.Err()
	default:
	}

	// 计算索引大小
	sizeBytes := idx.calculateSize(idx.root)
	idx.stats.SizeBytes = sizeBytes

	return idx.stats, nil
}

// calculateSize 计算节点及其子节点的大小
func (idx *BTreeIndex) calculateSize(node *BTreeNode) int64 {
	if node == nil {
		return 0
	}

	// 节点基本大小
	size := int64(24) // 指针和标志位

	// 键的大小
	for _, key := range node.keys {
		size += int64(len(key))
	}

	// 值的大小（叶子节点）
	if node.isLeaf {
		for _, value := range node.values {
			size += int64(value.Size() * 16) // 每个ID估计16字节
		}
	} else {
		// 递归计算子节点大小
		for _, child := range node.children {
			size += idx.calculateSize(child)
		}
	}

	return size
}

// Close 关闭索引
func (idx *BTreeIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空数据
	idx.root = nil

	return nil
}

// Compact 手动触发压缩（仅LSM树索引支持）
func (idx *BTreeIndex) Compact(ctx context.Context) error {
	// BTree索引不支持压缩操作
	return nil
}

// Snapshot 创建索引快照（仅LSM树索引支持）
func (idx *BTreeIndex) Snapshot() (IndexSnapshot, error) {
	// BTree索引不支持快照
	return nil, fmt.Errorf("snapshot not supported for btree index")
}

// Flush 将内存中的数据刷新到磁盘（仅LSM树索引支持）
func (idx *BTreeIndex) Flush(ctx context.Context) error {
	// BTree索引不支持刷盘操作
	return nil
}
