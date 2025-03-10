package index

import (
	"context"
	"fmt"
	"sync"
)

// IndexManagerImpl 索引管理器实现
type IndexManagerImpl struct {
	// 索引映射 database -> collection -> indexName -> index
	indexes map[string]map[string]map[string]Index
	// 互斥锁
	mu sync.RWMutex
}

// NewIndexManager 创建新的索引管理器
func NewIndexManager() *IndexManagerImpl {
	return &IndexManagerImpl{
		indexes: make(map[string]map[string]map[string]Index),
	}
}

// CreateIndex 创建索引
func (m *IndexManagerImpl) CreateIndex(ctx context.Context, database, collection string, options IndexOptions) (Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 确保数据库存在
	if _, ok := m.indexes[database]; !ok {
		m.indexes[database] = make(map[string]map[string]Index)
	}

	// 确保集合存在
	if _, ok := m.indexes[database][collection]; !ok {
		m.indexes[database][collection] = make(map[string]Index)
	}

	// 检查索引是否已存在
	if _, ok := m.indexes[database][collection][options.Name]; ok {
		return nil, fmt.Errorf("index %s already exists", options.Name)
	}

	// 创建索引
	var index Index
	switch options.Type {
	case IndexTypeInverted:
		index = NewInvertedIndex(options.Name, options.Fields, options.Unique)
	case IndexTypeBTree:
		index = NewBTreeIndex(options.Name, options.Fields, options.Unique)
	case IndexTypeBitmap:
		index = NewBitmapIndex(options.Name, options.Fields, options.Unique)
	case IndexTypeHash:
		index = NewHashIndex(options.Name, options.Fields, options.Unique)
	case IndexTypeComposite:
		index = NewCompositeIndex(options.Name, options.Fields, options.Unique)
	default:
		// 自动选择索引类型
		index = m.selectIndexType(options)
	}

	// 保存索引
	m.indexes[database][collection][options.Name] = index

	return index, nil
}

// selectIndexType 根据索引选项自动选择索引类型
func (m *IndexManagerImpl) selectIndexType(options IndexOptions) Index {
	// 如果是多字段索引，使用复合索引
	if len(options.Fields) > 1 {
		return NewCompositeIndex(options.Name, options.Fields, options.Unique)
	}

	// 根据基数估计选择索引类型
	if options.CardinalityEstimate > 0 {
		if options.CardinalityEstimate < 100 {
			// 低基数，使用位图索引
			return NewBitmapIndex(options.Name, options.Fields, options.Unique)
		} else if options.CardinalityEstimate > 10000 {
			// 高基数，使用B+树索引（如果需要范围查询）或哈希索引
			if options.SupportRange {
				return NewBTreeIndex(options.Name, options.Fields, options.Unique)
			} else {
				return NewHashIndex(options.Name, options.Fields, options.Unique)
			}
		}
	}

	// 根据查询特性选择索引类型
	if options.SupportRange {
		// 支持范围查询，使用B+树索引
		return NewBTreeIndex(options.Name, options.Fields, options.Unique)
	} else if options.SupportPrefix || options.SupportRegex {
		// 支持前缀匹配或正则表达式，使用倒排索引
		return NewInvertedIndex(options.Name, options.Fields, options.Unique)
	} else {
		// 默认使用哈希索引
		return NewHashIndex(options.Name, options.Fields, options.Unique)
	}
}

// DropIndex 删除索引
func (m *IndexManagerImpl) DropIndex(ctx context.Context, database, collection, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 检查索引是否存在
	if !m.indexExists(database, collection, indexName) {
		return fmt.Errorf("index %s does not exist", indexName)
	}

	// 关闭索引
	if err := m.indexes[database][collection][indexName].Close(); err != nil {
		return err
	}

	// 删除索引
	delete(m.indexes[database][collection], indexName)

	// 如果集合为空，删除集合
	if len(m.indexes[database][collection]) == 0 {
		delete(m.indexes[database], collection)
		// 如果数据库为空，删除数据库
		if len(m.indexes[database]) == 0 {
			delete(m.indexes, database)
		}
	}

	return nil
}

// GetIndex 获取索引
func (m *IndexManagerImpl) GetIndex(ctx context.Context, database, collection, indexName string) (Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 检查索引是否存在
	if !m.indexExists(database, collection, indexName) {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	return m.indexes[database][collection][indexName], nil
}

// ListIndexes 列出集合的所有索引
func (m *IndexManagerImpl) ListIndexes(ctx context.Context, database, collection string) ([]Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 检查数据库和集合是否存在
	if _, ok := m.indexes[database]; !ok {
		return []Index{}, nil
	}
	if _, ok := m.indexes[database][collection]; !ok {
		return []Index{}, nil
	}

	// 收集索引
	result := make([]Index, 0, len(m.indexes[database][collection]))
	for _, index := range m.indexes[database][collection] {
		result = append(result, index)
	}

	return result, nil
}

// RebuildIndex 重建索引
func (m *IndexManagerImpl) RebuildIndex(ctx context.Context, database, collection, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 检查索引是否存在
	if !m.indexExists(database, collection, indexName) {
		return fmt.Errorf("index %s does not exist", indexName)
	}

	// 获取索引
	index := m.indexes[database][collection][indexName]

	// 清空索引
	if err := index.Clear(ctx); err != nil {
		return err
	}

	// 注意：这里只是清空了索引，实际重建需要遍历数据并重新插入
	// 这部分逻辑应该由调用者实现

	return nil
}

// GetStats 获取索引统计信息
func (m *IndexManagerImpl) GetStats(ctx context.Context, database, collection, indexName string) (IndexStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return IndexStats{}, ctx.Err()
	default:
	}

	// 检查索引是否存在
	if !m.indexExists(database, collection, indexName) {
		return IndexStats{}, fmt.Errorf("index %s does not exist", indexName)
	}

	// 获取索引
	index := m.indexes[database][collection][indexName]

	// 获取统计信息
	return index.Stats(ctx)
}

// SelectBestIndex 根据查询条件选择最佳索引
func (m *IndexManagerImpl) SelectBestIndex(ctx context.Context, database, collection string, conditions []IndexCondition) (Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 检查数据库和集合是否存在
	if _, ok := m.indexes[database]; !ok {
		return nil, fmt.Errorf("database %s does not exist", database)
	}
	if _, ok := m.indexes[database][collection]; !ok {
		return nil, fmt.Errorf("collection %s does not exist", collection)
	}

	// 如果没有条件，返回错误
	if len(conditions) == 0 {
		return nil, fmt.Errorf("no conditions provided")
	}

	// 收集所有可能的索引
	candidates := make([]Index, 0)
	for _, index := range m.indexes[database][collection] {
		// 检查索引是否支持所有条件
		if m.indexSupportsConditions(index, conditions) {
			candidates = append(candidates, index)
		}
	}

	// 如果没有候选索引，返回错误
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no suitable index found")
	}

	// 选择最佳索引
	// 这里使用一个简单的启发式算法：
	// 1. 优先选择复合索引
	// 2. 优先选择覆盖字段最多的索引
	// 3. 优先选择查询命中率高的索引
	var bestIndex Index
	var bestScore float64
	for _, index := range candidates {
		score := m.calculateIndexScore(index, conditions)
		if bestIndex == nil || score > bestScore {
			bestIndex = index
			bestScore = score
		}
	}

	return bestIndex, nil
}

// indexExists 检查索引是否存在
func (m *IndexManagerImpl) indexExists(database, collection, indexName string) bool {
	if _, ok := m.indexes[database]; !ok {
		return false
	}
	if _, ok := m.indexes[database][collection]; !ok {
		return false
	}
	_, ok := m.indexes[database][collection][indexName]
	return ok
}

// indexSupportsConditions 检查索引是否支持所有条件
func (m *IndexManagerImpl) indexSupportsConditions(index Index, conditions []IndexCondition) bool {
	// 获取索引字段
	indexFields := index.Fields()

	// 检查索引类型
	switch index.Type() {
	case IndexTypeComposite:
		// 复合索引需要特殊处理
		// 检查条件中的字段是否是索引字段的前缀
		conditionFields := make(map[string]bool)
		for _, condition := range conditions {
			conditionFields[condition.Field] = true
		}
		// 检查索引字段是否是条件字段的前缀
		for i, field := range indexFields {
			if i >= len(conditions) || !conditionFields[field] {
				return false
			}
		}
		return true
	default:
		// 其他索引类型
		// 检查条件中是否有索引字段
		for _, condition := range conditions {
			for _, field := range indexFields {
				if condition.Field == field {
					return true
				}
			}
		}
		return false
	}
}

// calculateIndexScore 计算索引得分
func (m *IndexManagerImpl) calculateIndexScore(index Index, conditions []IndexCondition) float64 {
	// 获取索引统计信息
	stats, err := index.Stats(context.Background())
	if err != nil {
		return 0
	}

	// 基础分数
	score := 1.0

	// 根据索引类型调整分数
	switch index.Type() {
	case IndexTypeComposite:
		// 复合索引得分最高
		score *= 2.0
	case IndexTypeBTree:
		// B+树索引适合范围查询
		for _, condition := range conditions {
			if condition.Operator == ">" || condition.Operator == ">=" || condition.Operator == "<" || condition.Operator == "<=" {
				score *= 1.5
				break
			}
		}
	case IndexTypeHash:
		// 哈希索引适合精确匹配
		for _, condition := range conditions {
			if condition.Operator == "=" || condition.Operator == "==" {
				score *= 1.5
				break
			}
		}
	case IndexTypeInverted:
		// 倒排索引适合文本搜索
		for _, condition := range conditions {
			if condition.Operator == "LIKE" || condition.Operator == "REGEX" {
				score *= 1.5
				break
			}
		}
	case IndexTypeBitmap:
		// 位图索引适合低基数字段
		if stats.ItemCount < 100 {
			score *= 1.5
		}
	}

	// 根据索引字段数量调整分数
	score *= float64(len(index.Fields()))

	// 根据命中率调整分数
	if stats.QueryCount > 0 {
		hitRate := float64(stats.HitCount) / float64(stats.QueryCount)
		score *= (0.5 + hitRate)
	}

	return score
}

// Close 关闭索引管理器
func (m *IndexManagerImpl) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 关闭所有索引
	for _, db := range m.indexes {
		for _, coll := range db {
			for _, index := range coll {
				if err := index.Close(); err != nil {
					return err
				}
			}
		}
	}

	// 清空索引映射
	m.indexes = nil

	return nil
}
