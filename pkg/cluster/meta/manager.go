package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ChronoGo/pkg/cluster/common"
)

// Manager 实现了元数据管理器接口
type Manager struct {
	// 元数据目录
	metaDir string
	// 元数据缓存
	cache map[string][]byte
	// 监视回调函数映射
	watchers map[string][]watchCallback
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 持久化间隔
	persistInterval time.Duration
	// 持久化定时器
	persistTimer *time.Timer
	// 停止信号通道
	stopCh chan struct{}
}

// watchCallback 表示监视回调函数
type watchCallback struct {
	prefix   string
	callback func(key string, value []byte, deleted bool)
	id       int64 // 用于标识回调函数
}

// 用于生成唯一的回调函数ID
var nextCallbackID int64 = 0

// NewManager 创建一个新的元数据管理器
func NewManager(metaDir string) (*Manager, error) {
	// 创建元数据目录
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create meta directory: %v", err)
	}

	return &Manager{
		metaDir:         metaDir,
		cache:           make(map[string][]byte),
		watchers:        make(map[string][]watchCallback),
		persistInterval: 5 * time.Minute, // 默认5分钟持久化一次元数据
		stopCh:          make(chan struct{}),
	}, nil
}

// Start 启动元数据管理器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("meta manager already started")
	}

	// 加载元数据
	if err := m.loadMetaData(); err != nil {
		return fmt.Errorf("failed to load meta data: %v", err)
	}

	// 启动持久化定时器
	m.persistTimer = time.NewTimer(m.persistInterval)
	go m.persistLoop()

	m.running = true
	return nil
}

// Stop 停止元数据管理器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("meta manager not started")
	}

	// 停止持久化定时器
	if m.persistTimer != nil {
		m.persistTimer.Stop()
	}
	close(m.stopCh)

	// 持久化元数据
	if err := m.persistMetaData(); err != nil {
		return fmt.Errorf("failed to persist meta data: %v", err)
	}

	m.running = false
	return nil
}

// GetMetaData 获取元数据
func (m *Manager) GetMetaData(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 从缓存中获取元数据
	value, exists := m.cache[key]
	if !exists {
		return nil, fmt.Errorf("meta data not found: %s", key)
	}

	// 返回元数据的副本
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy, nil
}

// SetMetaData 设置元数据
func (m *Manager) SetMetaData(ctx context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 检查是否有变化
	oldValue, exists := m.cache[key]
	if exists && string(oldValue) == string(value) {
		return nil
	}

	// 更新缓存
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	m.cache[key] = valueCopy

	// 通知监视者
	m.notifyWatchers(key, valueCopy, false)

	return nil
}

// DeleteMetaData 删除元数据
func (m *Manager) DeleteMetaData(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 检查元数据是否存在
	_, exists := m.cache[key]
	if !exists {
		return fmt.Errorf("meta data not found: %s", key)
	}

	// 从缓存中删除元数据
	delete(m.cache, key)

	// 通知监视者
	m.notifyWatchers(key, nil, true)

	return nil
}

// ListMetaData 列出元数据
func (m *Manager) ListMetaData(ctx context.Context, prefix string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 创建结果映射
	result := make(map[string][]byte)

	// 遍历缓存，查找匹配前缀的元数据
	for key, value := range m.cache {
		if strings.HasPrefix(key, prefix) {
			// 返回元数据的副本
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			result[key] = valueCopy
		}
	}

	return result, nil
}

// WatchMetaData 监视元数据变化
func (m *Manager) WatchMetaData(ctx context.Context, prefix string, callback func(key string, value []byte, deleted bool)) {
	m.mu.Lock()

	// 生成唯一的回调函数ID
	callbackID := nextCallbackID
	nextCallbackID++

	// 创建监视回调函数
	watcher := watchCallback{
		prefix:   prefix,
		callback: callback,
		id:       callbackID,
	}

	// 添加到监视者列表
	m.watchers[prefix] = append(m.watchers[prefix], watcher)
	m.mu.Unlock()

	// 启动监视协程
	go func() {
		<-ctx.Done()
		// 上下文取消时，移除监视者
		m.mu.Lock()
		defer m.mu.Unlock()
		watchers := m.watchers[prefix]
		for i, w := range watchers {
			if w.id == callbackID {
				m.watchers[prefix] = append(watchers[:i], watchers[i+1:]...)
				break
			}
		}
		if len(m.watchers[prefix]) == 0 {
			delete(m.watchers, prefix)
		}
	}()
}

// Backup 备份元数据
func (m *Manager) Backup(ctx context.Context, backupPath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 创建备份目录
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %v", err)
	}

	// 遍历缓存，将元数据写入备份文件
	for key, value := range m.cache {
		// 创建文件路径
		filePath := filepath.Join(backupPath, strings.ReplaceAll(key, "/", "_"))

		// 写入文件
		if err := os.WriteFile(filePath, value, 0644); err != nil {
			return fmt.Errorf("failed to write backup file: %v", err)
		}
	}

	return nil
}

// Restore 恢复元数据
func (m *Manager) Restore(ctx context.Context, backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 检查备份目录是否存在
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", backupPath)
	}

	// 清空缓存
	m.cache = make(map[string][]byte)

	// 遍历备份目录，加载元数据
	err := filepath.Walk(backupPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过目录
		if info.IsDir() {
			return nil
		}

		// 读取文件内容
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read backup file: %v", err)
		}

		// 提取键名
		key := strings.ReplaceAll(filepath.Base(path), "_", "/")

		// 更新缓存
		m.cache[key] = data

		// 通知监视者
		m.notifyWatchers(key, data, false)

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to restore meta data: %v", err)
	}

	return nil
}

// loadMetaData 加载元数据
func (m *Manager) loadMetaData() error {
	// 遍历元数据目录，加载元数据
	err := filepath.Walk(m.metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过目录
		if info.IsDir() {
			return nil
		}

		// 读取文件内容
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read meta file: %v", err)
		}

		// 提取键名
		relPath, err := filepath.Rel(m.metaDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %v", err)
		}
		key := strings.ReplaceAll(relPath, string(filepath.Separator), "/")

		// 更新缓存
		m.cache[key] = data

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to load meta data: %v", err)
	}

	return nil
}

// persistMetaData 持久化元数据
func (m *Manager) persistMetaData() error {
	// 遍历缓存，将元数据写入文件
	for key, value := range m.cache {
		// 创建文件路径
		filePath := filepath.Join(m.metaDir, strings.ReplaceAll(key, "/", string(filepath.Separator)))

		// 创建目录
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %v", err)
		}

		// 写入文件
		if err := os.WriteFile(filePath, value, 0644); err != nil {
			return fmt.Errorf("failed to write meta file: %v", err)
		}
	}

	return nil
}

// persistLoop 定期持久化元数据
func (m *Manager) persistLoop() {
	for {
		select {
		case <-m.persistTimer.C:
			// 持久化元数据
			m.mu.Lock()
			if err := m.persistMetaData(); err != nil {
				fmt.Printf("failed to persist meta data: %v\n", err)
			}
			m.mu.Unlock()

			// 重置定时器
			m.persistTimer.Reset(m.persistInterval)
		case <-m.stopCh:
			return
		}
	}
}

// notifyWatchers 通知监视者
func (m *Manager) notifyWatchers(key string, value []byte, deleted bool) {
	// 遍历所有监视者
	for prefix, watchers := range m.watchers {
		// 检查键名是否匹配前缀
		if strings.HasPrefix(key, prefix) {
			// 通知所有匹配的监视者
			for _, watcher := range watchers {
				go watcher.callback(key, value, deleted)
			}
		}
	}
}

// SetPersistInterval 设置持久化间隔
func (m *Manager) SetPersistInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.persistInterval = interval
	if m.persistTimer != nil {
		m.persistTimer.Reset(interval)
	}
}

// SaveClusterInfo 保存集群信息
func (m *Manager) SaveClusterInfo(ctx context.Context, info *common.ClusterInfo) error {
	// 序列化集群信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, "cluster/info", data)
}

// LoadClusterInfo 加载集群信息
func (m *Manager) LoadClusterInfo(ctx context.Context) (*common.ClusterInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, "cluster/info")
	if err != nil {
		return nil, err
	}

	// 反序列化集群信息
	var info common.ClusterInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster info: %v", err)
	}

	return &info, nil
}

// SaveDNodeInfo 保存物理节点信息
func (m *Manager) SaveDNodeInfo(ctx context.Context, info *common.DNodeInfo) error {
	// 序列化节点信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal dnode info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, fmt.Sprintf("dnode/%s", info.NodeID), data)
}

// LoadDNodeInfo 加载物理节点信息
func (m *Manager) LoadDNodeInfo(ctx context.Context, nodeID string) (*common.DNodeInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, fmt.Sprintf("dnode/%s", nodeID))
	if err != nil {
		return nil, err
	}

	// 反序列化节点信息
	var info common.DNodeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dnode info: %v", err)
	}

	return &info, nil
}

// ListDNodeInfo 列出所有物理节点信息
func (m *Manager) ListDNodeInfo(ctx context.Context) ([]*common.DNodeInfo, error) {
	// 获取所有物理节点元数据
	metaMap, err := m.ListMetaData(ctx, "dnode/")
	if err != nil {
		return nil, err
	}

	// 创建结果列表
	result := make([]*common.DNodeInfo, 0, len(metaMap))

	// 反序列化节点信息
	for _, data := range metaMap {
		var info common.DNodeInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dnode info: %v", err)
		}
		result = append(result, &info)
	}

	return result, nil
}

// SaveMNodeInfo 保存虚拟管理节点信息
func (m *Manager) SaveMNodeInfo(ctx context.Context, info *common.MNodeInfo) error {
	// 序列化节点信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal mnode info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, fmt.Sprintf("mnode/%s", info.NodeID), data)
}

// LoadMNodeInfo 加载虚拟管理节点信息
func (m *Manager) LoadMNodeInfo(ctx context.Context, nodeID string) (*common.MNodeInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, fmt.Sprintf("mnode/%s", nodeID))
	if err != nil {
		return nil, err
	}

	// 反序列化节点信息
	var info common.MNodeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal mnode info: %v", err)
	}

	return &info, nil
}

// ListMNodeInfo 列出所有虚拟管理节点信息
func (m *Manager) ListMNodeInfo(ctx context.Context) ([]*common.MNodeInfo, error) {
	// 获取所有虚拟管理节点元数据
	metaMap, err := m.ListMetaData(ctx, "mnode/")
	if err != nil {
		return nil, err
	}

	// 创建结果列表
	result := make([]*common.MNodeInfo, 0, len(metaMap))

	// 反序列化节点信息
	for _, data := range metaMap {
		var info common.MNodeInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to unmarshal mnode info: %v", err)
		}
		result = append(result, &info)
	}

	return result, nil
}

// SaveVNodeInfo 保存虚拟数据节点信息
func (m *Manager) SaveVNodeInfo(ctx context.Context, info *common.VNodeInfo) error {
	// 序列化节点信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal vnode info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, fmt.Sprintf("vnode/%s", info.NodeID), data)
}

// LoadVNodeInfo 加载虚拟数据节点信息
func (m *Manager) LoadVNodeInfo(ctx context.Context, nodeID string) (*common.VNodeInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, fmt.Sprintf("vnode/%s", nodeID))
	if err != nil {
		return nil, err
	}

	// 反序列化节点信息
	var info common.VNodeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal vnode info: %v", err)
	}

	return &info, nil
}

// ListVNodeInfo 列出所有虚拟数据节点信息
func (m *Manager) ListVNodeInfo(ctx context.Context) ([]*common.VNodeInfo, error) {
	// 获取所有虚拟数据节点元数据
	metaMap, err := m.ListMetaData(ctx, "vnode/")
	if err != nil {
		return nil, err
	}

	// 创建结果列表
	result := make([]*common.VNodeInfo, 0, len(metaMap))

	// 反序列化节点信息
	for _, data := range metaMap {
		var info common.VNodeInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to unmarshal vnode info: %v", err)
		}
		result = append(result, &info)
	}

	return result, nil
}

// SaveVGroupInfo 保存虚拟数据节点组信息
func (m *Manager) SaveVGroupInfo(ctx context.Context, info *common.VGroupInfo) error {
	// 序列化节点组信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal vgroup info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, fmt.Sprintf("vgroup/%s", info.VGroupID), data)
}

// LoadVGroupInfo 加载虚拟数据节点组信息
func (m *Manager) LoadVGroupInfo(ctx context.Context, groupID string) (*common.VGroupInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, fmt.Sprintf("vgroup/%s", groupID))
	if err != nil {
		return nil, err
	}

	// 反序列化节点组信息
	var info common.VGroupInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal vgroup info: %v", err)
	}

	return &info, nil
}

// ListVGroupInfo 列出所有虚拟数据节点组信息
func (m *Manager) ListVGroupInfo(ctx context.Context) ([]*common.VGroupInfo, error) {
	// 获取所有虚拟数据节点组元数据
	metaMap, err := m.ListMetaData(ctx, "vgroup/")
	if err != nil {
		return nil, err
	}

	// 创建结果列表
	result := make([]*common.VGroupInfo, 0, len(metaMap))

	// 反序列化节点组信息
	for _, data := range metaMap {
		var info common.VGroupInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to unmarshal vgroup info: %v", err)
		}
		result = append(result, &info)
	}

	return result, nil
}

// SaveDatabaseClusterInfo 保存数据库集群信息
func (m *Manager) SaveDatabaseClusterInfo(ctx context.Context, info *common.DatabaseClusterInfo) error {
	// 序列化数据库集群信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal database cluster info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, fmt.Sprintf("database/%s", info.DatabaseName), data)
}

// LoadDatabaseClusterInfo 加载数据库集群信息
func (m *Manager) LoadDatabaseClusterInfo(ctx context.Context, dbName string) (*common.DatabaseClusterInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, fmt.Sprintf("database/%s", dbName))
	if err != nil {
		return nil, err
	}

	// 反序列化数据库集群信息
	var info common.DatabaseClusterInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal database cluster info: %v", err)
	}

	return &info, nil
}

// ListDatabaseClusterInfo 列出所有数据库集群信息
func (m *Manager) ListDatabaseClusterInfo(ctx context.Context) ([]*common.DatabaseClusterInfo, error) {
	// 获取所有数据库集群元数据
	metaMap, err := m.ListMetaData(ctx, "database/")
	if err != nil {
		return nil, err
	}

	// 创建结果列表
	result := make([]*common.DatabaseClusterInfo, 0, len(metaMap))

	// 反序列化数据库集群信息
	for _, data := range metaMap {
		var info common.DatabaseClusterInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to unmarshal database cluster info: %v", err)
		}
		result = append(result, &info)
	}

	return result, nil
}

// SaveTableClusterInfo 保存表集群信息
func (m *Manager) SaveTableClusterInfo(ctx context.Context, info *common.TableClusterInfo) error {
	// 序列化表集群信息
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal table cluster info: %v", err)
	}

	// 保存元数据
	return m.SetMetaData(ctx, fmt.Sprintf("table/%s/%s", info.DatabaseName, info.TableName), data)
}

// LoadTableClusterInfo 加载表集群信息
func (m *Manager) LoadTableClusterInfo(ctx context.Context, dbName, tableName string) (*common.TableClusterInfo, error) {
	// 获取元数据
	data, err := m.GetMetaData(ctx, fmt.Sprintf("table/%s/%s", dbName, tableName))
	if err != nil {
		return nil, err
	}

	// 反序列化表集群信息
	var info common.TableClusterInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table cluster info: %v", err)
	}

	return &info, nil
}

// ListTableClusterInfo 列出数据库中所有表集群信息
func (m *Manager) ListTableClusterInfo(ctx context.Context, dbName string) ([]*common.TableClusterInfo, error) {
	// 获取数据库中所有表集群元数据
	metaMap, err := m.ListMetaData(ctx, fmt.Sprintf("table/%s/", dbName))
	if err != nil {
		return nil, err
	}

	// 创建结果列表
	result := make([]*common.TableClusterInfo, 0, len(metaMap))

	// 反序列化表集群信息
	for _, data := range metaMap {
		var info common.TableClusterInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to unmarshal table cluster info: %v", err)
		}
		result = append(result, &info)
	}

	return result, nil
}
