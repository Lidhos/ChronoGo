package vnode

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"ChronoGo/pkg/cluster"
	"ChronoGo/pkg/model"
	"ChronoGo/pkg/storage"
)

// Manager 实现了虚拟数据节点管理器接口
type Manager struct {
	// 节点信息
	info *cluster.VNodeInfo
	// 配置信息
	config *cluster.ClusterConfig
	// 存储引擎
	storageEngine *storage.StorageEngine
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 表列表
	tables []string
	// 数据同步间隔
	syncInterval time.Duration
	// 数据同步定时器
	syncTimer *time.Timer
	// 停止信号通道
	stopCh chan struct{}
}

// NewManager 创建一个新的虚拟数据节点管理器
func NewManager(info *cluster.VNodeInfo, config *cluster.ClusterConfig) (*Manager, error) {
	// 创建虚拟数据节点数据目录
	vnodeDataDir := filepath.Join(config.DataDir, "vnodes", info.NodeID)
	if err := os.MkdirAll(vnodeDataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create vnode data directory: %v", err)
	}

	// 初始化存储引擎
	storageEngine, err := storage.NewStorageEngine(vnodeDataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage engine: %v", err)
	}

	return &Manager{
		info:          info,
		config:        config,
		storageEngine: storageEngine,
		tables:        make([]string, 0),
		syncInterval:  5 * time.Minute, // 默认5分钟同步一次数据
		stopCh:        make(chan struct{}),
	}, nil
}

// Start 启动虚拟数据节点管理器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("vnode manager already started")
	}

	// 更新节点状态
	m.info.Status = cluster.NodeStatusStarting

	// 加载表列表
	tables, err := m.storageEngine.ListTables(m.info.DatabaseName)
	if err != nil {
		return fmt.Errorf("failed to list tables: %v", err)
	}

	// 提取表名
	m.tables = make([]string, 0, len(tables))
	for _, table := range tables {
		m.tables = append(m.tables, table.Name)
	}
	m.info.TableCount = len(m.tables)

	// 如果是从节点，启动数据同步
	if m.info.Role == cluster.NodeRoleSlave {
		m.syncTimer = time.NewTimer(m.syncInterval)
		go m.syncDataLoop()
	}

	// 更新节点状态
	m.info.Status = cluster.NodeStatusOnline
	m.running = true

	return nil
}

// Stop 停止虚拟数据节点管理器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("vnode manager not started")
	}

	// 更新节点状态
	m.info.Status = cluster.NodeStatusStopping

	// 停止数据同步
	if m.syncTimer != nil {
		m.syncTimer.Stop()
	}
	close(m.stopCh)

	// 关闭存储引擎
	if err := m.storageEngine.Close(); err != nil {
		return fmt.Errorf("failed to close storage engine: %v", err)
	}

	// 更新节点状态
	m.info.Status = cluster.NodeStatusOffline
	m.running = false

	return nil
}

// GetInfo 获取虚拟数据节点信息
func (m *Manager) GetInfo() (*cluster.VNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回节点信息的副本
	infoCopy := *m.info
	return &infoCopy, nil
}

// UpdateInfo 更新虚拟数据节点信息
func (m *Manager) UpdateInfo(info *cluster.VNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新节点信息
	m.info = info
	return nil
}

// CreateTable 在虚拟数据节点上创建表
func (m *Manager) CreateTable(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查表是否已存在
	for _, name := range m.tables {
		if name == tableName {
			return fmt.Errorf("table already exists: %s", tableName)
		}
	}

	// 创建默认的表结构
	schema := model.Schema{
		TimeField: "timestamp",
		TagFields: map[string]string{"tags": "object"},
		Fields:    map[string]string{},
	}

	// 创建默认的标签索引
	tagIndexes := []model.TagIndex{
		{
			Name:          "tags_idx",
			Fields:        []string{"tags"},
			Type:          "inverted",
			SupportRange:  true,
			SupportPrefix: true,
		},
	}

	// 创建表
	if err := m.storageEngine.CreateTable(m.info.DatabaseName, tableName, schema, tagIndexes); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// 更新表列表
	m.tables = append(m.tables, tableName)
	m.info.TableCount = len(m.tables)

	return nil
}

// DropTable 在虚拟数据节点上删除表
func (m *Manager) DropTable(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查表是否存在
	tableExists := false
	for _, name := range m.tables {
		if name == tableName {
			tableExists = true
			break
		}
	}
	if !tableExists {
		return fmt.Errorf("table not found: %s", tableName)
	}

	// 删除表
	if err := m.storageEngine.DropTable(m.info.DatabaseName, tableName); err != nil {
		return fmt.Errorf("failed to drop table: %v", err)
	}

	// 更新表列表
	for i, name := range m.tables {
		if name == tableName {
			m.tables = append(m.tables[:i], m.tables[i+1:]...)
			break
		}
	}
	m.info.TableCount = len(m.tables)

	return nil
}

// ListTables 列出虚拟数据节点上的所有表
func (m *Manager) ListTables() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回表列表的副本
	tablesCopy := make([]string, len(m.tables))
	copy(tablesCopy, m.tables)
	return tablesCopy, nil
}

// SyncFromMaster 从主节点同步数据
func (m *Manager) SyncFromMaster(masterVNodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否是从节点
	if m.info.Role != cluster.NodeRoleSlave {
		return fmt.Errorf("only slave vnode can sync from master")
	}

	// TODO: 实现从主节点同步数据的逻辑
	// 1. 获取主节点的连接信息
	// 2. 建立连接
	// 3. 获取主节点的表列表
	// 4. 对比表列表，创建缺失的表
	// 5. 同步表数据

	return fmt.Errorf("not implemented")
}

// SyncToSlave 同步数据到从节点
func (m *Manager) SyncToSlave(slaveVNodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否是主节点
	if m.info.Role != cluster.NodeRoleMaster {
		return fmt.Errorf("only master vnode can sync to slave")
	}

	// TODO: 实现同步数据到从节点的逻辑
	// 1. 获取从节点的连接信息
	// 2. 建立连接
	// 3. 发送表列表
	// 4. 发送表数据

	return fmt.Errorf("not implemented")
}

// Backup 备份数据
func (m *Manager) Backup(backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 创建备份目录
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %v", err)
	}

	// TODO: 实现数据备份逻辑
	// 1. 获取表列表
	// 2. 对每个表进行备份
	// 3. 备份元数据

	return fmt.Errorf("not implemented")
}

// Restore 恢复数据
func (m *Manager) Restore(backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查备份目录是否存在
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", backupPath)
	}

	// TODO: 实现数据恢复逻辑
	// 1. 恢复元数据
	// 2. 获取备份的表列表
	// 3. 对每个表进行恢复

	return fmt.Errorf("not implemented")
}

// syncDataLoop 定期从主节点同步数据
func (m *Manager) syncDataLoop() {
	for {
		select {
		case <-m.syncTimer.C:
			// 如果是从节点，从主节点同步数据
			if m.info.Role == cluster.NodeRoleSlave {
				// TODO: 获取主节点ID
				masterVNodeID := ""
				if err := m.SyncFromMaster(masterVNodeID); err != nil {
					fmt.Printf("failed to sync from master: %v\n", err)
				}
			}

			// 重置定时器
			m.syncTimer.Reset(m.syncInterval)
		case <-m.stopCh:
			return
		}
	}
}

// SetSyncInterval 设置数据同步间隔
func (m *Manager) SetSyncInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncInterval = interval
	if m.syncTimer != nil {
		m.syncTimer.Reset(interval)
	}
}

// SetRole 设置节点角色
func (m *Manager) SetRole(role cluster.NodeRole) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新节点角色
	m.info.Role = role

	// 如果角色变为从节点，启动数据同步
	if role == cluster.NodeRoleSlave && m.running && m.syncTimer == nil {
		m.syncTimer = time.NewTimer(m.syncInterval)
		go m.syncDataLoop()
	}

	// 如果角色变为主节点，停止数据同步
	if role == cluster.NodeRoleMaster && m.syncTimer != nil {
		m.syncTimer.Stop()
		m.syncTimer = nil
	}

	return nil
}
