package mnode

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"ChronoGo/pkg/cluster/common"
)

// Manager 实现了虚拟管理节点管理器接口
type Manager struct {
	// 节点信息
	info *common.MNodeInfo
	// 配置信息
	config *common.ClusterConfig
	// 元数据管理器
	metaManager common.MetaDataManager
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 主节点选举间隔
	electionInterval time.Duration
	// 主节点选举定时器
	electionTimer *time.Timer
	// 元数据同步间隔
	syncInterval time.Duration
	// 元数据同步定时器
	syncTimer *time.Timer
	// 停止信号通道
	stopCh chan struct{}
	// 其他管理节点列表
	otherMNodes []*common.MNodeInfo
	// 集群信息
	clusterInfo *common.ClusterInfo
}

// NewManager 创建一个新的虚拟管理节点管理器
func NewManager(info *common.MNodeInfo, config *common.ClusterConfig, metaManager common.MetaDataManager) (*Manager, error) {
	// 创建虚拟管理节点数据目录
	mnodeDataDir := filepath.Join(config.DataDir, "mnodes", info.NodeID)
	if err := os.MkdirAll(mnodeDataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create mnode data directory: %v", err)
	}

	// 创建集群信息
	clusterInfo := &common.ClusterInfo{
		ClusterID:  config.ClusterID,
		DNodes:     make([]string, 0),
		MNodes:     make([]string, 0),
		VGroups:    make([]string, 0),
		Databases:  make([]string, 0),
		CreateTime: time.Now(),
		Status:     common.NodeStatusStarting,
	}

	return &Manager{
		info:             info,
		config:           config,
		metaManager:      metaManager,
		electionInterval: 30 * time.Second, // 默认30秒进行一次主节点选举
		syncInterval:     5 * time.Minute,  // 默认5分钟同步一次元数据
		stopCh:           make(chan struct{}),
		otherMNodes:      make([]*common.MNodeInfo, 0),
		clusterInfo:      clusterInfo,
	}, nil
}

// Start 启动虚拟管理节点管理器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("mnode manager already started")
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusStarting

	// 加载集群信息
	if err := m.loadClusterInfo(); err != nil {
		return fmt.Errorf("failed to load cluster info: %v", err)
	}

	// 加载其他管理节点信息
	if err := m.loadOtherMNodes(); err != nil {
		return fmt.Errorf("failed to load other mnodes: %v", err)
	}

	// 启动主节点选举
	m.electionTimer = time.NewTimer(m.electionInterval)
	go m.electionLoop()

	// 如果是从节点，启动元数据同步
	if !m.info.IsMaster {
		m.syncTimer = time.NewTimer(m.syncInterval)
		go m.syncMetaDataLoop()
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusOnline
	m.running = true

	return nil
}

// Stop 停止虚拟管理节点管理器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("mnode manager not started")
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusStopping

	// 停止主节点选举
	if m.electionTimer != nil {
		m.electionTimer.Stop()
	}

	// 停止元数据同步
	if m.syncTimer != nil {
		m.syncTimer.Stop()
	}

	close(m.stopCh)

	// 更新节点状态
	m.info.Status = common.NodeStatusOffline
	m.running = false

	return nil
}

// GetInfo 获取虚拟管理节点信息
func (m *Manager) GetInfo() (*common.MNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回节点信息的副本
	infoCopy := *m.info
	return &infoCopy, nil
}

// UpdateInfo 更新虚拟管理节点信息
func (m *Manager) UpdateInfo(info *common.MNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新节点信息
	m.info = info
	return nil
}

// IsMaster 判断是否是主管理节点
func (m *Manager) IsMaster() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.info.IsMaster
}

// ElectMaster 选举主管理节点
func (m *Manager) ElectMaster() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果没有其他管理节点，自己成为主节点
	if len(m.otherMNodes) == 0 {
		m.info.IsMaster = true
		m.info.Role = common.NodeRoleMaster
		return nil
	}

	// TODO: 实现基于Raft或其他共识算法的主节点选举
	// 这里简化处理，使用节点ID的字典序作为选举依据
	var minNodeID string
	if len(m.otherMNodes) > 0 {
		minNodeID = m.otherMNodes[0].NodeID
		for _, node := range m.otherMNodes {
			if node.NodeID < minNodeID {
				minNodeID = node.NodeID
			}
		}
	}

	// 如果自己的节点ID最小，成为主节点
	if m.info.NodeID < minNodeID {
		m.info.IsMaster = true
		m.info.Role = common.NodeRoleMaster
	} else {
		m.info.IsMaster = false
		m.info.Role = common.NodeRoleSlave
	}

	return nil
}

// SyncFromMaster 从主管理节点同步元数据
func (m *Manager) SyncFromMaster(masterMNodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否是从节点
	if m.info.IsMaster {
		return fmt.Errorf("only slave mnode can sync from master")
	}

	// TODO: 实现从主管理节点同步元数据的逻辑
	// 1. 获取主管理节点的连接信息
	// 2. 建立连接
	// 3. 获取主管理节点的元数据
	// 4. 更新本地元数据

	return fmt.Errorf("not implemented")
}

// SyncToSlave 同步元数据到从管理节点
func (m *Manager) SyncToSlave(slaveMNodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否是主节点
	if !m.info.IsMaster {
		return fmt.Errorf("only master mnode can sync to slave")
	}

	// TODO: 实现同步元数据到从管理节点的逻辑
	// 1. 获取从管理节点的连接信息
	// 2. 建立连接
	// 3. 发送元数据

	return fmt.Errorf("not implemented")
}

// loadClusterInfo 加载集群信息
func (m *Manager) loadClusterInfo() error {
	// TODO: 从元数据管理器加载集群信息
	return nil
}

// loadOtherMNodes 加载其他管理节点信息
func (m *Manager) loadOtherMNodes() error {
	// TODO: 从元数据管理器加载其他管理节点信息
	return nil
}

// electionLoop 定期进行主节点选举
func (m *Manager) electionLoop() {
	for {
		select {
		case <-m.electionTimer.C:
			// 进行主节点选举
			if err := m.ElectMaster(); err != nil {
				fmt.Printf("failed to elect master: %v\n", err)
			}

			// 重置定时器
			m.electionTimer.Reset(m.electionInterval)
		case <-m.stopCh:
			return
		}
	}
}

// syncMetaDataLoop 定期从主节点同步元数据
func (m *Manager) syncMetaDataLoop() {
	for {
		select {
		case <-m.syncTimer.C:
			// 如果是从节点，从主节点同步元数据
			if !m.info.IsMaster {
				// 查找主节点
				var masterMNodeID string
				m.mu.RLock()
				for _, node := range m.otherMNodes {
					if node.IsMaster {
						masterMNodeID = node.NodeID
						break
					}
				}
				m.mu.RUnlock()

				if masterMNodeID != "" {
					if err := m.SyncFromMaster(masterMNodeID); err != nil {
						fmt.Printf("failed to sync from master: %v\n", err)
					}
				}
			}

			// 重置定时器
			m.syncTimer.Reset(m.syncInterval)
		case <-m.stopCh:
			return
		}
	}
}

// SetElectionInterval 设置主节点选举间隔
func (m *Manager) SetElectionInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.electionInterval = interval
	if m.electionTimer != nil {
		m.electionTimer.Reset(interval)
	}
}

// SetSyncInterval 设置元数据同步间隔
func (m *Manager) SetSyncInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncInterval = interval
	if m.syncTimer != nil {
		m.syncTimer.Reset(interval)
	}
}

// AddMNode 添加管理节点
func (m *Manager) AddMNode(info *common.MNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查节点是否已存在
	for _, node := range m.otherMNodes {
		if node.NodeID == info.NodeID {
			return fmt.Errorf("mnode already exists: %s", info.NodeID)
		}
	}

	// 添加节点
	m.otherMNodes = append(m.otherMNodes, info)

	// 更新集群信息
	if !contains(m.clusterInfo.MNodes, info.NodeID) {
		m.clusterInfo.MNodes = append(m.clusterInfo.MNodes, info.NodeID)
	}

	return nil
}

// RemoveMNode 移除管理节点
func (m *Manager) RemoveMNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查节点是否存在
	nodeExists := false
	for i, node := range m.otherMNodes {
		if node.NodeID == nodeID {
			// 移除节点
			m.otherMNodes = append(m.otherMNodes[:i], m.otherMNodes[i+1:]...)
			nodeExists = true
			break
		}
	}
	if !nodeExists {
		return fmt.Errorf("mnode not found: %s", nodeID)
	}

	// 更新集群信息
	for i, id := range m.clusterInfo.MNodes {
		if id == nodeID {
			m.clusterInfo.MNodes = append(m.clusterInfo.MNodes[:i], m.clusterInfo.MNodes[i+1:]...)
			break
		}
	}

	return nil
}

// GetClusterInfo 获取集群信息
func (m *Manager) GetClusterInfo() (*common.ClusterInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回集群信息的副本
	infoCopy := *m.clusterInfo
	return &infoCopy, nil
}

// UpdateClusterInfo 更新集群信息
func (m *Manager) UpdateClusterInfo(info *common.ClusterInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新集群信息
	m.clusterInfo = info
	return nil
}

// contains 检查切片是否包含指定元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
