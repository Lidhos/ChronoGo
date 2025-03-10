package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"ChronoGo/pkg/cluster/common"
	"ChronoGo/pkg/cluster/dnode"
	"ChronoGo/pkg/cluster/heartbeat"
	"ChronoGo/pkg/cluster/meta"
	"ChronoGo/pkg/cluster/mnode"
)

// Manager 实现了集群管理器接口
type Manager struct {
	// 配置信息
	config *common.ClusterConfig
	// 元数据管理器
	metaMgr *meta.Manager
	// 心跳管理器
	heartbeatMgr *heartbeat.Manager
	// 负载均衡器
	balanceMgr common.LoadBalancer
	// 物理节点管理器
	dnodeMgr *dnode.Manager
	// 虚拟管理节点管理器
	mnodeMgr *mnode.Manager
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 集群信息
	clusterInfo *common.ClusterInfo
	// 数据库集群信息映射
	dbClusterInfos map[string]*common.DatabaseClusterInfo
	// 表集群信息映射
	tableClusterInfos map[string]map[string]*common.TableClusterInfo
}

// NewManager 创建一个新的集群管理器
func NewManager(config *common.ClusterConfig) (*Manager, error) {
	// 如果未指定集群ID，生成一个新的
	if config.ClusterID == "" {
		config.ClusterID = uuid.New().String()
	}

	// 创建元数据管理器
	metaMgr, err := meta.NewManager(config.DataDir + "/meta")
	if err != nil {
		return nil, fmt.Errorf("failed to create meta manager: %v", err)
	}

	// 创建心跳管理器
	heartbeatMgr := heartbeat.NewManager(config.HeartbeatInterval, config.OfflineThreshold)

	// 创建物理节点管理器
	dnodeMgr, err := dnode.NewManager(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dnode manager: %v", err)
	}

	// 设置物理节点管理器的心跳管理器
	dnodeMgr.SetHeartbeatManager(heartbeatMgr)

	// 创建集群管理器
	mgr := &Manager{
		config:            config,
		metaMgr:           metaMgr,
		heartbeatMgr:      heartbeatMgr,
		dnodeMgr:          dnodeMgr,
		dbClusterInfos:    make(map[string]*common.DatabaseClusterInfo),
		tableClusterInfos: make(map[string]map[string]*common.TableClusterInfo),
	}

	return mgr, nil
}

// SetBalanceManager 设置负载均衡器
func (m *Manager) SetBalanceManager(balanceMgr common.LoadBalancer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.balanceMgr = balanceMgr
}

// Start 启动集群管理器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("cluster manager already started")
	}

	// 启动元数据管理器
	if err := m.metaMgr.Start(); err != nil {
		return fmt.Errorf("failed to start meta manager: %v", err)
	}

	// 启动心跳管理器
	if err := m.heartbeatMgr.Start(); err != nil {
		return fmt.Errorf("failed to start heartbeat manager: %v", err)
	}

	// 注册心跳处理函数
	m.heartbeatMgr.RegisterHeartbeatHandler(func(nodeID string, nodeType common.NodeType, stats *common.NodeStats) {
		m.handleHeartbeat(nodeID, nodeType, stats)
	})

	// 启动物理节点管理器
	if err := m.dnodeMgr.Start(); err != nil {
		return fmt.Errorf("failed to start dnode manager: %v", err)
	}

	// 加载集群信息
	if err := m.loadClusterInfo(); err != nil {
		return fmt.Errorf("failed to load cluster info: %v", err)
	}

	// 创建虚拟管理节点
	if err := m.createMNode(); err != nil {
		return fmt.Errorf("failed to create mnode: %v", err)
	}

	// 启动负载均衡器
	if m.balanceMgr != nil {
		if err := m.balanceMgr.Start(); err != nil {
			return fmt.Errorf("failed to start balance manager: %v", err)
		}
	}

	m.running = true
	return nil
}

// Stop 停止集群管理器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("cluster manager not started")
	}

	// 停止负载均衡器
	if m.balanceMgr != nil {
		if err := m.balanceMgr.Stop(); err != nil {
			fmt.Printf("failed to stop balance manager: %v\n", err)
		}
	}

	// 停止虚拟管理节点管理器
	if m.mnodeMgr != nil {
		if err := m.mnodeMgr.Stop(); err != nil {
			fmt.Printf("failed to stop mnode manager: %v\n", err)
		}
	}

	// 停止物理节点管理器
	if m.dnodeMgr != nil {
		if err := m.dnodeMgr.Stop(); err != nil {
			fmt.Printf("failed to stop dnode manager: %v\n", err)
		}
	}

	// 停止心跳管理器
	if m.heartbeatMgr != nil {
		if err := m.heartbeatMgr.Stop(); err != nil {
			fmt.Printf("failed to stop heartbeat manager: %v\n", err)
		}
	}

	// 停止元数据管理器
	if m.metaMgr != nil {
		if err := m.metaMgr.Stop(); err != nil {
			fmt.Printf("failed to stop meta manager: %v\n", err)
		}
	}

	m.running = false
	return nil
}

// GetClusterInfo 获取集群信息
func (m *Manager) GetClusterInfo() (*common.ClusterInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.clusterInfo == nil {
		return nil, fmt.Errorf("cluster info not available")
	}

	// 返回集群信息的副本
	infoCopy := *m.clusterInfo
	return &infoCopy, nil
}

// AddDNode 添加物理节点
func (m *Manager) AddDNode(info *common.DNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查节点是否已存在
	_, err := m.GetDNodeInfo(info.NodeID)
	if err == nil {
		return fmt.Errorf("dnode already exists: %s", info.NodeID)
	}

	// 保存节点信息到元数据
	ctx := context.Background()
	if err := m.metaMgr.SaveDNodeInfo(ctx, info); err != nil {
		return fmt.Errorf("failed to save dnode info: %v", err)
	}

	// 更新集群信息
	m.clusterInfo.DNodes = append(m.clusterInfo.DNodes, info.NodeID)
	if err := m.metaMgr.SaveClusterInfo(ctx, m.clusterInfo); err != nil {
		return fmt.Errorf("failed to save cluster info: %v", err)
	}

	return nil
}

// RemoveDNode 移除物理节点
func (m *Manager) RemoveDNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查节点是否存在
	dnode, err := m.GetDNodeInfo(nodeID)
	if err != nil {
		return err
	}

	// 检查节点是否有虚拟节点
	if len(dnode.VNodes) > 0 {
		return fmt.Errorf("cannot remove dnode with vnodes")
	}

	// 检查节点是否有虚拟管理节点
	if dnode.MNode != "" {
		return fmt.Errorf("cannot remove dnode with mnode")
	}

	// 从元数据中删除节点信息
	ctx := context.Background()
	if err := m.metaMgr.DeleteMetaData(ctx, fmt.Sprintf("dnode/%s", nodeID)); err != nil {
		return fmt.Errorf("failed to delete dnode info: %v", err)
	}

	// 更新集群信息
	for i, id := range m.clusterInfo.DNodes {
		if id == nodeID {
			m.clusterInfo.DNodes = append(m.clusterInfo.DNodes[:i], m.clusterInfo.DNodes[i+1:]...)
			break
		}
	}
	if err := m.metaMgr.SaveClusterInfo(ctx, m.clusterInfo); err != nil {
		return fmt.Errorf("failed to save cluster info: %v", err)
	}

	// 从心跳管理器中移除节点
	m.heartbeatMgr.RemoveNode(nodeID)

	return nil
}

// GetDNodeInfo 获取物理节点信息
func (m *Manager) GetDNodeInfo(nodeID string) (*common.DNodeInfo, error) {
	// 从元数据中获取节点信息
	ctx := context.Background()
	return m.metaMgr.LoadDNodeInfo(ctx, nodeID)
}

// ListDNodes 列出所有物理节点
func (m *Manager) ListDNodes() ([]*common.DNodeInfo, error) {
	// 从元数据中获取所有节点信息
	ctx := context.Background()
	return m.metaMgr.ListDNodeInfo(ctx)
}

// GetMNodeInfo 获取虚拟管理节点信息
func (m *Manager) GetMNodeInfo(nodeID string) (*common.MNodeInfo, error) {
	// 从元数据中获取节点信息
	ctx := context.Background()
	return m.metaMgr.LoadMNodeInfo(ctx, nodeID)
}

// ListMNodes 列出所有虚拟管理节点
func (m *Manager) ListMNodes() ([]*common.MNodeInfo, error) {
	// 从元数据中获取所有节点信息
	ctx := context.Background()
	return m.metaMgr.ListMNodeInfo(ctx)
}

// GetVNodeInfo 获取虚拟数据节点信息
func (m *Manager) GetVNodeInfo(nodeID string) (*common.VNodeInfo, error) {
	// 从元数据中获取节点信息
	ctx := context.Background()
	return m.metaMgr.LoadVNodeInfo(ctx, nodeID)
}

// ListVNodes 列出所有虚拟数据节点
func (m *Manager) ListVNodes() ([]*common.VNodeInfo, error) {
	// 从元数据中获取所有节点信息
	ctx := context.Background()
	return m.metaMgr.ListVNodeInfo(ctx)
}

// GetVGroupInfo 获取虚拟数据节点组信息
func (m *Manager) GetVGroupInfo(groupID string) (*common.VGroupInfo, error) {
	// 从元数据中获取节点组信息
	ctx := context.Background()
	return m.metaMgr.LoadVGroupInfo(ctx, groupID)
}

// ListVGroups 列出所有虚拟数据节点组
func (m *Manager) ListVGroups() ([]*common.VGroupInfo, error) {
	// 从元数据中获取所有节点组信息
	ctx := context.Background()
	return m.metaMgr.ListVGroupInfo(ctx)
}

// GetDatabaseClusterInfo 获取数据库在集群中的信息
func (m *Manager) GetDatabaseClusterInfo(dbName string) (*common.DatabaseClusterInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查缓存
	if info, exists := m.dbClusterInfos[dbName]; exists {
		// 返回副本
		infoCopy := *info
		return &infoCopy, nil
	}

	// 从元数据中获取数据库信息
	ctx := context.Background()
	return m.metaMgr.LoadDatabaseClusterInfo(ctx, dbName)
}

// ListDatabaseClusterInfo 列出所有数据库在集群中的信息
func (m *Manager) ListDatabaseClusterInfo() ([]*common.DatabaseClusterInfo, error) {
	// 从元数据中获取所有数据库信息
	ctx := context.Background()
	return m.metaMgr.ListDatabaseClusterInfo(ctx)
}

// GetTableClusterInfo 获取表在集群中的信息
func (m *Manager) GetTableClusterInfo(dbName, tableName string) (*common.TableClusterInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查缓存
	if dbTables, exists := m.tableClusterInfos[dbName]; exists {
		if info, exists := dbTables[tableName]; exists {
			// 返回副本
			infoCopy := *info
			return &infoCopy, nil
		}
	}

	// 从元数据中获取表信息
	ctx := context.Background()
	return m.metaMgr.LoadTableClusterInfo(ctx, dbName, tableName)
}

// ListTableClusterInfo 列出数据库中所有表在集群中的信息
func (m *Manager) ListTableClusterInfo(dbName string) ([]*common.TableClusterInfo, error) {
	// 从元数据中获取数据库中所有表信息
	ctx := context.Background()
	return m.metaMgr.ListTableClusterInfo(ctx, dbName)
}

// GetNodeStats 获取节点统计信息
func (m *Manager) GetNodeStats(nodeID string) (*common.NodeStats, error) {
	// 从心跳管理器中获取节点统计信息
	stats, exists := m.heartbeatMgr.GetNodeStats(nodeID)
	if !exists {
		return nil, fmt.Errorf("node stats not found: %s", nodeID)
	}
	return stats, nil
}

// ListNodeStats 列出所有节点的统计信息
func (m *Manager) ListNodeStats() ([]*common.NodeStats, error) {
	// 获取所有在线节点
	onlineNodes := m.heartbeatMgr.ListOnlineNodes()

	// 创建结果列表
	result := make([]*common.NodeStats, 0, len(onlineNodes))

	// 获取每个节点的统计信息
	for _, nodeID := range onlineNodes {
		stats, exists := m.heartbeatMgr.GetNodeStats(nodeID)
		if exists {
			result = append(result, stats)
		}
	}

	return result, nil
}

// loadClusterInfo 加载集群信息
func (m *Manager) loadClusterInfo() error {
	// 从元数据中加载集群信息
	ctx := context.Background()
	info, err := m.metaMgr.LoadClusterInfo(ctx)
	if err != nil {
		// 如果不存在，创建新的集群信息
		info = &common.ClusterInfo{
			ClusterID:  m.config.ClusterID,
			DNodes:     make([]string, 0),
			MNodes:     make([]string, 0),
			VGroups:    make([]string, 0),
			Databases:  make([]string, 0),
			CreateTime: time.Now(),
			Status:     common.NodeStatusOnline,
		}

		// 保存到元数据
		if err := m.metaMgr.SaveClusterInfo(ctx, info); err != nil {
			return fmt.Errorf("failed to save cluster info: %v", err)
		}
	}

	m.clusterInfo = info
	return nil
}

// createMNode 创建虚拟管理节点
func (m *Manager) createMNode() error {
	// 创建虚拟管理节点
	mnodeInfo, err := m.dnodeMgr.CreateMNode()
	if err != nil {
		return fmt.Errorf("failed to create mnode: %v", err)
	}

	// 创建虚拟管理节点管理器
	m.mnodeMgr, err = mnode.NewManager(mnodeInfo, m.config, m.metaMgr)
	if err != nil {
		return fmt.Errorf("failed to create mnode manager: %v", err)
	}

	// 启动虚拟管理节点管理器
	if err := m.mnodeMgr.Start(); err != nil {
		return fmt.Errorf("failed to start mnode manager: %v", err)
	}

	// 保存虚拟管理节点信息到元数据
	ctx := context.Background()
	if err := m.metaMgr.SaveMNodeInfo(ctx, mnodeInfo); err != nil {
		return fmt.Errorf("failed to save mnode info: %v", err)
	}

	// 更新集群信息
	m.clusterInfo.MNodes = append(m.clusterInfo.MNodes, mnodeInfo.NodeID)
	if err := m.metaMgr.SaveClusterInfo(ctx, m.clusterInfo); err != nil {
		return fmt.Errorf("failed to save cluster info: %v", err)
	}

	return nil
}

// handleHeartbeat 处理心跳
func (m *Manager) handleHeartbeat(nodeID string, nodeType common.NodeType, stats *common.NodeStats) {
	// 根据节点类型处理心跳
	switch nodeType {
	case common.NodeTypeDNode:
		// 更新物理节点状态
		m.updateDNodeStatus(nodeID, stats)
	case common.NodeTypeVNode:
		// 更新虚拟数据节点状态
		m.updateVNodeStatus(nodeID, stats)
	case common.NodeTypeMNode:
		// 更新虚拟管理节点状态
		m.updateMNodeStatus(nodeID, stats)
	}
}

// updateDNodeStatus 更新物理节点状态
func (m *Manager) updateDNodeStatus(nodeID string, stats *common.NodeStats) {
	// 从元数据中获取节点信息
	ctx := context.Background()
	dnode, err := m.metaMgr.LoadDNodeInfo(ctx, nodeID)
	if err != nil {
		return
	}

	// 更新节点状态
	dnode.Status = common.NodeStatusOnline
	dnode.LastHeartbeat = time.Now()

	// 更新统计信息
	if stats != nil {
		dnode.CPUUsage = stats.CPUUsage
		dnode.MemoryUsage = stats.MemoryUsage
		dnode.DiskUsage = stats.DiskUsage
		dnode.NetworkUsage = stats.NetworkUsage
	}

	// 保存到元数据
	if err := m.metaMgr.SaveDNodeInfo(ctx, dnode); err != nil {
		fmt.Printf("failed to save dnode info: %v\n", err)
	}
}

// updateVNodeStatus 更新虚拟数据节点状态
func (m *Manager) updateVNodeStatus(nodeID string, stats *common.NodeStats) {
	// 从元数据中获取节点信息
	ctx := context.Background()
	vnode, err := m.metaMgr.LoadVNodeInfo(ctx, nodeID)
	if err != nil {
		return
	}

	// 更新节点状态
	vnode.Status = common.NodeStatusOnline
	vnode.LastHeartbeat = time.Now()

	// 保存到元数据
	if err := m.metaMgr.SaveVNodeInfo(ctx, vnode); err != nil {
		fmt.Printf("failed to save vnode info: %v\n", err)
	}
}

// updateMNodeStatus 更新虚拟管理节点状态
func (m *Manager) updateMNodeStatus(nodeID string, stats *common.NodeStats) {
	// 从元数据中获取节点信息
	ctx := context.Background()
	mnode, err := m.metaMgr.LoadMNodeInfo(ctx, nodeID)
	if err != nil {
		return
	}

	// 更新节点状态
	mnode.Status = common.NodeStatusOnline
	mnode.LastHeartbeat = time.Now()

	// 保存到元数据
	if err := m.metaMgr.SaveMNodeInfo(ctx, mnode); err != nil {
		fmt.Printf("failed to save mnode info: %v\n", err)
	}
}
