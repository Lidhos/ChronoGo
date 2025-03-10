package dnode

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	netstat "github.com/shirou/gopsutil/v3/net"

	"ChronoGo/pkg/cluster/common"
)

// Manager 实现了物理节点管理器接口
type Manager struct {
	// 节点信息
	info *common.DNodeInfo
	// 配置信息
	config *common.ClusterConfig
	// 虚拟数据节点管理器映射表
	vnodeMgrs map[string]common.VNodeManager
	// 虚拟管理节点管理器
	mnodeMgr common.MNodeManager
	// 心跳管理器
	heartbeatMgr common.HeartbeatManager
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 统计信息收集间隔
	statsInterval time.Duration
	// 统计信息收集定时器
	statsTimer *time.Timer
	// 停止信号通道
	stopCh chan struct{}
}

// NewManager 创建一个新的物理节点管理器
func NewManager(config *common.ClusterConfig) (*Manager, error) {
	// 生成节点ID
	nodeID := uuid.New().String()

	// 解析IP地址
	var publicIP, privateIP, internalIP net.IP
	if config.PublicIP != "" {
		publicIP = net.ParseIP(config.PublicIP)
		if publicIP == nil {
			return nil, fmt.Errorf("invalid public IP: %s", config.PublicIP)
		}
	}
	if config.PrivateIP != "" {
		privateIP = net.ParseIP(config.PrivateIP)
		if privateIP == nil {
			return nil, fmt.Errorf("invalid private IP: %s", config.PrivateIP)
		}
	} else {
		// 如果未指定私有IP，使用公共IP
		privateIP = publicIP
	}
	if config.InternalIP != "" {
		internalIP = net.ParseIP(config.InternalIP)
		if internalIP == nil {
			return nil, fmt.Errorf("invalid internal IP: %s", config.InternalIP)
		}
	} else {
		// 如果未指定内部IP，使用私有IP
		internalIP = privateIP
	}

	// 创建节点信息
	info := &common.DNodeInfo{
		NodeInfo: common.NodeInfo{
			NodeID:      nodeID,
			NodeType:    common.NodeTypeDNode,
			Status:      common.NodeStatusStarting,
			Role:        common.NodeRoleUnknown,
			PublicIP:    publicIP,
			PrivateIP:   privateIP,
			InternalIP:  internalIP,
			PublicPort:  config.PublicPort,
			PrivatePort: config.PrivatePort,
			StartTime:   time.Now(),
		},
		VNodes: make([]string, 0),
	}

	return &Manager{
		info:          info,
		config:        config,
		vnodeMgrs:     make(map[string]common.VNodeManager),
		statsInterval: 10 * time.Second, // 默认10秒收集一次统计信息
		stopCh:        make(chan struct{}),
	}, nil
}

// Start 启动物理节点管理器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("dnode manager already started")
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusStarting

	// 创建数据目录
	if err := os.MkdirAll(m.config.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// 启动统计信息收集
	m.statsTimer = time.NewTimer(m.statsInterval)
	go m.collectStatsLoop()

	// 如果有心跳管理器，启动心跳
	if m.heartbeatMgr != nil {
		if err := m.heartbeatMgr.Start(); err != nil {
			return fmt.Errorf("failed to start heartbeat manager: %v", err)
		}
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusOnline
	m.running = true

	return nil
}

// Stop 停止物理节点管理器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("dnode manager not started")
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusStopping

	// 停止统计信息收集
	if m.statsTimer != nil {
		m.statsTimer.Stop()
	}
	close(m.stopCh)

	// 停止所有虚拟数据节点
	for _, vnodeMgr := range m.vnodeMgrs {
		if err := vnodeMgr.Stop(); err != nil {
			// 记录错误但继续停止其他节点
			fmt.Printf("failed to stop vnode: %v\n", err)
		}
	}

	// 停止虚拟管理节点
	if m.mnodeMgr != nil {
		if err := m.mnodeMgr.Stop(); err != nil {
			fmt.Printf("failed to stop mnode: %v\n", err)
		}
	}

	// 如果有心跳管理器，停止心跳
	if m.heartbeatMgr != nil {
		if err := m.heartbeatMgr.Stop(); err != nil {
			fmt.Printf("failed to stop heartbeat manager: %v\n", err)
		}
	}

	// 更新节点状态
	m.info.Status = common.NodeStatusOffline
	m.running = false

	return nil
}

// GetInfo 获取物理节点信息
func (m *Manager) GetInfo() (*common.DNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 返回节点信息的副本
	infoCopy := *m.info
	return &infoCopy, nil
}

// UpdateInfo 更新物理节点信息
func (m *Manager) UpdateInfo(info *common.DNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新节点信息
	m.info = info
	return nil
}

// CreateVNode 创建虚拟数据节点
func (m *Manager) CreateVNode(dbName string, vgroupID string) (*common.VNodeInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否达到最大虚拟数据节点数
	if len(m.vnodeMgrs) >= m.config.MaxVNodesPerDNode {
		return nil, fmt.Errorf("reached maximum number of vnodes per dnode: %d", m.config.MaxVNodesPerDNode)
	}

	// 生成虚拟数据节点ID
	vnodeID := uuid.New().String()

	// 创建虚拟数据节点信息
	vnodeInfo := &common.VNodeInfo{
		NodeInfo: common.NodeInfo{
			NodeID:      vnodeID,
			NodeType:    common.NodeTypeVNode,
			Status:      common.NodeStatusStarting,
			Role:        common.NodeRoleSlave, // 默认为从节点，后续可能会被选为主节点
			PublicIP:    m.info.PublicIP,
			PrivateIP:   m.info.PrivateIP,
			InternalIP:  m.info.InternalIP,
			PublicPort:  m.info.PublicPort,
			PrivatePort: m.info.PrivatePort,
			StartTime:   time.Now(),
		},
		DNodeID:      m.info.NodeID,
		VGroupID:     vgroupID,
		DatabaseName: dbName,
		TableCount:   0,
		DataSize:     0,
	}

	// TODO: 创建并启动虚拟数据节点管理器
	// vnodeMgr, err := vnode.NewManager(vnodeInfo, m.config)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create vnode manager: %v", err)
	// }
	// if err := vnodeMgr.Start(); err != nil {
	// 	return nil, fmt.Errorf("failed to start vnode manager: %v", err)
	// }
	// m.vnodeMgrs[vnodeID] = vnodeMgr

	// 更新物理节点信息
	m.info.VNodes = append(m.info.VNodes, vnodeID)

	return vnodeInfo, nil
}

// RemoveVNode 移除虚拟数据节点
func (m *Manager) RemoveVNode(vnodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查虚拟数据节点是否存在
	vnodeMgr, exists := m.vnodeMgrs[vnodeID]
	if !exists {
		return fmt.Errorf("vnode not found: %s", vnodeID)
	}

	// 停止虚拟数据节点
	if err := vnodeMgr.Stop(); err != nil {
		return fmt.Errorf("failed to stop vnode: %v", err)
	}

	// 从映射表中移除
	delete(m.vnodeMgrs, vnodeID)

	// 更新物理节点信息
	for i, id := range m.info.VNodes {
		if id == vnodeID {
			m.info.VNodes = append(m.info.VNodes[:i], m.info.VNodes[i+1:]...)
			break
		}
	}

	return nil
}

// GetVNodeInfo 获取虚拟数据节点信息
func (m *Manager) GetVNodeInfo(vnodeID string) (*common.VNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查虚拟数据节点是否存在
	vnodeMgr, exists := m.vnodeMgrs[vnodeID]
	if !exists {
		return nil, fmt.Errorf("vnode not found: %s", vnodeID)
	}

	// 获取虚拟数据节点信息
	return vnodeMgr.GetInfo()
}

// ListVNodes 列出所有虚拟数据节点
func (m *Manager) ListVNodes() ([]*common.VNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 创建结果列表
	result := make([]*common.VNodeInfo, 0, len(m.vnodeMgrs))

	// 获取所有虚拟数据节点信息
	for _, vnodeMgr := range m.vnodeMgrs {
		info, err := vnodeMgr.GetInfo()
		if err != nil {
			return nil, fmt.Errorf("failed to get vnode info: %v", err)
		}
		result = append(result, info)
	}

	return result, nil
}

// CreateMNode 创建虚拟管理节点
func (m *Manager) CreateMNode() (*common.MNodeInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否已经有虚拟管理节点
	if m.mnodeMgr != nil {
		return nil, fmt.Errorf("mnode already exists")
	}

	// 生成虚拟管理节点ID
	mnodeID := uuid.New().String()

	// 创建虚拟管理节点信息
	mnodeInfo := &common.MNodeInfo{
		NodeInfo: common.NodeInfo{
			NodeID:      mnodeID,
			NodeType:    common.NodeTypeMNode,
			Status:      common.NodeStatusStarting,
			Role:        common.NodeRoleUnknown, // 默认为未知角色，后续会通过选举确定
			PublicIP:    m.info.PublicIP,
			PrivateIP:   m.info.PrivateIP,
			InternalIP:  m.info.InternalIP,
			PublicPort:  m.info.PublicPort,
			PrivatePort: m.info.PrivatePort,
			StartTime:   time.Now(),
		},
		DNodeID:  m.info.NodeID,
		IsMaster: false, // 默认不是主管理节点，后续会通过选举确定
	}

	// TODO: 创建并启动虚拟管理节点管理器
	// mnodeMgr, err := mnode.NewManager(mnodeInfo, m.config)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create mnode manager: %v", err)
	// }
	// if err := mnodeMgr.Start(); err != nil {
	// 	return nil, fmt.Errorf("failed to start mnode manager: %v", err)
	// }
	// m.mnodeMgr = mnodeMgr

	// 更新物理节点信息
	m.info.MNode = mnodeID

	return mnodeInfo, nil
}

// RemoveMNode 移除虚拟管理节点
func (m *Manager) RemoveMNode() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查虚拟管理节点是否存在
	if m.mnodeMgr == nil {
		return fmt.Errorf("mnode not found")
	}

	// 停止虚拟管理节点
	if err := m.mnodeMgr.Stop(); err != nil {
		return fmt.Errorf("failed to stop mnode: %v", err)
	}

	// 清空虚拟管理节点管理器
	m.mnodeMgr = nil

	// 更新物理节点信息
	m.info.MNode = ""

	return nil
}

// GetMNodeInfo 获取虚拟管理节点信息
func (m *Manager) GetMNodeInfo() (*common.MNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查虚拟管理节点是否存在
	if m.mnodeMgr == nil {
		return nil, fmt.Errorf("mnode not found")
	}

	// 获取虚拟管理节点信息
	return m.mnodeMgr.GetInfo()
}

// CollectStats 收集节点统计信息
func (m *Manager) CollectStats() (*common.NodeStats, error) {
	// 收集CPU使用率
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get CPU usage: %v", err)
	}

	// 收集内存使用率
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to get memory usage: %v", err)
	}

	// 收集磁盘使用率
	diskInfo, err := disk.Usage(m.config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk usage: %v", err)
	}

	// 收集网络使用率
	// 这里简化处理，实际应用中可能需要更复杂的网络使用率计算
	netInfo, err := netstat.IOCounters(false)
	if err != nil {
		return nil, fmt.Errorf("failed to get network usage: %v", err)
	}
	var networkUsage float64
	if len(netInfo) > 0 {
		// 简单地使用总流量作为网络使用率的指标
		networkUsage = float64(netInfo[0].BytesSent+netInfo[0].BytesRecv) / 1024 / 1024 // MB
	}

	// 创建统计信息
	stats := &common.NodeStats{
		NodeID:       m.info.NodeID,
		CPUUsage:     cpuPercent[0],
		MemoryUsage:  memInfo.UsedPercent,
		DiskUsage:    diskInfo.UsedPercent,
		NetworkUsage: networkUsage,
		// TODO: 收集QPS和TPS
		QPS:         0,
		TPS:         0,
		Connections: runtime.NumGoroutine(), // 使用goroutine数量作为连接数的近似值
		CollectTime: time.Now(),
	}

	return stats, nil
}

// collectStatsLoop 定期收集统计信息
func (m *Manager) collectStatsLoop() {
	for {
		select {
		case <-m.statsTimer.C:
			// 收集统计信息
			stats, err := m.CollectStats()
			if err != nil {
				fmt.Printf("failed to collect stats: %v\n", err)
			} else {
				// 更新节点信息
				m.mu.Lock()
				m.info.CPUUsage = stats.CPUUsage
				m.info.MemoryUsage = stats.MemoryUsage
				m.info.DiskUsage = stats.DiskUsage
				m.info.NetworkUsage = stats.NetworkUsage
				m.mu.Unlock()

				// 如果有心跳管理器，发送心跳
				if m.heartbeatMgr != nil {
					if err := m.heartbeatMgr.SendHeartbeat(m.info.NodeID, common.NodeTypeDNode); err != nil {
						fmt.Printf("failed to send heartbeat: %v\n", err)
					}
				}
			}

			// 重置定时器
			m.statsTimer.Reset(m.statsInterval)
		case <-m.stopCh:
			return
		}
	}
}

// SetHeartbeatManager 设置心跳管理器
func (m *Manager) SetHeartbeatManager(heartbeatMgr common.HeartbeatManager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatMgr = heartbeatMgr
}

// SetStatsInterval 设置统计信息收集间隔
func (m *Manager) SetStatsInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statsInterval = interval
	if m.statsTimer != nil {
		m.statsTimer.Reset(interval)
	}
}
