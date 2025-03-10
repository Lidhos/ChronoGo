package balance

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"ChronoGo/pkg/cluster/common"
)

// Manager 实现了负载均衡器接口
type Manager struct {
	// 集群管理器
	clusterMgr common.ClusterManager
	// 负载均衡间隔
	balanceInterval time.Duration
	// 负载均衡阈值
	balanceThreshold float64
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 是否正在进行负载均衡
	balancing bool
	// 负载均衡定时器
	balanceTimer *time.Timer
	// 停止信号通道
	stopCh chan struct{}
}

// NewManager 创建一个新的负载均衡器
func NewManager(clusterMgr common.ClusterManager, balanceInterval time.Duration, balanceThreshold float64) *Manager {
	return &Manager{
		clusterMgr:       clusterMgr,
		balanceInterval:  balanceInterval,
		balanceThreshold: balanceThreshold,
		stopCh:           make(chan struct{}),
	}
}

// Start 启动负载均衡器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("load balancer already started")
	}

	// 启动负载均衡定时器
	m.balanceTimer = time.NewTimer(m.balanceInterval)
	go m.balanceLoop()

	m.running = true
	return nil
}

// Stop 停止负载均衡器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("load balancer not started")
	}

	// 停止负载均衡定时器
	if m.balanceTimer != nil {
		m.balanceTimer.Stop()
	}
	close(m.stopCh)

	m.running = false
	return nil
}

// Balance 执行负载均衡
func (m *Manager) Balance() error {
	m.mu.Lock()
	if m.balancing {
		m.mu.Unlock()
		return fmt.Errorf("load balancing already in progress")
	}
	m.balancing = true
	m.mu.Unlock()

	// 完成后重置状态
	defer func() {
		m.mu.Lock()
		m.balancing = false
		m.mu.Unlock()
	}()

	// 获取所有物理节点信息
	dnodes, err := m.clusterMgr.ListDNodes()
	if err != nil {
		return fmt.Errorf("failed to list dnodes: %v", err)
	}

	// 如果节点数量不足，无法进行负载均衡
	if len(dnodes) < 2 {
		return nil
	}

	// 计算每个节点的负载
	nodeLoads := make(map[string]float64)
	for _, dnode := range dnodes {
		// 使用CPU、内存、磁盘和网络使用率的加权平均值作为负载指标
		load := 0.4*dnode.CPUUsage + 0.3*dnode.MemoryUsage + 0.2*dnode.DiskUsage + 0.1*dnode.NetworkUsage
		nodeLoads[dnode.NodeID] = load
	}

	// 计算平均负载
	var totalLoad float64
	for _, load := range nodeLoads {
		totalLoad += load
	}
	avgLoad := totalLoad / float64(len(nodeLoads))

	// 找出负载过高和过低的节点
	var highLoadNodes, lowLoadNodes []string
	for nodeID, load := range nodeLoads {
		if load > avgLoad*(1+m.balanceThreshold) {
			highLoadNodes = append(highLoadNodes, nodeID)
		} else if load < avgLoad*(1-m.balanceThreshold) {
			lowLoadNodes = append(lowLoadNodes, nodeID)
		}
	}

	// 如果没有负载不平衡的节点，无需进行负载均衡
	if len(highLoadNodes) == 0 || len(lowLoadNodes) == 0 {
		return nil
	}

	// 对高负载节点按负载从高到低排序
	sort.Slice(highLoadNodes, func(i, j int) bool {
		return nodeLoads[highLoadNodes[i]] > nodeLoads[highLoadNodes[j]]
	})

	// 对低负载节点按负载从低到高排序
	sort.Slice(lowLoadNodes, func(i, j int) bool {
		return nodeLoads[lowLoadNodes[i]] < nodeLoads[lowLoadNodes[j]]
	})

	// 从高负载节点迁移虚拟节点到低负载节点
	for i := 0; i < len(highLoadNodes) && i < len(lowLoadNodes); i++ {
		highNodeID := highLoadNodes[i]
		lowNodeID := lowLoadNodes[i]

		// 获取高负载节点信息
		highNode, err := m.clusterMgr.GetDNodeInfo(highNodeID)
		if err != nil {
			fmt.Printf("failed to get high load node info: %v\n", err)
			continue
		}

		// 如果高负载节点没有虚拟节点，跳过
		if len(highNode.VNodes) == 0 {
			continue
		}

		// 获取低负载节点信息
		lowNode, err := m.clusterMgr.GetDNodeInfo(lowNodeID)
		if err != nil {
			fmt.Printf("failed to get low load node info: %v\n", err)
			continue
		}

		// 迁移虚拟节点
		if err := m.migrateVNode(highNode, lowNode); err != nil {
			fmt.Printf("failed to migrate vnode: %v\n", err)
		}
	}

	return nil
}

// SetBalanceInterval 设置负载均衡间隔
func (m *Manager) SetBalanceInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.balanceInterval = interval
	if m.balanceTimer != nil {
		m.balanceTimer.Reset(interval)
	}
}

// SetBalanceThreshold 设置负载均衡阈值
func (m *Manager) SetBalanceThreshold(threshold float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.balanceThreshold = threshold
}

// IsBalancing 判断是否正在进行负载均衡
func (m *Manager) IsBalancing() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.balancing
}

// balanceLoop 定期执行负载均衡
func (m *Manager) balanceLoop() {
	for {
		select {
		case <-m.balanceTimer.C:
			// 执行负载均衡
			if err := m.Balance(); err != nil {
				fmt.Printf("failed to balance load: %v\n", err)
			}

			// 重置定时器
			m.balanceTimer.Reset(m.balanceInterval)
		case <-m.stopCh:
			return
		}
	}
}

// migrateVNode 迁移虚拟节点
func (m *Manager) migrateVNode(sourceNode, targetNode *common.DNodeInfo) error {
	// 选择要迁移的虚拟节点
	if len(sourceNode.VNodes) == 0 {
		return fmt.Errorf("source node has no vnodes")
	}

	// 简单起见，选择第一个虚拟节点进行迁移
	vnodeID := sourceNode.VNodes[0]

	// 获取虚拟节点信息
	vnode, err := m.clusterMgr.GetVNodeInfo(vnodeID)
	if err != nil {
		return fmt.Errorf("failed to get vnode info: %v", err)
	}

	// 获取虚拟节点组信息
	vgroup, err := m.clusterMgr.GetVGroupInfo(vnode.VGroupID)
	if err != nil {
		return fmt.Errorf("failed to get vgroup info: %v", err)
	}

	// 检查目标节点是否已经有该虚拟节点组的节点
	for _, id := range vgroup.VNodes {
		vnodeInfo, err := m.clusterMgr.GetVNodeInfo(id)
		if err != nil {
			continue
		}
		if vnodeInfo.DNodeID == targetNode.NodeID {
			return fmt.Errorf("target node already has a vnode in the same vgroup")
		}
	}

	fmt.Printf("Migrating vnode %s from node %s to node %s\n", vnodeID, sourceNode.NodeID, targetNode.NodeID)

	// TODO: 实现虚拟节点迁移的具体逻辑
	// 1. 在目标节点上创建新的虚拟节点
	// 2. 将数据从源虚拟节点同步到目标虚拟节点
	// 3. 更新虚拟节点组信息
	// 4. 更新源节点和目标节点信息

	return fmt.Errorf("vnode migration not implemented")
}

// GetBalanceInterval 获取负载均衡间隔
func (m *Manager) GetBalanceInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.balanceInterval
}

// GetBalanceThreshold 获取负载均衡阈值
func (m *Manager) GetBalanceThreshold() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.balanceThreshold
}
