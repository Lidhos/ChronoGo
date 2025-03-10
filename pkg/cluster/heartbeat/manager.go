package heartbeat

import (
	"fmt"
	"sync"
	"time"

	"ChronoGo/pkg/cluster/common"
)

// Manager 实现了心跳管理器接口
type Manager struct {
	// 心跳间隔
	heartbeatInterval time.Duration
	// 离线阈值
	offlineThreshold time.Duration
	// 节点心跳时间映射
	nodeHeartbeats map[string]time.Time
	// 节点类型映射
	nodeTypes map[string]common.NodeType
	// 节点统计信息映射
	nodeStats map[string]*common.NodeStats
	// 心跳处理函数
	handlers []func(nodeID string, nodeType common.NodeType, stats *common.NodeStats)
	// 互斥锁
	mu sync.RWMutex
	// 是否正在运行
	running bool
	// 心跳检查定时器
	checkTimer *time.Timer
	// 停止信号通道
	stopCh chan struct{}
}

// NewManager 创建一个新的心跳管理器
func NewManager(heartbeatInterval, offlineThreshold time.Duration) *Manager {
	return &Manager{
		heartbeatInterval: heartbeatInterval,
		offlineThreshold:  offlineThreshold,
		nodeHeartbeats:    make(map[string]time.Time),
		nodeTypes:         make(map[string]common.NodeType),
		nodeStats:         make(map[string]*common.NodeStats),
		handlers:          make([]func(nodeID string, nodeType common.NodeType, stats *common.NodeStats), 0),
		stopCh:            make(chan struct{}),
	}
}

// Start 启动心跳管理器
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("heartbeat manager already started")
	}

	// 启动心跳检查定时器
	m.checkTimer = time.NewTimer(m.heartbeatInterval)
	go m.checkHeartbeatsLoop()

	m.running = true
	return nil
}

// Stop 停止心跳管理器
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("heartbeat manager not started")
	}

	// 停止心跳检查定时器
	if m.checkTimer != nil {
		m.checkTimer.Stop()
	}
	close(m.stopCh)

	m.running = false
	return nil
}

// SendHeartbeat 发送心跳
func (m *Manager) SendHeartbeat(nodeID string, nodeType common.NodeType) error {
	return m.SendHeartbeatWithStats(nodeID, nodeType, nil)
}

// SendHeartbeatWithStats 发送带统计信息的心跳
func (m *Manager) SendHeartbeatWithStats(nodeID string, nodeType common.NodeType, stats *common.NodeStats) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新心跳时间
	m.nodeHeartbeats[nodeID] = time.Now()
	m.nodeTypes[nodeID] = nodeType

	// 更新统计信息
	if stats != nil {
		m.nodeStats[nodeID] = stats
	}

	// 通知处理函数
	for _, handler := range m.handlers {
		go handler(nodeID, nodeType, stats)
	}

	return nil
}

// SetHeartbeatInterval 设置心跳间隔
func (m *Manager) SetHeartbeatInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatInterval = interval
	if m.checkTimer != nil {
		m.checkTimer.Reset(interval)
	}
}

// SetOfflineThreshold 设置离线阈值
func (m *Manager) SetOfflineThreshold(threshold time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.offlineThreshold = threshold
}

// RegisterHeartbeatHandler 注册心跳处理函数
func (m *Manager) RegisterHeartbeatHandler(handler func(nodeID string, nodeType common.NodeType, stats *common.NodeStats)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
}

// GetNodeHeartbeat 获取节点心跳时间
func (m *Manager) GetNodeHeartbeat(nodeID string) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	heartbeat, exists := m.nodeHeartbeats[nodeID]
	return heartbeat, exists
}

// GetNodeType 获取节点类型
func (m *Manager) GetNodeType(nodeID string) (common.NodeType, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeType, exists := m.nodeTypes[nodeID]
	return nodeType, exists
}

// GetNodeStats 获取节点统计信息
func (m *Manager) GetNodeStats(nodeID string) (*common.NodeStats, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats, exists := m.nodeStats[nodeID]
	return stats, exists
}

// IsNodeOnline 判断节点是否在线
func (m *Manager) IsNodeOnline(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	heartbeat, exists := m.nodeHeartbeats[nodeID]
	if !exists {
		return false
	}
	return time.Since(heartbeat) < m.offlineThreshold
}

// ListOnlineNodes 列出所有在线节点
func (m *Manager) ListOnlineNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0)
	now := time.Now()
	for nodeID, heartbeat := range m.nodeHeartbeats {
		if now.Sub(heartbeat) < m.offlineThreshold {
			result = append(result, nodeID)
		}
	}
	return result
}

// ListOfflineNodes 列出所有离线节点
func (m *Manager) ListOfflineNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0)
	now := time.Now()
	for nodeID, heartbeat := range m.nodeHeartbeats {
		if now.Sub(heartbeat) >= m.offlineThreshold {
			result = append(result, nodeID)
		}
	}
	return result
}

// checkHeartbeatsLoop 定期检查心跳
func (m *Manager) checkHeartbeatsLoop() {
	for {
		select {
		case <-m.checkTimer.C:
			// 检查心跳
			m.checkHeartbeats()

			// 重置定时器
			m.checkTimer.Reset(m.heartbeatInterval)
		case <-m.stopCh:
			return
		}
	}
}

// checkHeartbeats 检查心跳
func (m *Manager) checkHeartbeats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	offlineNodes := make([]string, 0)

	// 检查所有节点的心跳
	for nodeID, heartbeat := range m.nodeHeartbeats {
		if now.Sub(heartbeat) >= m.offlineThreshold {
			offlineNodes = append(offlineNodes, nodeID)
		}
	}

	// 通知处理函数
	for _, nodeID := range offlineNodes {
		nodeType := m.nodeTypes[nodeID]
		for _, handler := range m.handlers {
			go handler(nodeID, nodeType, nil)
		}
	}
}

// RemoveNode 移除节点
func (m *Manager) RemoveNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodeHeartbeats, nodeID)
	delete(m.nodeTypes, nodeID)
	delete(m.nodeStats, nodeID)
}

// GetHeartbeatInterval 获取心跳间隔
func (m *Manager) GetHeartbeatInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.heartbeatInterval
}

// GetOfflineThreshold 获取离线阈值
func (m *Manager) GetOfflineThreshold() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.offlineThreshold
}
