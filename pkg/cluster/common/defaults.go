package common

import "time"

// 默认配置值
const (
	// DefaultHeartbeatInterval 默认心跳间隔
	DefaultHeartbeatInterval = 10 * time.Second
	// DefaultOfflineThreshold 默认离线阈值
	DefaultOfflineThreshold = 30 * time.Second
	// DefaultBalanceInterval 默认负载均衡间隔
	DefaultBalanceInterval = 5 * time.Minute
	// DefaultBalanceThreshold 默认负载均衡阈值
	DefaultBalanceThreshold = 0.2
	// DefaultSessionsPerVNode 默认每个虚拟数据节点的会话数
	DefaultSessionsPerVNode = 2000
	// DefaultMaxVNodesPerDNode 默认每个物理节点的最大虚拟数据节点数
	DefaultMaxVNodesPerDNode = 64
	// DefaultMaxMNodesPerCluster 默认集群中的最大虚拟管理节点数
	DefaultMaxMNodesPerCluster = 5
	// DefaultReplicas 默认副本数
	DefaultReplicas = 1
)
