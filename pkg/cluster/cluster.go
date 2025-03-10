package cluster

import (
	"ChronoGo/pkg/cluster/common"
	"ChronoGo/pkg/cluster/manager"
)

// NewClusterManager 创建一个新的集群管理器
func NewClusterManager(config *common.ClusterConfig) (common.ClusterManager, error) {
	return manager.NewManager(config)
}

// NewClusterConfig 创建一个新的集群配置
func NewClusterConfig() *common.ClusterConfig {
	return &common.ClusterConfig{
		HeartbeatInterval:   common.DefaultHeartbeatInterval,
		OfflineThreshold:    common.DefaultOfflineThreshold,
		BalanceInterval:     common.DefaultBalanceInterval,
		BalanceThreshold:    common.DefaultBalanceThreshold,
		SessionsPerVNode:    common.DefaultSessionsPerVNode,
		MaxVNodesPerDNode:   common.DefaultMaxVNodesPerDNode,
		MaxMNodesPerCluster: common.DefaultMaxMNodesPerCluster,
		DefaultReplicas:     common.DefaultReplicas,
	}
}
