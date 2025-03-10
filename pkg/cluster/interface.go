package cluster

import (
	"context"
	"time"
)

// ClusterManager 是集群管理器接口
type ClusterManager interface {
	// Start 启动集群管理器
	Start() error
	// Stop 停止集群管理器
	Stop() error
	// GetClusterInfo 获取集群信息
	GetClusterInfo() (*ClusterInfo, error)
	// AddDNode 添加物理节点
	AddDNode(info *DNodeInfo) error
	// RemoveDNode 移除物理节点
	RemoveDNode(nodeID string) error
	// GetDNodeInfo 获取物理节点信息
	GetDNodeInfo(nodeID string) (*DNodeInfo, error)
	// ListDNodes 列出所有物理节点
	ListDNodes() ([]*DNodeInfo, error)
	// GetMNodeInfo 获取虚拟管理节点信息
	GetMNodeInfo(nodeID string) (*MNodeInfo, error)
	// ListMNodes 列出所有虚拟管理节点
	ListMNodes() ([]*MNodeInfo, error)
	// GetVNodeInfo 获取虚拟数据节点信息
	GetVNodeInfo(nodeID string) (*VNodeInfo, error)
	// ListVNodes 列出所有虚拟数据节点
	ListVNodes() ([]*VNodeInfo, error)
	// GetVGroupInfo 获取虚拟数据节点组信息
	GetVGroupInfo(groupID string) (*VGroupInfo, error)
	// ListVGroups 列出所有虚拟数据节点组
	ListVGroups() ([]*VGroupInfo, error)
	// GetDatabaseClusterInfo 获取数据库在集群中的信息
	GetDatabaseClusterInfo(dbName string) (*DatabaseClusterInfo, error)
	// ListDatabaseClusterInfo 列出所有数据库在集群中的信息
	ListDatabaseClusterInfo() ([]*DatabaseClusterInfo, error)
	// GetTableClusterInfo 获取表在集群中的信息
	GetTableClusterInfo(dbName, tableName string) (*TableClusterInfo, error)
	// ListTableClusterInfo 列出数据库中所有表在集群中的信息
	ListTableClusterInfo(dbName string) ([]*TableClusterInfo, error)
	// GetNodeStats 获取节点统计信息
	GetNodeStats(nodeID string) (*NodeStats, error)
	// ListNodeStats 列出所有节点的统计信息
	ListNodeStats() ([]*NodeStats, error)
}

// DNodeManager 是物理节点管理器接口
type DNodeManager interface {
	// Start 启动物理节点管理器
	Start() error
	// Stop 停止物理节点管理器
	Stop() error
	// GetInfo 获取物理节点信息
	GetInfo() (*DNodeInfo, error)
	// UpdateInfo 更新物理节点信息
	UpdateInfo(info *DNodeInfo) error
	// CreateVNode 创建虚拟数据节点
	CreateVNode(dbName string, vgroupID string) (*VNodeInfo, error)
	// RemoveVNode 移除虚拟数据节点
	RemoveVNode(vnodeID string) error
	// GetVNodeInfo 获取虚拟数据节点信息
	GetVNodeInfo(vnodeID string) (*VNodeInfo, error)
	// ListVNodes 列出所有虚拟数据节点
	ListVNodes() ([]*VNodeInfo, error)
	// CreateMNode 创建虚拟管理节点
	CreateMNode() (*MNodeInfo, error)
	// RemoveMNode 移除虚拟管理节点
	RemoveMNode() error
	// GetMNodeInfo 获取虚拟管理节点信息
	GetMNodeInfo() (*MNodeInfo, error)
	// CollectStats 收集节点统计信息
	CollectStats() (*NodeStats, error)
}

// VNodeManager 是虚拟数据节点管理器接口
type VNodeManager interface {
	// Start 启动虚拟数据节点管理器
	Start() error
	// Stop 停止虚拟数据节点管理器
	Stop() error
	// GetInfo 获取虚拟数据节点信息
	GetInfo() (*VNodeInfo, error)
	// UpdateInfo 更新虚拟数据节点信息
	UpdateInfo(info *VNodeInfo) error
	// CreateTable 在虚拟数据节点上创建表
	CreateTable(tableName string) error
	// DropTable 在虚拟数据节点上删除表
	DropTable(tableName string) error
	// ListTables 列出虚拟数据节点上的所有表
	ListTables() ([]string, error)
	// SyncFromMaster 从主节点同步数据
	SyncFromMaster(masterVNodeID string) error
	// SyncToSlave 同步数据到从节点
	SyncToSlave(slaveVNodeID string) error
	// Backup 备份数据
	Backup(backupPath string) error
	// Restore 恢复数据
	Restore(backupPath string) error
}

// MNodeManager 是虚拟管理节点管理器接口
type MNodeManager interface {
	// Start 启动虚拟管理节点管理器
	Start() error
	// Stop 停止虚拟管理节点管理器
	Stop() error
	// GetInfo 获取虚拟管理节点信息
	GetInfo() (*MNodeInfo, error)
	// UpdateInfo 更新虚拟管理节点信息
	UpdateInfo(info *MNodeInfo) error
	// IsMaster 判断是否是主管理节点
	IsMaster() bool
	// ElectMaster 选举主管理节点
	ElectMaster() error
	// SyncFromMaster 从主管理节点同步元数据
	SyncFromMaster(masterMNodeID string) error
	// SyncToSlave 同步元数据到从管理节点
	SyncToSlave(slaveMNodeID string) error
}

// VGroupManager 是虚拟数据节点组管理器接口
type VGroupManager interface {
	// CreateVGroup 创建虚拟数据节点组
	CreateVGroup(dbName string, replicas int) (*VGroupInfo, error)
	// DropVGroup 删除虚拟数据节点组
	DropVGroup(vgroupID string) error
	// GetVGroupInfo 获取虚拟数据节点组信息
	GetVGroupInfo(vgroupID string) (*VGroupInfo, error)
	// ListVGroups 列出所有虚拟数据节点组
	ListVGroups() ([]*VGroupInfo, error)
	// AddVNodeToVGroup 向虚拟数据节点组添加虚拟数据节点
	AddVNodeToVGroup(vgroupID, vnodeID string) error
	// RemoveVNodeFromVGroup 从虚拟数据节点组移除虚拟数据节点
	RemoveVNodeFromVGroup(vgroupID, vnodeID string) error
	// ElectMasterVNode 选举主虚拟数据节点
	ElectMasterVNode(vgroupID string) error
	// GetMasterVNode 获取主虚拟数据节点
	GetMasterVNode(vgroupID string) (string, error)
}

// HeartbeatManager 是心跳管理器接口
type HeartbeatManager interface {
	// Start 启动心跳管理器
	Start() error
	// Stop 停止心跳管理器
	Stop() error
	// SendHeartbeat 发送心跳
	SendHeartbeat(nodeID string, nodeType NodeType) error
	// SetHeartbeatInterval 设置心跳间隔
	SetHeartbeatInterval(interval time.Duration)
	// SetOfflineThreshold 设置离线阈值
	SetOfflineThreshold(threshold time.Duration)
	// RegisterHeartbeatHandler 注册心跳处理函数
	RegisterHeartbeatHandler(handler func(nodeID string, nodeType NodeType, stats *NodeStats))
}

// LoadBalancer 是负载均衡器接口
type LoadBalancer interface {
	// Start 启动负载均衡器
	Start() error
	// Stop 停止负载均衡器
	Stop() error
	// Balance 执行负载均衡
	Balance() error
	// SetBalanceInterval 设置负载均衡间隔
	SetBalanceInterval(interval time.Duration)
	// SetBalanceThreshold 设置负载均衡阈值
	SetBalanceThreshold(threshold float64)
	// IsBalancing 判断是否正在进行负载均衡
	IsBalancing() bool
}

// MetaDataManager 是元数据管理器接口
type MetaDataManager interface {
	// Start 启动元数据管理器
	Start() error
	// Stop 停止元数据管理器
	Stop() error
	// GetMetaData 获取元数据
	GetMetaData(ctx context.Context, key string) ([]byte, error)
	// SetMetaData 设置元数据
	SetMetaData(ctx context.Context, key string, value []byte) error
	// DeleteMetaData 删除元数据
	DeleteMetaData(ctx context.Context, key string) error
	// ListMetaData 列出元数据
	ListMetaData(ctx context.Context, prefix string) (map[string][]byte, error)
	// WatchMetaData 监视元数据变化
	WatchMetaData(ctx context.Context, prefix string, callback func(key string, value []byte, deleted bool))
	// Backup 备份元数据
	Backup(ctx context.Context, backupPath string) error
	// Restore 恢复元数据
	Restore(ctx context.Context, backupPath string) error
}

// ClusterConfig 是集群配置
type ClusterConfig struct {
	// ClusterID 是集群的唯一标识
	ClusterID string
	// PublicIP 是节点对外服务的IP地址
	PublicIP string
	// PrivateIP 是节点内部通信的IP地址
	PrivateIP string
	// InternalIP 是节点在云环境中的内部IP地址
	InternalIP string
	// PublicPort 是节点对外服务的端口
	PublicPort int
	// PrivatePort 是节点内部通信的端口
	PrivatePort int
	// MasterIP 是主节点的IP地址
	MasterIP string
	// SecondIP 是备用主节点的IP地址
	SecondIP string
	// DataDir 是数据存储目录
	DataDir string
	// LogDir 是日志存储目录
	LogDir string
	// HeartbeatInterval 是心跳间隔
	HeartbeatInterval time.Duration
	// OfflineThreshold 是离线阈值
	OfflineThreshold time.Duration
	// BalanceInterval 是负载均衡间隔
	BalanceInterval time.Duration
	// BalanceThreshold 是负载均衡阈值
	BalanceThreshold float64
	// SessionsPerVNode 是每个虚拟数据节点的会话数
	SessionsPerVNode int
	// MaxVNodesPerDNode 是每个物理节点的最大虚拟数据节点数
	MaxVNodesPerDNode int
	// MaxMNodesPerCluster 是集群中的最大虚拟管理节点数
	MaxMNodesPerCluster int
	// DefaultReplicas 是默认副本数
	DefaultReplicas int
}
