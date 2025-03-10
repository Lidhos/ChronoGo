package cluster

import (
	"net"
	"time"
)

// NodeStatus 表示节点的状态
type NodeStatus int

const (
	// NodeStatusOffline 表示节点离线
	NodeStatusOffline NodeStatus = iota
	// NodeStatusOnline 表示节点在线
	NodeStatusOnline
	// NodeStatusStarting 表示节点正在启动
	NodeStatusStarting
	// NodeStatusStopping 表示节点正在停止
	NodeStatusStopping
)

// NodeRole 表示节点在集群中的角色
type NodeRole int

const (
	// NodeRoleUnknown 表示未知角色
	NodeRoleUnknown NodeRole = iota
	// NodeRoleMaster 表示主节点
	NodeRoleMaster
	// NodeRoleSlave 表示从节点
	NodeRoleSlave
)

// NodeType 表示节点类型
type NodeType int

const (
	// NodeTypeDNode 表示物理节点
	NodeTypeDNode NodeType = iota
	// NodeTypeVNode 表示虚拟数据节点
	NodeTypeVNode
	// NodeTypeMNode 表示虚拟管理节点
	NodeTypeMNode
)

// NodeInfo 表示节点的基本信息
type NodeInfo struct {
	// NodeID 是节点的唯一标识
	NodeID string
	// NodeType 是节点类型
	NodeType NodeType
	// Status 是节点状态
	Status NodeStatus
	// Role 是节点角色
	Role NodeRole
	// PublicIP 是节点对外服务的IP地址
	PublicIP net.IP
	// PrivateIP 是节点内部通信的IP地址
	PrivateIP net.IP
	// InternalIP 是节点在云环境中的内部IP地址
	InternalIP net.IP
	// PublicPort 是节点对外服务的端口
	PublicPort int
	// PrivatePort 是节点内部通信的端口
	PrivatePort int
	// StartTime 是节点启动时间
	StartTime time.Time
	// LastHeartbeat 是最后一次心跳时间
	LastHeartbeat time.Time
}

// DNodeInfo 表示物理节点的信息
type DNodeInfo struct {
	// 继承基本节点信息
	NodeInfo
	// CPU 使用率
	CPUUsage float64
	// 内存使用率
	MemoryUsage float64
	// 磁盘使用率
	DiskUsage float64
	// 网络带宽使用率
	NetworkUsage float64
	// VNodes 是该物理节点上的虚拟数据节点列表
	VNodes []string
	// MNode 是该物理节点上的虚拟管理节点（如果有）
	MNode string
}

// VNodeInfo 表示虚拟数据节点的信息
type VNodeInfo struct {
	// 继承基本节点信息
	NodeInfo
	// DNodeID 是所属物理节点的ID
	DNodeID string
	// VGroupID 是所属虚拟数据节点组的ID
	VGroupID string
	// DatabaseName 是该虚拟节点所属的数据库名
	DatabaseName string
	// TableCount 是该虚拟节点包含的表数量
	TableCount int
	// DataSize 是该虚拟节点存储的数据大小（字节）
	DataSize int64
}

// VGroupInfo 表示虚拟数据节点组的信息
type VGroupInfo struct {
	// VGroupID 是虚拟数据节点组的唯一标识
	VGroupID string
	// DatabaseName 是该虚拟节点组所属的数据库名
	DatabaseName string
	// Replicas 是副本数
	Replicas int
	// VNodes 是该虚拟节点组包含的虚拟数据节点列表
	VNodes []string
	// MasterVNode 是主虚拟数据节点
	MasterVNode string
}

// MNodeInfo 表示虚拟管理节点的信息
type MNodeInfo struct {
	// 继承基本节点信息
	NodeInfo
	// DNodeID 是所属物理节点的ID
	DNodeID string
	// IsMaster 表示是否是主管理节点
	IsMaster bool
}

// ClusterInfo 表示集群的整体信息
type ClusterInfo struct {
	// ClusterID 是集群的唯一标识
	ClusterID string
	// DNodes 是集群中的物理节点列表
	DNodes []string
	// MNodes 是集群中的虚拟管理节点列表
	MNodes []string
	// VGroups 是集群中的虚拟数据节点组列表
	VGroups []string
	// Databases 是集群中的数据库列表
	Databases []string
	// CreateTime 是集群创建时间
	CreateTime time.Time
	// Status 是集群状态
	Status NodeStatus
}

// DatabaseClusterInfo 表示数据库在集群中的信息
type DatabaseClusterInfo struct {
	// DatabaseName 是数据库名
	DatabaseName string
	// Replicas 是副本数
	Replicas int
	// VGroups 是该数据库使用的虚拟数据节点组列表
	VGroups []string
	// TableCount 是该数据库包含的表数量
	TableCount int
	// CreateTime 是数据库创建时间
	CreateTime time.Time
}

// TableClusterInfo 表示表在集群中的信息
type TableClusterInfo struct {
	// TableName 是表名
	TableName string
	// DatabaseName 是所属数据库名
	DatabaseName string
	// VGroupID 是所属虚拟数据节点组的ID
	VGroupID string
	// CreateTime 是表创建时间
	CreateTime time.Time
}

// NodeStats 表示节点的统计信息
type NodeStats struct {
	// NodeID 是节点的唯一标识
	NodeID string
	// CPUUsage 是CPU使用率
	CPUUsage float64
	// MemoryUsage 是内存使用率
	MemoryUsage float64
	// DiskUsage 是磁盘使用率
	DiskUsage float64
	// NetworkUsage 是网络带宽使用率
	NetworkUsage float64
	// QPS 是每秒查询数
	QPS float64
	// TPS 是每秒事务数
	TPS float64
	// Connections 是当前连接数
	Connections int
	// CollectTime 是统计信息收集时间
	CollectTime time.Time
}
