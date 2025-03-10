package chronogo

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// 测试连接配置
const (
	// 使用正确的端口27017
	testMongoURI       = "mongodb://localhost:27017"
	testDatabaseName   = "test_chronogo"
	testCollectionName = "test_timeseries"
)

// 测试数据结构
type TimeSeriesData struct {
	Timestamp time.Time `bson:"timestamp"`
	Value     float64   `bson:"value"`
	Tags      struct {
		Sensor   string `bson:"sensor"`
		Location string `bson:"location"`
	} `bson:"tags"`
}

func TestMongoDBConnection(t *testing.T) {
	// 创建MongoDB客户端，添加更多诊断信息
	t.Log("开始测试MongoDB连接...")
	t.Logf("连接URI: %s", testMongoURI)

	// 使用更长的超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 配置客户端选项
	clientOptions := options.Client().
		ApplyURI(testMongoURI).
		SetServerSelectionTimeout(15 * time.Second).
		SetConnectTimeout(15 * time.Second).
		SetDirect(true) // 使用直接连接模式，避免拓扑发现问题

	t.Log("正在连接到MongoDB服务器...")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		t.Fatalf("无法创建MongoDB客户端: %v", err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			t.Logf("断开连接时出错: %v", err)
		}
	}()

	// 尝试简单的ping操作
	t.Log("尝试Ping操作...")
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	err = client.Ping(pingCtx, readpref.Primary())
	if err != nil {
		t.Fatalf("Ping失败: %v", err)
	}

	t.Log("成功连接到ChronoGo服务")

	// 尝试简单的命令，检查服务器信息
	t.Log("尝试获取服务器信息...")
	var result bson.M
	cmdErr := client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&result)
	if cmdErr != nil {
		t.Logf("获取服务器信息失败: %v", cmdErr)
	} else {
		t.Logf("服务器信息: %v", result)
	}

	// 测试isMaster命令
	t.Log("测试isMaster命令...")
	var isMasterResult bson.M
	isMasterErr := client.Database("admin").RunCommand(ctx, bson.D{{"isMaster", 1}}).Decode(&isMasterResult)
	if isMasterErr != nil {
		t.Logf("isMaster命令失败: %v", isMasterErr)
	} else {
		t.Logf("isMaster结果: %v", isMasterResult)
	}

	// 测试buildInfo命令
	t.Log("测试buildInfo命令...")
	var buildInfoResult bson.M
	buildInfoErr := client.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}).Decode(&buildInfoResult)
	if buildInfoErr != nil {
		t.Logf("buildInfo命令失败: %v", buildInfoErr)
	} else {
		t.Logf("buildInfo结果: %v", buildInfoResult)
	}

	t.Log("基本连接测试成功完成")
}
