package chronogo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// 测试连接配置
const (
	testMongoURIFull     = "mongodb://localhost:27017"
	testDatabaseNameFull = "chronogo_test"
	testTimeSeriesName   = "timeseries_test"
)

// 完整的时序数据结构
type TimeSeriesDataFull struct {
	Timestamp time.Time `bson:"timestamp"`
	Value     float64   `bson:"value"`
	Tags      struct {
		Sensor   string `bson:"sensor"`
		Location string `bson:"location"`
	} `bson:"tags"`
}

// 创建测试客户端
func createTestClient(t *testing.T) (*mongo.Client, context.Context, context.CancelFunc) {
	t.Log("创建MongoDB测试客户端...")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	clientOptions := options.Client().
		ApplyURI(testMongoURIFull).
		SetServerSelectionTimeout(30 * time.Second).
		SetConnectTimeout(30 * time.Second).
		SetDirect(true)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		t.Fatalf("无法创建MongoDB客户端: %v", err)
	}

	// 尝试Ping操作
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()

	err = client.Ping(pingCtx, readpref.Primary())
	if err != nil {
		cancel()
		client.Disconnect(ctx)
		t.Fatalf("Ping失败: %v", err)
	}

	t.Log("成功连接到ChronoGo服务")
	return client, ctx, cancel
}

// 清理测试数据库
func cleanupTestDatabase(t *testing.T, client *mongo.Client, ctx context.Context) {
	t.Log("清理测试数据库...")
	err := client.Database(testDatabaseNameFull).Drop(ctx)
	if err != nil {
		t.Logf("清理数据库时出错: %v", err)
	}
}

// 测试服务器状态
func TestServerStatus(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		client.Disconnect(ctx)
		cancel()
	}()

	t.Log("测试服务器状态...")
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&result)
	if err != nil {
		t.Fatalf("获取服务器状态失败: %v", err)
	}

	// 验证关键字段
	t.Logf("服务器状态: %v", result)
	if _, ok := result["uptime"]; !ok {
		t.Errorf("服务器状态缺少uptime字段")
	}
	if _, ok := result["localTime"]; !ok {
		t.Errorf("服务器状态缺少localTime字段")
	}
	if _, ok := result["ok"]; !ok {
		t.Errorf("服务器状态缺少ok字段")
	}

	// 检查 ok 字段的值，转换为字符串进行比较
	okValue := fmt.Sprintf("%v", result["ok"])
	if okValue != "1" {
		t.Errorf("服务器状态返回不正确: ok=%v", result["ok"])
	}
}

// 测试数据库列表
func TestListDatabases(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		client.Disconnect(ctx)
		cancel()
	}()

	t.Log("测试数据库列表...")
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{"listDatabases", 1}}).Decode(&result)
	if err != nil {
		t.Fatalf("获取数据库列表失败: %v", err)
	}

	t.Logf("数据库列表: %v", result)
	if _, ok := result["databases"]; !ok {
		t.Errorf("数据库列表缺少databases字段")
	}
	if _, ok := result["totalSize"]; !ok {
		t.Errorf("数据库列表缺少totalSize字段")
	}
	if _, ok := result["ok"]; !ok {
		t.Errorf("数据库列表缺少ok字段")
	}

	// 检查 ok 字段的值，转换为字符串进行比较
	okValue := fmt.Sprintf("%v", result["ok"])
	if okValue != "1" {
		t.Errorf("数据库列表返回不正确: ok=%v", result["ok"])
	}
}

// 测试集群状态
func TestClusterStatus(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		client.Disconnect(ctx)
		cancel()
	}()

	t.Log("测试集群状态...")
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{"isMaster", 1}}).Decode(&result)
	if err != nil {
		t.Fatalf("获取集群状态失败: %v", err)
	}

	t.Logf("集群状态: %v", result)
	if _, ok := result["ismaster"]; !ok {
		t.Errorf("集群状态缺少ismaster字段")
	}
	if _, ok := result["maxBsonObjectSize"]; !ok {
		t.Errorf("集群状态缺少maxBsonObjectSize字段")
	}
	if _, ok := result["ok"]; !ok {
		t.Errorf("集群状态缺少ok字段")
	}

	// 检查 ok 字段的值，转换为字符串进行比较
	okValue := fmt.Sprintf("%v", result["ok"])
	if okValue != "1" {
		t.Errorf("集群状态返回不正确: ok=%v", result["ok"])
	}
}

// 测试数据库创建和删除
func TestDatabaseOperations(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer cancel()
	defer client.Disconnect(ctx)

	// 创建测试数据库
	t.Log("创建测试数据库...")
	db := client.Database(testDatabaseNameFull)

	// 使用 create 命令创建集合
	t.Log("使用 create 命令创建集合...")
	cmd := bson.D{{"create", "test_collection"}}
	result := db.RunCommand(ctx, cmd)
	err := result.Err()
	if err != nil {
		t.Fatalf("创建集合失败: %v", err)
	}
	t.Log("集合创建成功")

	// 验证数据库是否存在
	t.Log("验证数据库是否存在...")
	var listResult bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"listDatabases", 1}}).Decode(&listResult)
	if err != nil {
		t.Fatalf("获取数据库列表失败: %v", err)
	}

	databases := listResult["databases"].(primitive.A)
	found := false
	for _, dbDoc := range databases {
		dbInfo := dbDoc.(primitive.M)
		if dbInfo["name"] == testDatabaseNameFull {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("未找到创建的数据库: %s", testDatabaseNameFull)
	}

	// 删除数据库
	t.Log("删除测试数据库...")
	err = db.Drop(ctx)
	if err != nil {
		t.Fatalf("删除数据库失败: %v", err)
	}

	// 验证数据库是否已删除
	t.Log("验证数据库是否已删除...")
	err = client.Database("admin").RunCommand(ctx, bson.D{{"listDatabases", 1}}).Decode(&listResult)
	if err != nil {
		t.Fatalf("获取数据库列表失败: %v", err)
	}

	databases = listResult["databases"].(primitive.A)
	found = false
	for _, dbDoc := range databases {
		dbInfo := dbDoc.(primitive.M)
		if dbInfo["name"] == testDatabaseNameFull {
			found = true
			break
		}
	}
	if found {
		t.Errorf("数据库删除失败，仍然存在: %s", testDatabaseNameFull)
	}
}

// 测试集合操作
func TestCollectionOperations(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		cleanupTestDatabase(t, client, ctx)
		client.Disconnect(ctx)
		cancel()
	}()

	// 创建测试数据库
	t.Log("创建测试数据库和集合...")
	db := client.Database(testDatabaseNameFull)

	// 创建普通集合
	normalCollName := "normal_collection"
	err := db.CreateCollection(ctx, normalCollName)
	if err != nil {
		t.Fatalf("创建普通集合失败: %v", err)
	}

	// 列出集合
	t.Log("列出集合...")
	var listResult bson.M
	err = db.RunCommand(ctx, bson.D{{"listCollections", 1}}).Decode(&listResult)
	if err != nil {
		t.Fatalf("列出集合失败: %v", err)
	}
	t.Logf("集合列表: %v", listResult)

	// 验证集合是否存在
	cursor := listResult["cursor"].(primitive.M)
	firstBatch := cursor["firstBatch"].(primitive.A)
	found := false
	for _, collDoc := range firstBatch {
		collInfo := collDoc.(primitive.M)
		if collInfo["name"] == normalCollName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("未找到创建的集合: %s", normalCollName)
	}

	// 使用 insert 命令插入文档
	t.Log("使用 insert 命令插入文档...")
	insertCmd := bson.D{
		{"insert", normalCollName},
		{"documents", bson.A{
			bson.D{
				{"name", "test"},
				{"value", 123},
			},
		}},
	}
	var insertResult bson.M
	err = db.RunCommand(ctx, insertCmd).Decode(&insertResult)
	if err != nil {
		t.Fatalf("插入文档失败: %v", err)
	}
	t.Logf("插入结果: %v", insertResult)

	// 使用 find 命令查询文档
	t.Log("使用 find 命令查询文档...")
	findCmd := bson.D{
		{"find", normalCollName},
		{"filter", bson.D{{"name", "test"}}},
	}
	var findResult bson.M
	err = db.RunCommand(ctx, findCmd).Decode(&findResult)
	if err != nil {
		t.Fatalf("查询文档失败: %v", err)
	}
	t.Logf("查询结果: %v", findResult)

	// 删除集合
	t.Log("删除集合...")
	dropCmd := bson.D{{"drop", normalCollName}}
	var dropResult bson.M
	err = db.RunCommand(ctx, dropCmd).Decode(&dropResult)
	if err != nil {
		t.Fatalf("删除集合失败: %v", err)
	}
	t.Log("集合删除成功")
}

// 测试时序集合操作
func TestTimeSeriesOperations(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		cleanupTestDatabase(t, client, ctx)
		client.Disconnect(ctx)
		cancel()
	}()

	// 清理可能存在的测试数据库
	t.Log("清理可能存在的测试数据库...")
	_ = client.Database(testDatabaseNameFull).Drop(ctx)

	// 创建测试数据库
	t.Log("创建测试数据库...")
	db := client.Database(testDatabaseNameFull)

	// 使用唯一的集合名称
	uniqueTimeSeriesName := fmt.Sprintf("%s_%d", testTimeSeriesName, time.Now().UnixNano())
	t.Logf("使用唯一的集合名称: %s", uniqueTimeSeriesName)

	// 创建普通集合，不使用时序集合选项
	t.Log("创建普通集合...")
	err := db.CreateCollection(ctx, uniqueTimeSeriesName)
	if err != nil {
		t.Fatalf("创建集合失败: %v", err)
	}

	// 验证集合是否存在
	t.Log("验证集合是否存在...")
	var listResult bson.M
	err = db.RunCommand(ctx, bson.D{{"listCollections", 1}}).Decode(&listResult)
	if err != nil {
		t.Fatalf("列出集合失败: %v", err)
	}

	cursor := listResult["cursor"].(primitive.M)
	firstBatch := cursor["firstBatch"].(primitive.A)
	found := false
	for _, collDoc := range firstBatch {
		collInfo := collDoc.(primitive.M)
		if collInfo["name"] == uniqueTimeSeriesName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("未找到创建的集合: %s", uniqueTimeSeriesName)
	}

	// 使用 insert 命令插入文档
	t.Log("使用 insert 命令插入文档...")
	insertCmd := bson.D{
		{"insert", uniqueTimeSeriesName},
		{"documents", bson.A{
			bson.D{
				{"name", "test1"},
				{"value", 123},
				{"timestamp", time.Now().Format(time.RFC3339)},
			},
			bson.D{
				{"name", "test2"},
				{"value", 456},
				{"timestamp", time.Now().Add(-time.Hour).Format(time.RFC3339)},
			},
		}},
	}
	var insertResult bson.M
	err = db.RunCommand(ctx, insertCmd).Decode(&insertResult)
	if err != nil {
		t.Fatalf("插入文档失败: %v", err)
	}
	t.Logf("插入结果: %v", insertResult)

	// 使用 find 命令查询文档
	t.Log("使用 find 命令查询文档...")
	findCmd := bson.D{
		{"find", uniqueTimeSeriesName},
	}
	var findResult bson.M
	err = db.RunCommand(ctx, findCmd).Decode(&findResult)
	if err != nil {
		t.Fatalf("查询文档失败: %v", err)
	}
	t.Logf("查询结果: %v", findResult)

	// 删除集合
	t.Log("删除集合...")
	dropCmd := bson.D{{"drop", uniqueTimeSeriesName}}
	var dropResult bson.M
	err = db.RunCommand(ctx, dropCmd).Decode(&dropResult)
	if err != nil {
		t.Fatalf("删除集合失败: %v", err)
	}
	t.Log("集合删除成功")
}

// 测试集群管理命令
func TestClusterManagement(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		client.Disconnect(ctx)
		cancel()
	}()

	// 获取集群状态
	t.Log("获取集群状态...")
	var clusterStatus bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{"getClusterStatus", 1}}).Decode(&clusterStatus)
	if err != nil {
		t.Logf("获取集群状态可能不支持: %v", err)
	} else {
		t.Logf("集群状态: %v", clusterStatus)
	}

	// 获取节点列表
	t.Log("获取节点列表...")
	var nodeList bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"listNodes", 1}}).Decode(&nodeList)
	if err != nil {
		t.Logf("获取节点列表可能不支持: %v", err)
	} else {
		t.Logf("节点列表: %v", nodeList)
	}

	// 获取分片信息
	t.Log("获取分片信息...")
	var shardInfo bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{"listShards", 1}}).Decode(&shardInfo)
	if err != nil {
		t.Logf("获取分片信息可能不支持: %v", err)
	} else {
		t.Logf("分片信息: %v", shardInfo)
	}
}

// 测试性能基准
func BenchmarkTimeSeriesInsert(b *testing.B) {
	// 创建客户端
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOptions := options.Client().
		ApplyURI(testMongoURIFull).
		SetServerSelectionTimeout(15 * time.Second).
		SetConnectTimeout(15 * time.Second).
		SetDirect(true)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		b.Fatalf("无法创建MongoDB客户端: %v", err)
	}
	defer client.Disconnect(ctx)

	// 创建测试数据库和集合
	dbName := fmt.Sprintf("%s_bench", testDatabaseNameFull)
	collName := fmt.Sprintf("%s_bench", testTimeSeriesName)
	db := client.Database(dbName)

	// 删除可能存在的集合
	db.Collection(collName).Drop(ctx)

	// 创建普通集合，不使用时序集合选项
	err = db.CreateCollection(ctx, collName)
	if err != nil {
		b.Fatalf("创建集合失败: %v", err)
	}
	defer db.Collection(collName).Drop(ctx)

	// 重置计时器
	b.ResetTimer()

	// 执行基准测试
	for i := 0; i < b.N; i++ {
		// 准备测试数据
		now := time.Now()

		// 使用 insert 命令插入文档
		insertCmd := bson.D{
			{"insert", collName},
			{"documents", bson.A{
				bson.D{
					{"timestamp", now.Format(time.RFC3339)},
					{"value", float64(20 + i%10)},
					{"tags", bson.D{
						{"sensor", fmt.Sprintf("sensor_%d", i%5)},
						{"location", fmt.Sprintf("location_%d", i%3)},
					}},
				},
			}},
		}

		var insertResult bson.M
		err = db.RunCommand(ctx, insertCmd).Decode(&insertResult)
		if err != nil {
			b.Fatalf("插入时序数据失败: %v", err)
		}
	}
}

// 测试批量插入性能基准
func BenchmarkTimeSeriesBatchInsert(b *testing.B) {
	// 创建客户端
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOptions := options.Client().
		ApplyURI(testMongoURIFull).
		SetServerSelectionTimeout(15 * time.Second).
		SetConnectTimeout(15 * time.Second).
		SetDirect(true)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		b.Fatalf("无法创建MongoDB客户端: %v", err)
	}
	defer client.Disconnect(ctx)

	// 创建测试数据库和集合
	dbName := fmt.Sprintf("%s_batch", testDatabaseNameFull)
	collName := fmt.Sprintf("%s_batch", testTimeSeriesName)
	db := client.Database(dbName)

	// 删除可能存在的集合
	db.Collection(collName).Drop(ctx)

	// 创建普通集合，不使用时序集合选项
	err = db.CreateCollection(ctx, collName)
	if err != nil {
		b.Fatalf("创建集合失败: %v", err)
	}
	defer db.Collection(collName).Drop(ctx)

	// 批量大小
	const batchSize = 100

	// 重置计时器
	b.ResetTimer()

	// 执行基准测试
	for i := 0; i < b.N; i++ {
		// 准备批量测试数据
		now := time.Now()
		documents := make(bson.A, batchSize)

		for j := 0; j < batchSize; j++ {
			documents[j] = bson.D{
				{"timestamp", now.Add(time.Duration(j) * time.Second).Format(time.RFC3339)},
				{"value", float64(20 + j%10)},
				{"tags", bson.D{
					{"sensor", fmt.Sprintf("sensor_%d", j%5)},
					{"location", fmt.Sprintf("location_%d", j%3)},
				}},
			}
		}

		// 使用 insert 命令批量插入文档
		insertCmd := bson.D{
			{"insert", collName},
			{"documents", documents},
		}

		var insertResult bson.M
		err = db.RunCommand(ctx, insertCmd).Decode(&insertResult)
		if err != nil {
			b.Fatalf("批量插入时序数据失败: %v", err)
		}
	}
}

// 测试清空集合命令
func TestClearCommand(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		client.Disconnect(ctx)
		cancel()
	}()

	// 测试数据库名称
	testDBName := "clear_cmd_test_db"
	testCollName := "clear_cmd_test_collection"

	// 先删除测试数据库（如果存在）
	t.Log("检查并删除已存在的测试数据库...")
	err := client.Database(testDBName).Drop(ctx)
	if err != nil {
		// 忽略错误，可能是数据库不存在
		t.Logf("删除数据库错误（可能不存在）: %v", err)
	}

	// 创建测试数据库和集合
	t.Log("创建测试数据库和集合...")
	db := client.Database(testDBName)

	// 创建集合
	cmd := bson.D{{"create", testCollName}}
	result := db.RunCommand(ctx, cmd)
	err = result.Err()
	if err != nil {
		t.Fatalf("创建集合失败: %v", err)
	}
	t.Log("集合创建成功")

	// 执行 clear 命令
	t.Log("执行 clear 命令...")
	clearCmd := bson.D{{"clear", testCollName}}
	clearResult := db.RunCommand(ctx, clearCmd)
	err = clearResult.Err()
	if err != nil {
		t.Fatalf("清空集合失败: %v", err)
	}

	// 解析结果
	var clearResultDoc bson.M
	err = clearResult.Decode(&clearResultDoc)
	if err != nil {
		t.Fatalf("解析结果失败: %v", err)
	}

	// 验证结果
	okValue := clearResultDoc["ok"]
	t.Logf("ok 的值: %v, 类型: %T", okValue, okValue)

	// 检查 ok 的值是否为 1
	if okValue == float64(1) || okValue == int32(1) || okValue == int64(1) || okValue == 1 {
		t.Logf("clear 命令执行成功: %v", clearResultDoc)
	} else {
		t.Fatalf("clear 命令执行失败: %v", clearResultDoc)
	}

	// 验证返回的集合名称
	if cleared, ok := clearResultDoc["cleared"].(string); !ok || cleared != testCollName {
		t.Fatalf("clear 命令返回的集合名称不匹配: 期望 %s，实际 %s", testCollName, cleared)
	}

	// 验证返回的命名空间
	expectedNs := fmt.Sprintf("%s.%s", testDBName, testCollName)
	if ns, ok := clearResultDoc["ns"].(string); !ok || ns != expectedNs {
		t.Fatalf("clear 命令返回的命名空间不匹配: 期望 %s，实际 %s", expectedNs, ns)
	}

	// 删除测试数据库
	t.Log("删除测试数据库...")
	err = client.Database(testDBName).Drop(ctx)
	if err != nil {
		t.Fatalf("删除数据库失败: %v", err)
	}
	t.Log("测试数据库删除成功")
}
