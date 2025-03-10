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

	// 插入文档
	t.Log("插入文档...")
	coll := db.Collection(normalCollName)
	_, err = coll.InsertOne(ctx, bson.M{"name": "test", "value": 123})
	if err != nil {
		t.Fatalf("插入文档失败: %v", err)
	}

	// 查询文档
	t.Log("查询文档...")
	var doc bson.M
	err = coll.FindOne(ctx, bson.M{"name": "test"}).Decode(&doc)
	if err != nil {
		t.Fatalf("查询文档失败: %v", err)
	}
	t.Logf("查询结果: %v", doc)
	if doc["value"] != int32(123) {
		t.Errorf("文档内容不正确: %v", doc)
	}

	// 更新文档
	t.Log("更新文档...")
	_, err = coll.UpdateOne(
		ctx,
		bson.M{"name": "test"},
		bson.M{"$set": bson.M{"value": 456}},
	)
	if err != nil {
		t.Fatalf("更新文档失败: %v", err)
	}

	// 验证更新
	err = coll.FindOne(ctx, bson.M{"name": "test"}).Decode(&doc)
	if err != nil {
		t.Fatalf("查询更新后的文档失败: %v", err)
	}
	if doc["value"] != int32(456) {
		t.Errorf("文档更新不正确: %v", doc)
	}

	// 删除文档
	t.Log("删除文档...")
	_, err = coll.DeleteOne(ctx, bson.M{"name": "test"})
	if err != nil {
		t.Fatalf("删除文档失败: %v", err)
	}

	// 验证删除
	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("计数文档失败: %v", err)
	}
	if count != 0 {
		t.Errorf("文档删除不正确，仍有 %d 个文档", count)
	}

	// 删除集合
	t.Log("删除集合...")
	err = coll.Drop(ctx)
	if err != nil {
		t.Fatalf("删除集合失败: %v", err)
	}
}

// 测试时序集合操作
func TestTimeSeriesOperations(t *testing.T) {
	client, ctx, cancel := createTestClient(t)
	defer func() {
		cleanupTestDatabase(t, client, ctx)
		client.Disconnect(ctx)
		cancel()
	}()

	// 创建测试数据库
	t.Log("创建测试数据库...")
	db := client.Database(testDatabaseNameFull)

	// 创建时序集合
	t.Log("创建时序集合...")
	timeField := "timestamp"
	metaField := "tags"
	granularity := "seconds"
	expireSeconds := int64(60 * 60 * 24 * 30) // 30天过期

	timeSeriesOptions := options.CreateCollectionOptions{
		TimeSeriesOptions: options.TimeSeries().
			SetTimeField(timeField).
			SetMetaField(metaField).
			SetGranularity(granularity),
		ExpireAfterSeconds: &expireSeconds,
	}

	err := db.CreateCollection(ctx, testTimeSeriesName, &timeSeriesOptions)
	if err != nil {
		t.Fatalf("创建时序集合失败: %v", err)
	}

	// 验证时序集合是否存在
	t.Log("验证时序集合是否存在...")
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
		if collInfo["name"] == testTimeSeriesName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("未找到创建的时序集合: %s", testTimeSeriesName)
	}

	// 插入时序数据
	t.Log("插入时序数据...")
	tsColl := db.Collection(testTimeSeriesName)

	// 准备测试数据
	now := time.Now()
	testData := []interface{}{
		TimeSeriesDataFull{
			Timestamp: now.Add(-time.Hour * 2),
			Value:     23.5,
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   "temp_1",
				Location: "room_a",
			},
		},
		TimeSeriesDataFull{
			Timestamp: now.Add(-time.Hour),
			Value:     24.2,
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   "temp_1",
				Location: "room_a",
			},
		},
		TimeSeriesDataFull{
			Timestamp: now,
			Value:     25.1,
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   "temp_1",
				Location: "room_a",
			},
		},
		TimeSeriesDataFull{
			Timestamp: now.Add(-time.Hour * 2),
			Value:     21.5,
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   "temp_2",
				Location: "room_b",
			},
		},
		TimeSeriesDataFull{
			Timestamp: now.Add(-time.Hour),
			Value:     22.2,
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   "temp_2",
				Location: "room_b",
			},
		},
		TimeSeriesDataFull{
			Timestamp: now,
			Value:     22.8,
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   "temp_2",
				Location: "room_b",
			},
		},
	}

	_, err = tsColl.InsertMany(ctx, testData)
	if err != nil {
		t.Fatalf("插入时序数据失败: %v", err)
	}

	// 查询时序数据
	t.Log("查询时序数据...")
	var results []TimeSeriesDataFull
	cursor1, err := tsColl.Find(ctx, bson.M{
		"tags.sensor": "temp_1",
		"timestamp": bson.M{
			"$gte": now.Add(-time.Hour * 3),
			"$lte": now.Add(time.Hour),
		},
	})
	if err != nil {
		t.Fatalf("查询时序数据失败: %v", err)
	}
	defer cursor1.Close(ctx)

	if err = cursor1.All(ctx, &results); err != nil {
		t.Fatalf("解析时序数据失败: %v", err)
	}
	t.Logf("查询到 %d 条时序数据", len(results))
	if len(results) != 3 {
		t.Errorf("查询结果数量不正确，期望3，实际%d", len(results))
	}

	// 测试时序聚合查询
	t.Log("测试时序聚合查询...")
	pipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"tags.sensor": "temp_1",
			"timestamp": bson.M{
				"$gte": now.Add(-time.Hour * 3),
				"$lte": now.Add(time.Hour),
			},
		}}},
		{{"$group", bson.M{
			"_id":      nil,
			"avgValue": bson.M{"$avg": "$value"},
			"maxValue": bson.M{"$max": "$value"},
			"minValue": bson.M{"$min": "$value"},
		}}},
	}

	aggCursor, err := tsColl.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("执行时序聚合查询失败: %v", err)
	}
	defer aggCursor.Close(ctx)

	var aggResults []bson.M
	if err = aggCursor.All(ctx, &aggResults); err != nil {
		t.Fatalf("解析聚合结果失败: %v", err)
	}

	if len(aggResults) != 1 {
		t.Fatalf("聚合结果数量不正确，期望1，实际%d", len(aggResults))
	}

	t.Logf("聚合结果: %v", aggResults[0])
	if _, ok := aggResults[0]["avgValue"]; !ok {
		t.Errorf("聚合结果缺少avgValue字段")
	}
	if _, ok := aggResults[0]["maxValue"]; !ok {
		t.Errorf("聚合结果缺少maxValue字段")
	}
	if _, ok := aggResults[0]["minValue"]; !ok {
		t.Errorf("聚合结果缺少minValue字段")
	}

	// 测试时间窗口聚合（如果支持）
	t.Log("测试时间窗口聚合...")
	timeWindowPipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"timestamp": bson.M{
				"$gte": now.Add(-time.Hour * 3),
				"$lte": now.Add(time.Hour),
			},
		}}},
		{{"$timeWindow", bson.M{
			"timestamp": "$timestamp",
			"window":    bson.M{"$hour": 1},
			"output": bson.M{
				"avgValue": bson.M{"$avg": "$value"},
				"count":    bson.M{"$sum": 1},
			},
		}}},
		{{"$sort", bson.M{"_id.timestamp": 1}}},
	}

	timeWindowCursor, err := tsColl.Aggregate(ctx, timeWindowPipeline)
	if err != nil {
		t.Logf("时间窗口聚合可能不支持: %v", err)
	} else {
		defer timeWindowCursor.Close(ctx)

		var timeWindowResults []bson.M
		if err = timeWindowCursor.All(ctx, &timeWindowResults); err != nil {
			t.Logf("解析时间窗口结果失败: %v", err)
		} else {
			t.Logf("时间窗口聚合结果: %v", timeWindowResults)
		}
	}

	// 测试移动窗口聚合（如果支持）
	t.Log("测试移动窗口聚合...")
	movingWindowPipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"timestamp": bson.M{
				"$gte": now.Add(-time.Hour * 3),
				"$lte": now.Add(time.Hour),
			},
		}}},
		{{"$movingWindow", bson.M{
			"timestamp": "$timestamp",
			"window":    bson.M{"$hour": 2},
			"step":      bson.M{"$minute": 30},
			"output": bson.M{
				"avgValue": bson.M{"$avg": "$value"},
				"count":    bson.M{"$sum": 1},
			},
		}}},
		{{"$sort", bson.M{"_id.timestamp": 1}}},
	}

	movingWindowCursor, err := tsColl.Aggregate(ctx, movingWindowPipeline)
	if err != nil {
		t.Logf("移动窗口聚合可能不支持: %v", err)
	} else {
		defer movingWindowCursor.Close(ctx)

		var movingWindowResults []bson.M
		if err = movingWindowCursor.All(ctx, &movingWindowResults); err != nil {
			t.Logf("解析移动窗口结果失败: %v", err)
		} else {
			t.Logf("移动窗口聚合结果: %v", movingWindowResults)
		}
	}

	// 删除时序集合
	t.Log("删除时序集合...")
	err = tsColl.Drop(ctx)
	if err != nil {
		t.Fatalf("删除时序集合失败: %v", err)
	}
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
	db := client.Database(fmt.Sprintf("%s_bench", testDatabaseNameFull))
	collName := fmt.Sprintf("%s_bench", testTimeSeriesName)

	// 确保集合存在
	timeField := "timestamp"
	metaField := "tags"
	granularity := "seconds"

	timeSeriesOptions := options.CreateCollectionOptions{
		TimeSeriesOptions: options.TimeSeries().
			SetTimeField(timeField).
			SetMetaField(metaField).
			SetGranularity(granularity),
	}

	// 删除可能存在的集合
	db.Collection(collName).Drop(ctx)

	err = db.CreateCollection(ctx, collName, &timeSeriesOptions)
	if err != nil {
		b.Fatalf("创建时序集合失败: %v", err)
	}
	defer db.Collection(collName).Drop(ctx)

	coll := db.Collection(collName)

	// 重置计时器
	b.ResetTimer()

	// 执行基准测试
	for i := 0; i < b.N; i++ {
		// 准备测试数据
		now := time.Now()
		data := TimeSeriesDataFull{
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Value:     float64(20 + i%10),
			Tags: struct {
				Sensor   string `bson:"sensor"`
				Location string `bson:"location"`
			}{
				Sensor:   fmt.Sprintf("sensor_%d", i%5),
				Location: fmt.Sprintf("location_%d", i%3),
			},
		}

		_, err := coll.InsertOne(ctx, data)
		if err != nil {
			b.Fatalf("插入时序数据失败: %v", err)
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
