package chronogo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestClearCollection(t *testing.T) {
	// 创建MongoDB客户端
	t.Log("创建MongoDB测试客户端...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Fatalf("连接MongoDB失败: %v", err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			t.Fatalf("断开连接失败: %v", err)
		}
	}()

	// 检查连接
	err = client.Ping(ctx, nil)
	if err != nil {
		t.Fatalf("Ping失败: %v", err)
	}
	t.Log("成功连接到ChronoGo服务")

	// 测试数据库名称
	testDBName := "clear_test_db"
	testCollName := "clear_test_collection"

	// 先删除测试数据库（如果存在）
	t.Log("检查并删除已存在的测试数据库...")
	err = client.Database(testDBName).Drop(ctx)
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

	// 插入一些测试数据
	t.Log("插入测试数据...")
	coll := db.Collection(testCollName)

	// 使用 insertOne 命令插入数据
	for i := 1; i <= 3; i++ {
		doc := bson.D{{"name", fmt.Sprintf("测试%d", i)}, {"value", i}}
		_, err := coll.InsertOne(ctx, doc)
		if err != nil {
			t.Fatalf("插入数据失败: %v", err)
		}
	}
	t.Log("数据插入成功")

	// 验证数据已插入
	t.Log("验证数据已插入...")
	count, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("统计文档数量失败: %v", err)
	}
	if count != 3 {
		t.Fatalf("预期文档数量为3，实际为%d", count)
	}
	t.Logf("集合中有 %d 个文档", count)

	// 执行删除操作
	t.Log("执行删除操作...")
	deleteResult, err := coll.DeleteMany(ctx, bson.D{})
	if err != nil {
		t.Fatalf("删除文档失败: %v", err)
	}
	t.Logf("删除了 %d 个文档", deleteResult.DeletedCount)

	// 验证集合已清空
	t.Log("验证集合已清空...")
	countAfterClear, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("统计文档数量失败: %v", err)
	}
	if countAfterClear != 0 {
		t.Fatalf("预期文档数量为0，实际为%d", countAfterClear)
	}
	t.Logf("集合中有 %d 个文档", countAfterClear)

	// 删除测试数据库
	t.Log("删除测试数据库...")
	err = client.Database(testDBName).Drop(ctx)
	if err != nil {
		t.Fatalf("删除数据库失败: %v", err)
	}
	t.Log("测试数据库删除成功")
}
