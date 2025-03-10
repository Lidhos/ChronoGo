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

func TestClearCommand(t *testing.T) {
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
	testDBName := "clear_cmd_test_db"
	testCollName := "clear_cmd_test_collection"

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
