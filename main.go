package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/model"
	"ChronoGo/pkg/protocol"
	"ChronoGo/pkg/query"
	"ChronoGo/pkg/storage"
)

var (
	dataDir   = flag.String("data-dir", "./data", "数据存储目录")
	httpAddr  = flag.String("http-addr", ":8080", "HTTP服务地址")
	mongoAddr = flag.String("mongo-addr", ":27017", "MongoDB协议服务地址")
)

func main() {
	// 解析命令行参数
	flag.Parse()

	// 创建数据目录
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// 初始化存储引擎
	storageEngine, err := storage.NewStorageEngine(*dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}
	defer storageEngine.Close()

	// 初始化查询引擎
	queryEngine := query.NewQueryEngine(storageEngine)

	// 创建命令处理器
	cmdHandler := protocol.NewCommandHandler()
	registerCommands(cmdHandler, storageEngine, queryEngine)

	// 启动MongoDB协议服务
	wireHandler, err := protocol.NewWireProtocolHandler(*mongoAddr, cmdHandler)
	if err != nil {
		log.Fatalf("Failed to create wire protocol handler: %v", err)
	}
	wireHandler.Start()
	defer wireHandler.Close()

	// 打印启动信息
	fmt.Printf("ChronoGo started\n")
	fmt.Printf("MongoDB protocol server listening on %s\n", *mongoAddr)

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")
}

// registerCommands 注册命令处理函数
func registerCommands(handler *protocol.CommandHandler, storageEngine *storage.StorageEngine, queryEngine *query.QueryEngine) {
	// 数据库管理命令
	handler.Register("listDatabases", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// TODO: 实现列出数据库命令
		return bson.D{
			{"ok", 1},
			{"databases", bson.A{}},
		}, nil
	})

	// 创建数据库命令
	handler.Register("create", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 解析命令参数
		var dbName string
		var isTimeseries bool
		var timeField string

		for _, elem := range cmd {
			switch elem.Key {
			case "create":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "timeseries":
				if doc, ok := elem.Value.(bson.D); ok {
					isTimeseries = true
					for _, field := range doc {
						switch field.Key {
						case "timeField":
							if str, ok := field.Value.(string); ok {
								timeField = str
							}
						}
					}
				}
			}
		}

		if dbName == "" {
			return nil, fmt.Errorf("missing database name")
		}

		// 创建数据库
		retentionPolicy := model.RetentionPolicy{
			Duration:  30 * 24 * time.Hour, // 默认30天
			Precision: "ns",
		}

		if err := storageEngine.CreateDatabase(dbName, retentionPolicy); err != nil {
			return nil, err
		}

		// 如果是时序集合，创建表
		if isTimeseries {
			schema := model.Schema{
				TimeField: timeField,
				TagFields: make(map[string]string),
				Fields:    make(map[string]string),
			}

			if err := storageEngine.CreateTable(dbName, dbName, schema, nil); err != nil {
				return nil, err
			}
		}

		return bson.D{{"ok", 1}}, nil
	})

	// 插入命令
	handler.Register("insert", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName string
		var documents bson.A

		for _, elem := range cmd {
			switch elem.Key {
			case "insert":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "documents":
				if docs, ok := elem.Value.(bson.A); ok {
					documents = docs
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			}
		}

		if dbName == "" || tableName == "" || len(documents) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 插入文档
		for _, doc := range documents {
			if bsonDoc, ok := doc.(bson.D); ok {
				point, err := model.FromBSON(bsonDoc)
				if err != nil {
					return nil, err
				}

				if err := storageEngine.InsertPoint(dbName, tableName, point); err != nil {
					return nil, err
				}
			}
		}

		return bson.D{{"ok", 1}, {"n", len(documents)}}, nil
	})

	// 查询命令
	handler.Register("find", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName string
		var filter bson.D

		for _, elem := range cmd {
			switch elem.Key {
			case "find":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			}
		}

		if dbName == "" || tableName == "" {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 解析查询
		parser := query.NewQueryParser()
		q, err := parser.ParseQuery(dbName, tableName, filter)
		if err != nil {
			return nil, err
		}

		// 执行查询
		result, err := queryEngine.Execute(ctx, q)
		if err != nil {
			return nil, err
		}

		// 转换结果为BSON
		docs := make(bson.A, 0, len(result.Points))
		for _, point := range result.Points {
			docs = append(docs, point.ToBSON())
		}

		return bson.D{
			{"ok", 1},
			{"cursor", bson.D{
				{"firstBatch", docs},
				{"id", int64(0)},
				{"ns", dbName + "." + tableName},
			}},
		}, nil
	})

	// 聚合命令
	handler.Register("aggregate", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// TODO: 实现聚合命令
		return bson.D{{"ok", 1}}, nil
	})
}
