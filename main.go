package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"ChronoGo/pkg/cluster"
	"ChronoGo/pkg/cluster/common"
	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/model"
	"ChronoGo/pkg/protocol"
	"ChronoGo/pkg/query"
	"ChronoGo/pkg/storage"
	_ "net/http/pprof" // 导入 pprof
)

var (
	dataDir       = flag.String("data-dir", "./data", "数据存储目录")
	httpAddr      = flag.String("http-addr", ":8080", "HTTP服务地址")
	mongoAddr     = flag.String("mongo-addr", ":27017", "MongoDB协议服务地址")
	clusterID     = flag.String("cluster-id", "", "集群ID，为空时自动生成")
	publicIP      = flag.String("public-ip", "", "节点对外服务的IP地址")
	privateIP     = flag.String("private-ip", "", "节点内部通信的IP地址")
	masterIP      = flag.String("master-ip", "", "主节点的IP地址，为空时表示自己是主节点")
	secondIP      = flag.String("second-ip", "", "备用主节点的IP地址")
	enableCluster = flag.Bool("enable-cluster", false, "是否启用集群模式")
	enableLog     = flag.Bool("enable-log", false, "是否启用日志记录")
	logDir        = flag.String("log-dir", "./logs", "日志存储目录")
)

func main() {
	// 解析命令行参数
	flag.Parse()

	// 初始化日志系统
	if err := logger.Init(*enableLog, *logDir); err != nil {
		log.Fatalf("初始化日志系统失败: %v", err)
	}
	defer logger.Close()

	// 记录启动信息
	log.Printf("系统启动，日志记录状态: %v", *enableLog)

	// 创建数据目录
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}

	// 初始化存储引擎
	storageEngine, err := storage.NewStorageEngine(*dataDir)
	if err != nil {
		log.Fatalf("初始化存储引擎失败: %v", err)
	}
	defer storageEngine.Close()

	// 初始化查询引擎
	queryEngine := query.NewQueryEngine(storageEngine)

	// 创建命令处理器
	commandHandler := protocol.NewCommandHandler()

	// 创建协议处理器
	wireHandler, err := protocol.NewWireProtocolHandler(*mongoAddr, commandHandler)
	if err != nil {
		log.Fatalf("创建协议处理器失败: %v", err)
	}

	// 设置存储引擎和查询引擎
	wireHandler.SetStorageEngine(storageEngine)
	wireHandler.SetQueryEngine(queryEngine)

	// 注册命令
	registerCommands(commandHandler, storageEngine, queryEngine)

	// 启动协议处理器
	go wireHandler.Start()
	defer wireHandler.Close()

	// 如果启用集群模式，初始化集群
	var clusterManager common.ClusterManager
	if *enableCluster {
		// 创建集群配置
		clusterConfig := cluster.NewClusterConfig()
		clusterConfig.ClusterID = *clusterID
		clusterConfig.DataDir = *dataDir
		clusterConfig.PublicIP = *publicIP
		clusterConfig.PrivateIP = *privateIP
		clusterConfig.MasterIP = *masterIP
		clusterConfig.SecondIP = *secondIP
		clusterConfig.PublicPort = 27017  // MongoDB协议端口
		clusterConfig.PrivatePort = 27018 // 集群内部通信端口

		// 创建集群管理器
		clusterManager, err = cluster.NewClusterManager(clusterConfig)
		if err != nil {
			log.Fatalf("创建集群管理器失败: %v", err)
		}

		// 启动集群管理器
		if err := clusterManager.Start(); err != nil {
			log.Fatalf("启动集群管理器失败: %v", err)
		}
		defer clusterManager.Stop()

		// 注册集群命令
		registerClusterCommands(commandHandler, clusterManager)
	}

	// 启动 pprof HTTP 服务器
	go func() {
		logger.Printf("启动 pprof 服务器，监听端口 :6060")
		err := http.ListenAndServe(":6060", nil)
		if err != nil {
			logger.Printf("pprof 服务器启动失败: %v", err)
		}
	}()

	// 打印启动信息
	log.Printf("ChronoGo started")
	log.Printf("MongoDB protocol server listening on %s", *mongoAddr)
	if *enableCluster {
		log.Printf("Cluster mode enabled")
	}

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 保存元数据
	log.Printf("ChronoGo shutting down")
}

// registerCommands 注册命令处理函数
func registerCommands(handler *protocol.CommandHandler, storageEngine *storage.StorageEngine, queryEngine *query.QueryEngine) {
	// 添加ping命令支持
	handler.Register("ping", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{{"ok", 1}}, nil
	})

	// 添加isMaster命令支持
	handler.Register("isMaster", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"ismaster", true},
			{"maxBsonObjectSize", 16777216},
			{"maxMessageSizeBytes", 48000000},
			{"maxWriteBatchSize", 100000},
			{"localTime", time.Now()},
			{"maxWireVersion", 13},
			{"minWireVersion", 0},
			{"readOnly", false},
			{"ok", 1},
		}, nil
	})

	// 添加hello命令支持（MongoDB 5.0+中isMaster的替代命令）
	handler.Register("hello", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"ismaster", true},
			{"maxBsonObjectSize", 16777216},
			{"maxMessageSizeBytes", 48000000},
			{"maxWriteBatchSize", 100000},
			{"localTime", time.Now()},
			{"maxWireVersion", 13},
			{"minWireVersion", 0},
			{"readOnly", false},
			{"ok", 1},
		}, nil
	})

	// 添加buildInfo命令支持
	handler.Register("buildInfo", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"version", "5.0.0"},
			{"gitVersion", "ChronoGo-1.0.0"},
			{"modules", bson.A{}},
			{"sysInfo", "Go version go1.20 linux/amd64"},
			{"versionArray", bson.A{1, 0, 0, 0}},
			{"bits", 64},
			{"debug", false},
			{"maxBsonObjectSize", 16777216},
			{"ok", 1},
		}, nil
	})

	// 数据库管理命令
	handler.Register("listDatabases", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 获取所有数据库
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			return nil, err
		}

		// 构建数据库列表
		dbList := make(bson.A, 0, len(databases))
		var totalSize int64

		for _, db := range databases {
			// 获取数据库大小
			dbSize, err := storageEngine.GetDatabaseSize(db.Name)
			if err != nil {
				// 如果获取大小失败，记录错误但继续处理
				log.Printf("获取数据库 %s 大小失败: %v", db.Name, err)
				dbSize = 0
			}

			totalSize += dbSize

			dbList = append(dbList, bson.D{
				{"name", db.Name},
				{"sizeOnDisk", dbSize}, // 实现数据库大小统计
				{"empty", len(db.Tables) == 0},
			})
		}

		// 计算总大小（MB）
		totalSizeMb := float64(totalSize) / (1024 * 1024)

		return bson.D{
			{"ok", 1},
			{"databases", dbList},
			{"totalSize", totalSize},     // 实现总大小统计
			{"totalSizeMb", totalSizeMb}, // 实现总大小统计(MB)
		}, nil
	})

	// 添加use命令支持
	handler.Register("use", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		logger.Printf("use命令: 开始处理")

		// 解析命令参数
		var dbName string
		for _, elem := range cmd {
			if elem.Key == "use" && elem.Value != nil {
				if str, ok := elem.Value.(string); ok {
					dbName = str
					logger.Printf("use命令: 数据库名称: %s", dbName)
				}
			}
		}

		if dbName == "" {
			logger.Printf("use命令: 缺少数据库名称")
			return nil, fmt.Errorf("missing database name")
		}

		// 设置当前数据库
		session, ok := ctx.Value("session").(*protocol.Session)
		if !ok {
			logger.Printf("use命令: 无法获取会话")
			return nil, fmt.Errorf("session not found")
		}

		// 检查数据库是否存在
		exists := false
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			logger.Printf("use命令: 获取数据库列表失败: %v", err)
		} else {
			for _, db := range databases {
				if db.Name == dbName {
					exists = true
					break
				}
			}
		}

		// 即使数据库不存在，也允许切换（MongoDB的行为）
		if !exists {
			logger.Printf("use命令: 数据库 %s 不存在，但仍然切换", dbName)
		}

		// 设置当前数据库
		session.CurrentDB = dbName
		logger.Printf("use命令: 切换到数据库 %s", dbName)

		return bson.D{{"ok", 1}}, nil
	})

	// 创建数据库命令
	handler.Register("create", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		logger.Printf("create命令: 开始处理")

		// 解析命令参数
		var dbName string
		var collectionName string
		var isTimeseries bool
		var timeField string

		// 获取会话信息
		session, ok := ctx.Value("session").(*protocol.Session)
		if ok && session.CurrentDB != "" {
			logger.Printf("create命令: 当前会话数据库: %s", session.CurrentDB)
		} else {
			logger.Printf("create命令: 当前会话没有选择数据库")
		}

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "create":
				if str, ok := elem.Value.(string); ok {
					collectionName = str
					logger.Printf("create命令: 集合名称: %s", collectionName)
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
					logger.Printf("create命令: 命令中指定的数据库: %s", dbName)
				}
			case "timeseries":
				if doc, ok := elem.Value.(bson.D); ok {
					isTimeseries = true
					logger.Printf("create命令: 是时序集合")
					for _, field := range doc {
						switch field.Key {
						case "timeField":
							if str, ok := field.Value.(string); ok {
								timeField = str
								logger.Printf("create命令: 时间字段: %s", timeField)
							}
						}
					}
				}
			}
		}

		// 如果命令中没有指定数据库，使用会话数据库
		if dbName == "" {
			if session != nil && session.CurrentDB != "" {
				dbName = session.CurrentDB
				logger.Printf("create命令: 使用会话数据库: %s", dbName)
			} else {
				// 如果没有当前数据库，使用集合名称作为数据库名称
				dbName = collectionName
				logger.Printf("create命令: 没有当前数据库，使用集合名称作为数据库名称: %s", dbName)
			}
		}

		if dbName == "" {
			logger.Printf("create命令: 缺少数据库名称")
			return nil, fmt.Errorf("missing database name")
		}

		// 检查数据库是否存在
		var existingDB *model.Database
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			logger.Printf("create命令: 获取数据库列表失败: %v", err)
			return nil, err
		}

		dbExists := false
		for _, db := range databases {
			if db.Name == dbName {
				dbExists = true
				existingDB = db
				break
			}
		}

		// 如果数据库不存在，创建它
		if !dbExists {
			logger.Printf("create命令: 数据库 %s 不存在，开始创建", dbName)
			retentionPolicy := model.RetentionPolicy{
				Duration:  30 * 24 * time.Hour, // 默认30天
				Precision: "1s",                // 默认1秒
			}

			if err := storageEngine.CreateDatabase(dbName, retentionPolicy); err != nil {
				logger.Printf("create命令: 创建数据库失败: %v", err)
				return nil, err
			}
			logger.Printf("create命令: 创建数据库成功: %s", dbName)
		} else {
			logger.Printf("create命令: 数据库 %s 已存在", dbName)

			// 检查表是否已存在
			if existingDB != nil {
				if _, tableExists := existingDB.Tables[collectionName]; tableExists {
					logger.Printf("create命令: 表 %s 已存在于数据库 %s 中", collectionName, dbName)
					return nil, fmt.Errorf("table %s already exists in database %s", collectionName, dbName)
				}
			}
		}

		// 创建集合（表）
		logger.Printf("create命令: 开始创建集合: %s", collectionName)
		schema := model.Schema{
			TagFields: make(map[string]string),
			Fields:    make(map[string]string),
		}

		// 如果是时序集合，设置时间字段
		if isTimeseries && timeField != "" {
			logger.Printf("create命令: 设置时间字段: %s", timeField)
			schema.TimeField = timeField
		}

		if err := storageEngine.CreateTable(dbName, collectionName, schema, nil); err != nil {
			logger.Printf("create命令: 创建集合失败: %v", err)
			return nil, err
		}
		logger.Printf("create命令: 创建集合成功: %s", collectionName)

		logger.Printf("create命令: 处理完成")
		return bson.D{{"ok", 1}}, nil
	})

	// 删除数据库命令
	handler.Register("dropDatabase", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName string

		for _, elem := range cmd {
			switch elem.Key {
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			}
		}

		if dbName == "" {
			return nil, fmt.Errorf("missing database name")
		}

		// 删除数据库
		if err := storageEngine.DropDatabase(dbName); err != nil {
			return nil, err
		}

		return bson.D{
			{"ok", 1},
			{"dropped", dbName},
		}, nil
	})

	// 删除集合命令
	handler.Register("drop", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string

		for _, elem := range cmd {
			switch elem.Key {
			case "drop":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			}
		}

		if dbName == "" || collName == "" {
			return nil, fmt.Errorf("missing database or collection name")
		}

		// 删除集合
		if err := storageEngine.DropTable(dbName, collName); err != nil {
			return nil, err
		}

		return bson.D{
			{"ok", 1},
			{"dropped", collName},
		}, nil
	})

	// 插入文档命令
	handler.Register("insert", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		logger.Printf("insert命令: 开始处理")

		var dbName, collName string
		var documents []interface{}

		for _, elem := range cmd {
			switch elem.Key {
			case "insert":
				if str, ok := elem.Value.(string); ok {
					collName = str
					logger.Printf("insert命令: 集合名称: %s", collName)
				}
			case "documents":
				if docs, ok := elem.Value.(bson.A); ok {
					documents = make([]interface{}, len(docs))
					for i, doc := range docs {
						documents[i] = doc
					}
					logger.Printf("insert命令: 文档数量: %d", len(documents))
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
					logger.Printf("insert命令: 数据库名称: %s", dbName)
				}
			}
		}

		// 如果没有指定数据库，使用当前会话的数据库
		if dbName == "" {
			session, ok := ctx.Value("session").(*protocol.Session)
			if ok && session.CurrentDB != "" {
				dbName = session.CurrentDB
				logger.Printf("insert命令: 使用会话数据库: %s", dbName)
			}
		}

		if dbName == "" {
			logger.Printf("insert命令: 缺少数据库名称")
			return nil, fmt.Errorf("missing database name")
		}

		if collName == "" {
			logger.Printf("insert命令: 缺少集合名称")
			return nil, fmt.Errorf("missing collection name")
		}

		if len(documents) == 0 {
			logger.Printf("insert命令: 没有要插入的文档")
			return nil, fmt.Errorf("no documents to insert")
		}

		// 检查数据库和集合是否存在
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			logger.Printf("insert命令: 获取数据库列表失败: %v", err)
			return nil, err
		}

		dbExists := false
		var db *model.Database
		for _, d := range databases {
			if d.Name == dbName {
				dbExists = true
				db = d
				break
			}
		}

		if !dbExists {
			logger.Printf("insert命令: 数据库 %s 不存在", dbName)
			return nil, fmt.Errorf("database %s does not exist", dbName)
		}

		tableExists := false
		if db != nil {
			_, tableExists = db.Tables[collName]
		}

		if !tableExists {
			logger.Printf("insert命令: 集合 %s 在数据库 %s 中不存在", collName, dbName)
			return nil, fmt.Errorf("collection %s does not exist in database %s", collName, dbName)
		}

		// 插入文档
		logger.Printf("insert命令: 开始插入文档到 %s.%s", dbName, collName)

		// 将文档转换为时序数据点
		for _, doc := range documents {
			if bsonDoc, ok := doc.(bson.D); ok {
				// 创建时序数据点
				point := &model.TimeSeriesPoint{
					Timestamp: time.Now().UnixNano(),
					Tags:      make(map[string]string),
					Fields:    make(map[string]interface{}),
				}

				// 解析文档字段
				for _, field := range bsonDoc {
					// 假设所有字段都是普通字段
					point.Fields[field.Key] = field.Value
				}

				// 插入数据点
				if err := storageEngine.InsertPoint(dbName, collName, point); err != nil {
					logger.Printf("insert命令: 插入文档失败: %v", err)
					return nil, err
				}
			}
		}

		logger.Printf("insert命令: 文档插入成功")

		return bson.D{
			{"ok", 1},
			{"n", len(documents)},
		}, nil
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

	// 计数命令
	handler.Register("count", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName string
		var filter bson.D

		for _, elem := range cmd {
			switch elem.Key {
			case "count":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "query":
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

		return bson.D{
			{"ok", 1},
			{"n", len(result.Points)},
		}, nil
	})

	// 聚合命令
	handler.Register("aggregate", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName string
		var pipeline bson.A

		for _, elem := range cmd {
			switch elem.Key {
			case "aggregate":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "pipeline":
				if p, ok := elem.Value.(bson.A); ok {
					pipeline = p
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			}
		}

		if dbName == "" || tableName == "" || len(pipeline) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 解析聚合管道
		// 目前只支持简单的$match和$group操作
		var matchStage bson.D
		var groupStage bson.D

		for _, stage := range pipeline {
			if stageDoc, ok := stage.(bson.D); ok {
				for _, elem := range stageDoc {
					switch elem.Key {
					case "$match":
						if match, ok := elem.Value.(bson.D); ok {
							matchStage = match
						}
					case "$group":
						if group, ok := elem.Value.(bson.D); ok {
							groupStage = group
						}
					}
				}
			}
		}

		// 先执行match查询
		parser := query.NewQueryParser()
		q, err := parser.ParseQuery(dbName, tableName, matchStage)
		if err != nil {
			return nil, err
		}

		// 如果有group阶段，添加聚合操作
		if len(groupStage) > 0 {
			// 解析group by字段
			var groupByFields []string
			var aggregations []query.Aggregation

			for _, elem := range groupStage {
				if elem.Key == "_id" {
					// 解析group by
					if groupBy, ok := elem.Value.(bson.D); ok {
						for _, field := range groupBy {
							if field.Key != "" && field.Value != nil {
								if fieldPath, ok := field.Value.(string); ok && len(fieldPath) > 1 && fieldPath[0] == '$' {
									groupByFields = append(groupByFields, fieldPath[1:])
								}
							}
						}
					}
				} else {
					// 解析聚合函数
					if aggFunc, ok := elem.Value.(bson.D); ok && len(aggFunc) == 1 {
						funcElem := aggFunc[0]
						funcName := funcElem.Key
						if funcName == "$sum" || funcName == "$avg" || funcName == "$min" || funcName == "$max" || funcName == "$count" {
							if fieldPath, ok := funcElem.Value.(string); ok && len(fieldPath) > 1 && fieldPath[0] == '$' {
								aggregations = append(aggregations, query.Aggregation{
									Function: funcName[1:],  // 去掉$前缀
									Field:    fieldPath[1:], // 去掉$前缀
									Alias:    elem.Key,
								})
							}
						}
					}
				}
			}

			q.GroupBy = groupByFields
			q.Aggregations = aggregations
		}

		// 执行聚合查询
		result, err := queryEngine.ExecuteAggregation(ctx, q)
		if err != nil {
			return nil, err
		}

		return bson.D{
			{"ok", 1},
			{"cursor", bson.D{
				{"firstBatch", result},
				{"id", int64(0)},
				{"ns", dbName + "." + tableName},
			}},
		}, nil
	})

	// 列出集合命令
	handler.Register("listCollections", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 解析命令参数
		var dbName string
		var filter bson.D
		var nameOnly bool

		for _, elem := range cmd {
			switch elem.Key {
			case "listCollections":
				if val, ok := elem.Value.(float64); ok && val == 1 {
					// 命令格式正确
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			case "nameOnly":
				if b, ok := elem.Value.(bool); ok {
					nameOnly = b
				}
			}
		}

		// 检查数据库是否存在
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			return nil, err
		}

		var dbExists bool
		var tables []*model.Table
		for _, db := range databases {
			if db.Name == dbName {
				dbExists = true
				// 获取数据库中的表
				tables, err = storageEngine.ListTables(dbName)
				if err != nil {
					return nil, err
				}
				break
			}
		}

		if !dbExists {
			return nil, fmt.Errorf("database %s does not exist", dbName)
		}

		// 构建集合列表
		collections := make(bson.A, 0, len(tables))
		for _, table := range tables {
			// 应用过滤器
			if filter != nil {
				// 简单实现：只支持按名称过滤
				match := true
				for _, elem := range filter {
					if elem.Key == "name" {
						if namePattern, ok := elem.Value.(string); ok && namePattern != table.Name {
							match = false
							break
						}
					}
				}
				if !match {
					continue
				}
			}

			collectionInfo := bson.D{
				{"name", table.Name},
				{"type", "collection"},
			}

			// 如果不是只返回名称，添加更多信息
			if !nameOnly {
				// 获取表大小
				size, err := storageEngine.GetTableSize(dbName, table.Name)
				if err != nil {
					// 如果获取大小失败，记录错误但继续处理
					log.Printf("获取表 %s.%s 大小失败: %v", dbName, table.Name, err)
					size = 0
				}

				// 添加额外信息
				collectionInfo = append(collectionInfo,
					bson.E{"options", bson.D{}},
					bson.E{"info", bson.D{
						{"readOnly", false},
						{"uuid", primitive.NewObjectID().Hex()},
					}},
					bson.E{"idIndex", bson.D{
						{"v", 2},
						{"key", bson.D{{"_id", 1}}},
						{"name", "_id_"},
					}},
					bson.E{"size", size},
				)
			}

			collections = append(collections, collectionInfo)
		}

		// 构建响应
		cursor := bson.D{
			{"id", int64(0)},
			{"ns", dbName + ".$cmd.listCollections"},
			{"firstBatch", collections},
		}

		return bson.D{
			{"cursor", cursor},
			{"ok", 1},
		}, nil
	})

	// 服务器状态命令
	handler.Register("serverStatus", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"ok", 1},
			{"host", "ChronoGo"},
			{"version", "1.0.0"},
			{"process", "ChronoGo"},
			{"pid", os.Getpid()},
			{"uptime", int64(time.Since(time.Now()).Seconds())},
			{"localTime", time.Now()},
		}, nil
	})

	// 降采样命令
	handler.Register("downsample", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName, field, aggregation string
		var timeWindow time.Duration
		var fillPolicy string

		for _, elem := range cmd {
			switch elem.Key {
			case "downsample":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "field":
				if str, ok := elem.Value.(string); ok {
					field = str
				}
			case "timeWindow":
				if str, ok := elem.Value.(string); ok {
					var err error
					timeWindow, err = time.ParseDuration(str)
					if err != nil {
						return nil, fmt.Errorf("invalid time window: %s", str)
					}
				}
			case "aggregation":
				if str, ok := elem.Value.(string); ok {
					aggregation = str
				}
			case "fillPolicy":
				if str, ok := elem.Value.(string); ok {
					fillPolicy = str
				}
			}
		}

		if dbName == "" || tableName == "" || field == "" || timeWindow == 0 || aggregation == "" {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 执行查询获取原始数据
		parser := query.NewQueryParser()
		q, err := parser.ParseQuery(dbName, tableName, bson.D{})
		if err != nil {
			return nil, err
		}

		// 执行查询
		result, err := queryEngine.Execute(ctx, q)
		if err != nil {
			return nil, err
		}

		// 执行降采样
		options := query.DownsampleOptions{
			TimeWindow:  timeWindow,
			Aggregation: aggregation,
			FillPolicy:  fillPolicy,
		}

		downsampledPoints, err := queryEngine.Downsample(result.Points, field, options)
		if err != nil {
			return nil, err
		}

		// 转换结果为BSON
		docs := make(bson.A, 0, len(downsampledPoints))
		for _, point := range downsampledPoints {
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

	// 插值命令
	handler.Register("interpolate", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName, field, method string
		var maxGap, resolution time.Duration
		var filter bson.D

		for _, elem := range cmd {
			switch elem.Key {
			case "interpolate":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "field":
				if str, ok := elem.Value.(string); ok {
					field = str
				}
			case "method":
				if str, ok := elem.Value.(string); ok {
					method = str
				}
			case "maxGap":
				if str, ok := elem.Value.(string); ok {
					var err error
					maxGap, err = time.ParseDuration(str)
					if err != nil {
						return nil, fmt.Errorf("invalid max gap: %s", str)
					}
				}
			case "resolution":
				if str, ok := elem.Value.(string); ok {
					var err error
					resolution, err = time.ParseDuration(str)
					if err != nil {
						return nil, fmt.Errorf("invalid resolution: %s", str)
					}
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			}
		}

		if dbName == "" || tableName == "" || field == "" || method == "" || resolution == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 执行查询获取原始数据
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

		// 执行插值
		options := query.InterpolationOptions{
			Method:     method,
			MaxGap:     maxGap,
			Resolution: resolution,
		}

		interpolatedPoints, err := queryEngine.Interpolate(result.Points, field, options)
		if err != nil {
			return nil, err
		}

		// 转换结果为BSON
		docs := make(bson.A, 0, len(interpolatedPoints))
		for _, point := range interpolatedPoints {
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

	// 移动窗口命令
	handler.Register("movingWindow", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName, field, windowFunction string
		var windowSize, stepSize time.Duration
		var filter bson.D

		for _, elem := range cmd {
			switch elem.Key {
			case "movingWindow":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "field":
				if str, ok := elem.Value.(string); ok {
					field = str
				}
			case "windowSize":
				if str, ok := elem.Value.(string); ok {
					var err error
					windowSize, err = time.ParseDuration(str)
					if err != nil {
						return nil, fmt.Errorf("invalid window size: %s", str)
					}
				}
			case "stepSize":
				if str, ok := elem.Value.(string); ok {
					var err error
					stepSize, err = time.ParseDuration(str)
					if err != nil {
						return nil, fmt.Errorf("invalid step size: %s", str)
					}
				}
			case "function":
				if str, ok := elem.Value.(string); ok {
					windowFunction = str
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			}
		}

		if dbName == "" || tableName == "" || field == "" || windowSize == 0 || windowFunction == "" {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 执行查询获取原始数据
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

		// 执行移动窗口计算
		options := query.MovingWindowOptions{
			WindowSize: windowSize,
			Function:   windowFunction,
			StepSize:   stepSize,
		}

		windowedPoints, err := queryEngine.MovingWindow(result.Points, field, options)
		if err != nil {
			return nil, err
		}

		// 转换结果为BSON
		docs := make(bson.A, 0, len(windowedPoints))
		for _, point := range windowedPoints {
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

	// 时间窗口聚合命令
	handler.Register("timeWindow", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, tableName string
		var windowSize time.Duration
		var aggregations bson.D
		var filter bson.D

		for _, elem := range cmd {
			switch elem.Key {
			case "timeWindow":
				if str, ok := elem.Value.(string); ok {
					tableName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "windowSize":
				if str, ok := elem.Value.(string); ok {
					var err error
					windowSize, err = time.ParseDuration(str)
					if err != nil {
						return nil, fmt.Errorf("invalid window size: %s", str)
					}
				}
			case "aggregations":
				if aggs, ok := elem.Value.(bson.D); ok {
					aggregations = aggs
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			}
		}

		if dbName == "" || tableName == "" || windowSize == 0 || len(aggregations) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 执行查询获取原始数据
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

		// 按时间窗口分组
		windowedPoints := make(map[int64][]model.TimeSeriesPoint)
		var minTime int64 = result.Points[0].Timestamp

		// 找到最小时间戳
		for _, point := range result.Points {
			if point.Timestamp < minTime {
				minTime = point.Timestamp
			}
		}

		// 将点分配到时间窗口
		windowSizeNanos := windowSize.Nanoseconds()
		for _, point := range result.Points {
			// 计算窗口开始时间
			windowStart := (point.Timestamp-minTime)/windowSizeNanos*windowSizeNanos + minTime
			windowedPoints[windowStart] = append(windowedPoints[windowStart], point)
		}

		// 对每个窗口执行聚合
		resultDocs := make(bson.A, 0, len(windowedPoints))
		for windowStart, points := range windowedPoints {
			// 创建结果文档
			resultDoc := bson.D{
				{"timestamp", windowStart},
			}

			// 执行每个聚合
			for _, agg := range aggregations {
				parts := strings.Split(agg.Key, ":")
				if len(parts) != 2 {
					continue
				}

				field := parts[0]
				function := parts[1]

				var aggFunc query.AggregateFunction
				switch function {
				case "avg":
					aggFunc = &query.AvgFunction{}
				case "sum":
					aggFunc = &query.SumFunction{}
				case "min":
					aggFunc = &query.MinFunction{}
				case "max":
					aggFunc = &query.MaxFunction{}
				case "count":
					aggFunc = &query.CountFunction{}
				default:
					return nil, fmt.Errorf("unsupported aggregation function: %s", function)
				}

				// 聚合字段值
				for _, point := range points {
					if fieldValue, exists := point.Fields[field]; exists {
						aggFunc.Add(fieldValue)
					}
				}

				// 添加聚合结果
				resultDoc = append(resultDoc, bson.E{Key: agg.Key, Value: aggFunc.Result()})
			}

			resultDocs = append(resultDocs, resultDoc)
		}

		// 按时间戳排序
		sort.Slice(resultDocs, func(i, j int) bool {
			iDoc := resultDocs[i].(bson.D)
			jDoc := resultDocs[j].(bson.D)

			var iTs, jTs int64
			for _, elem := range iDoc {
				if elem.Key == "timestamp" {
					if ts, ok := elem.Value.(int64); ok {
						iTs = ts
					}
				}
			}

			for _, elem := range jDoc {
				if elem.Key == "timestamp" {
					if ts, ok := elem.Value.(int64); ok {
						jTs = ts
					}
				}
			}

			return iTs < jTs
		})

		return bson.D{
			{"ok", 1},
			{"cursor", bson.D{
				{"firstBatch", resultDocs},
				{"id", int64(0)},
				{"ns", dbName + "." + tableName},
			}},
		}, nil
	})

	// 数据库统计命令
	handler.Register("dbStats", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 解析命令参数
		var dbName string
		var scale float64 = 1 // 默认比例为1

		for _, elem := range cmd {
			switch elem.Key {
			case "dbStats":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok && dbName == "" {
					dbName = str
				}
			case "scale":
				if val, ok := elem.Value.(float64); ok {
					scale = val
				}
			}
		}

		if dbName == "" {
			return nil, fmt.Errorf("missing database name")
		}

		// 检查数据库是否存在
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			return nil, err
		}

		var dbExists bool
		for _, d := range databases {
			if d.Name == dbName {
				dbExists = true
				break
			}
		}

		if !dbExists {
			return nil, fmt.Errorf("database %s does not exist", dbName)
		}

		// 获取数据库中的表
		tables, err := storageEngine.ListTables(dbName)
		if err != nil {
			return nil, err
		}

		// 计算统计信息
		var avgObjSize float64
		var dataSize int64
		var storageSize int64
		var numObjects int64
		var indexes int
		var indexSize int64
		var fileSize int64

		// 计算表统计信息
		for _, table := range tables {
			// 获取表大小
			tableSize, err := storageEngine.GetTableSize(dbName, table.Name)
			if err != nil {
				// 如果获取大小失败，记录错误但继续处理
				log.Printf("获取表 %s.%s 大小失败: %v", dbName, table.Name, err)
				continue
			}

			storageSize += tableSize
			fileSize += tableSize
			dataSize += tableSize

			// 获取表中的索引
			tableIndexes, err := storageEngine.ListIndexes(dbName, table.Name)
			if err != nil {
				// 如果获取索引失败，记录错误但继续处理
				log.Printf("获取表 %s.%s 索引失败: %v", dbName, table.Name, err)
			} else {
				indexes += len(tableIndexes)
				// 假设每个索引大小为表大小的10%
				indexSize += tableSize / 10
			}

			// 假设每个对象大小为100字节
			tableObjects := tableSize / 100
			numObjects += tableObjects
		}

		// 计算平均对象大小
		if numObjects > 0 {
			avgObjSize = float64(dataSize) / float64(numObjects)
		}

		// 应用比例
		scaledAvgObjSize := avgObjSize / scale
		scaledDataSize := float64(dataSize) / scale
		scaledStorageSize := float64(storageSize) / scale
		scaledIndexSize := float64(indexSize) / scale
		scaledFileSize := float64(fileSize) / scale

		// 构建响应
		return bson.D{
			{"db", dbName},
			{"collections", len(tables)},
			{"views", 0},
			{"objects", numObjects},
			{"avgObjSize", scaledAvgObjSize},
			{"dataSize", scaledDataSize},
			{"storageSize", scaledStorageSize},
			{"numExtents", 0},
			{"indexes", indexes},
			{"indexSize", scaledIndexSize},
			{"fsUsedSize", scaledFileSize},
			{"fsTotalSize", scaledFileSize * 10}, // 假设总空间是已用空间的10倍
			{"ok", 1},
		}, nil
	})

	// 创建集合命令
	handler.Register("createCollection", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var timeseriesOptions *model.TimeSeriesOptions

		for _, elem := range cmd {
			switch elem.Key {
			case "createCollection":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "timeseries":
				if doc, ok := elem.Value.(bson.D); ok {
					timeseriesOptions = &model.TimeSeriesOptions{}
					for _, field := range doc {
						switch field.Key {
						case "timeField":
							if str, ok := field.Value.(string); ok {
								timeseriesOptions.TimeField = str
							}
						case "metaField":
							if str, ok := field.Value.(string); ok {
								timeseriesOptions.MetaField = str
							}
						case "granularity":
							if str, ok := field.Value.(string); ok {
								timeseriesOptions.Granularity = str
							}
						}
					}
				}
			case "expireAfterSeconds":
				if val, ok := elem.Value.(int64); ok {
					if timeseriesOptions == nil {
						timeseriesOptions = &model.TimeSeriesOptions{}
					}
					timeseriesOptions.ExpirySeconds = val
				}
			}
		}

		if dbName == "" || collName == "" {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 检查数据库是否存在
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}

		dbExists := false
		for _, db := range databases {
			if db.Name == dbName {
				dbExists = true
				break
			}
		}

		// 如果数据库不存在，创建数据库
		if !dbExists {
			retentionPolicy := model.RetentionPolicy{
				Duration:  30 * 24 * time.Hour, // 默认30天
				Precision: "ns",
			}

			if err := storageEngine.CreateDatabase(dbName, retentionPolicy); err != nil {
				return nil, fmt.Errorf("failed to create database: %w", err)
			}
		}

		// 检查集合是否已存在
		tables, err := storageEngine.ListTables(dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}

		for _, table := range tables {
			if table.Name == collName {
				return nil, fmt.Errorf("collection %s already exists", collName)
			}
		}

		// 创建集合
		schema := model.Schema{
			Fields:    make(map[string]string),
			TagFields: make(map[string]string),
		}

		// 如果是时序集合
		if timeseriesOptions != nil {
			schema.TimeField = timeseriesOptions.TimeField
			if timeseriesOptions.MetaField != "" {
				// 将元数据字段添加为标签字段
				schema.TagFields[timeseriesOptions.MetaField] = "object"
			}
		}

		// 创建表
		if err := storageEngine.CreateTable(dbName, collName, schema, nil); err != nil {
			return nil, fmt.Errorf("failed to create collection: %w", err)
		}

		return bson.D{{"ok", 1}}, nil
	})

	// 清空集合命令
	handler.Register("clear", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		logger.Printf("clear命令: 开始处理")

		var dbName, collName string

		for _, elem := range cmd {
			switch elem.Key {
			case "clear":
				if str, ok := elem.Value.(string); ok {
					collName = str
					logger.Printf("clear命令: 集合名称: %s", collName)
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
					logger.Printf("clear命令: 数据库名称: %s", dbName)
				}
			}
		}

		// 如果没有指定数据库，使用当前会话的数据库
		if dbName == "" {
			session, ok := ctx.Value("session").(*protocol.Session)
			if ok && session.CurrentDB != "" {
				dbName = session.CurrentDB
				logger.Printf("clear命令: 使用会话数据库: %s", dbName)
			}
		}

		if dbName == "" {
			logger.Printf("clear命令: 缺少数据库名称")
			return nil, fmt.Errorf("missing database name")
		}

		if collName == "" {
			logger.Printf("clear命令: 缺少集合名称")
			return nil, fmt.Errorf("missing collection name")
		}

		// 清空集合
		logger.Printf("clear命令: 开始清空集合 %s.%s", dbName, collName)
		if err := storageEngine.ClearTable(dbName, collName); err != nil {
			logger.Printf("clear命令: 清空集合失败: %v", err)
			return nil, err
		}
		logger.Printf("clear命令: 集合清空成功")

		return bson.D{
			{"ok", 1},
			{"cleared", collName},
			{"ns", dbName + "." + collName},
		}, nil
	})

	// 插入单个文档命令
	handler.Register("insertOne", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		logger.Printf("insertOne命令: 开始处理")

		var dbName, collName string
		var document interface{}

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "insertOne":
				collName = elem.Value.(string)
				logger.Printf("insertOne命令: 集合名称: %s", collName)
			case "document":
				document = elem.Value
				logger.Printf("insertOne命令: 找到文档")
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
					logger.Printf("insertOne命令: 数据库名称: %s", dbName)
				}
			}
		}

		// 如果没有指定数据库，使用当前会话的数据库
		if dbName == "" {
			session, ok := ctx.Value("session").(*protocol.Session)
			if ok && session.CurrentDB != "" {
				dbName = session.CurrentDB
				logger.Printf("insertOne命令: 使用会话数据库: %s", dbName)
			}
		}

		if dbName == "" {
			logger.Printf("insertOne命令: 缺少数据库名称")
			return nil, fmt.Errorf("missing database name")
		}

		if collName == "" {
			logger.Printf("insertOne命令: 缺少集合名称")
			return nil, fmt.Errorf("missing collection name")
		}

		if document == nil {
			logger.Printf("insertOne命令: 没有要插入的文档")
			return nil, fmt.Errorf("no document to insert")
		}

		// 检查数据库和集合是否存在
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			logger.Printf("insertOne命令: 获取数据库列表失败: %v", err)
			return nil, err
		}

		dbExists := false
		var db *model.Database
		for _, d := range databases {
			if d.Name == dbName {
				dbExists = true
				db = d
				break
			}
		}

		if !dbExists {
			logger.Printf("insertOne命令: 数据库 %s 不存在", dbName)
			return nil, fmt.Errorf("database %s does not exist", dbName)
		}

		tableExists := false
		if db != nil {
			_, tableExists = db.Tables[collName]
		}

		if !tableExists {
			logger.Printf("insertOne命令: 集合 %s 在数据库 %s 中不存在", collName, dbName)
			return nil, fmt.Errorf("collection %s does not exist in database %s", collName, dbName)
		}

		// 插入文档
		logger.Printf("insertOne命令: 开始插入文档到 %s.%s", dbName, collName)

		// 将文档转换为时序数据点
		if bsonDoc, ok := document.(bson.D); ok {
			// 创建时序数据点
			point := &model.TimeSeriesPoint{
				Timestamp: time.Now().UnixNano(),
				Tags:      make(map[string]string),
				Fields:    make(map[string]interface{}),
			}

			// 解析文档字段
			for _, field := range bsonDoc {
				// 假设所有字段都是普通字段
				point.Fields[field.Key] = field.Value
			}

			// 插入数据点
			if err := storageEngine.InsertPoint(dbName, collName, point); err != nil {
				logger.Printf("insertOne命令: 插入文档失败: %v", err)
				return nil, err
			}
		} else {
			logger.Printf("insertOne命令: 文档格式不正确")
			return nil, fmt.Errorf("invalid document format")
		}

		logger.Printf("insertOne命令: 文档插入成功")

		return bson.D{
			{"ok", 1},
			{"n", 1},
		}, nil
	})

	// 批量插入文档命令
	handler.Register("insertMany", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		logger.Printf("insertMany命令: 开始处理")

		var dbName, collName string
		var documents []interface{}

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "insertMany":
				collName = elem.Value.(string)
				logger.Printf("insertMany命令: 集合名称: %s", collName)
			case "documents":
				if docs, ok := elem.Value.(bson.A); ok {
					documents = make([]interface{}, len(docs))
					for i, doc := range docs {
						documents[i] = doc
					}
					logger.Printf("insertMany命令: 文档数量: %d", len(documents))
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
					logger.Printf("insertMany命令: 数据库名称: %s", dbName)
				}
			}
		}

		// 如果没有指定数据库，使用当前会话的数据库
		if dbName == "" {
			session, ok := ctx.Value("session").(*protocol.Session)
			if ok && session.CurrentDB != "" {
				dbName = session.CurrentDB
				logger.Printf("insertMany命令: 使用会话数据库: %s", dbName)
			}
		}

		if dbName == "" {
			logger.Printf("insertMany命令: 缺少数据库名称")
			return nil, fmt.Errorf("missing database name")
		}

		if collName == "" {
			logger.Printf("insertMany命令: 缺少集合名称")
			return nil, fmt.Errorf("missing collection name")
		}

		if len(documents) == 0 {
			logger.Printf("insertMany命令: 没有要插入的文档")
			return nil, fmt.Errorf("no documents to insert")
		}

		// 检查数据库和集合是否存在
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			logger.Printf("insertMany命令: 获取数据库列表失败: %v", err)
			return nil, err
		}

		dbExists := false
		var db *model.Database
		for _, d := range databases {
			if d.Name == dbName {
				dbExists = true
				db = d
				break
			}
		}

		if !dbExists {
			logger.Printf("insertMany命令: 数据库 %s 不存在", dbName)
			return nil, fmt.Errorf("database %s does not exist", dbName)
		}

		tableExists := false
		if db != nil {
			_, tableExists = db.Tables[collName]
		}

		if !tableExists {
			logger.Printf("insertMany命令: 集合 %s 在数据库 %s 中不存在", collName, dbName)
			return nil, fmt.Errorf("collection %s does not exist in database %s", collName, dbName)
		}

		// 批量插入文档
		logger.Printf("insertMany命令: 开始批量插入文档到 %s.%s", dbName, collName)

		// 将文档转换为时序数据点
		points := make([]*model.TimeSeriesPoint, 0, len(documents))
		for _, doc := range documents {
			if bsonDoc, ok := doc.(bson.D); ok {
				// 创建时序数据点
				point := &model.TimeSeriesPoint{
					Timestamp: time.Now().UnixNano(),
					Tags:      make(map[string]string),
					Fields:    make(map[string]interface{}),
				}

				// 解析文档字段
				for _, field := range bsonDoc {
					// 假设所有字段都是普通字段
					point.Fields[field.Key] = field.Value
				}

				points = append(points, point)
			}
		}

		// 批量插入数据点
		if err := storageEngine.InsertPoints(dbName, collName, points); err != nil {
			logger.Printf("insertMany命令: 批量插入文档失败: %v", err)
			return nil, err
		}

		logger.Printf("insertMany命令: 文档批量插入成功")
		return bson.D{{"ok", 1}, {"n", len(points)}}, nil
	})
}

// registerClusterCommands 注册集群相关命令
func registerClusterCommands(handler *protocol.CommandHandler, clusterManager common.ClusterManager) {
	// 获取集群状态命令
	handler.Register("getClusterStatus", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 获取集群信息
		clusterInfo, err := clusterManager.GetClusterInfo()
		if err != nil {
			return nil, err
		}

		// 获取所有节点
		dnodes, err := clusterManager.ListDNodes()
		if err != nil {
			return nil, err
		}

		// 构建节点列表
		nodeList := make(bson.A, 0, len(dnodes))
		for _, dnode := range dnodes {
			nodeList = append(nodeList, bson.D{
				{"nodeId", dnode.NodeID},
				{"status", int(dnode.Status)},
				{"role", int(dnode.Role)},
				{"publicIp", dnode.PublicIP.String()},
				{"privateIp", dnode.PrivateIP.String()},
				{"cpuUsage", dnode.CPUUsage},
				{"memoryUsage", dnode.MemoryUsage},
				{"diskUsage", dnode.DiskUsage},
				{"vnodeCount", len(dnode.VNodes)},
			})
		}

		// 获取所有虚拟节点组
		vgroups, err := clusterManager.ListVGroups()
		if err != nil {
			return nil, err
		}

		// 构建虚拟节点组列表
		vgroupList := make(bson.A, 0, len(vgroups))
		for _, vgroup := range vgroups {
			vgroupList = append(vgroupList, bson.D{
				{"vgroupId", vgroup.VGroupID},
				{"database", vgroup.DatabaseName},
				{"replicas", vgroup.Replicas},
				{"vnodeCount", len(vgroup.VNodes)},
				{"masterVnode", vgroup.MasterVNode},
			})
		}

		return bson.D{
			{"ok", 1},
			{"clusterId", clusterInfo.ClusterID},
			{"status", int(clusterInfo.Status)},
			{"nodeCount", len(dnodes)},
			{"vgroupCount", len(vgroups)},
			{"nodes", nodeList},
			{"vgroups", vgroupList},
		}, nil
	})

	// 添加节点命令
	handler.Register("addNode", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 解析参数
		var publicIP, privateIP string
		for _, elem := range cmd {
			switch elem.Key {
			case "publicIp":
				if ip, ok := elem.Value.(string); ok {
					publicIP = ip
				}
			case "privateIp":
				if ip, ok := elem.Value.(string); ok {
					privateIP = ip
				}
			}
		}

		if publicIP == "" {
			return nil, fmt.Errorf("publicIp is required")
		}

		// 创建节点信息
		nodeInfo := &common.DNodeInfo{
			NodeInfo: common.NodeInfo{
				NodeID:      primitive.NewObjectID().Hex(),
				NodeType:    common.NodeTypeDNode,
				Status:      common.NodeStatusOnline,
				Role:        common.NodeRoleUnknown,
				PublicIP:    net.ParseIP(publicIP),
				PrivateIP:   net.ParseIP(privateIP),
				PublicPort:  27017,
				PrivatePort: 27018,
				StartTime:   time.Now(),
			},
			VNodes: make([]string, 0),
		}

		// 添加节点
		if err := clusterManager.AddDNode(nodeInfo); err != nil {
			return nil, err
		}

		return bson.D{
			{"ok", 1},
			{"nodeId", nodeInfo.NodeID},
		}, nil
	})

	// 移除节点命令
	handler.Register("removeNode", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		// 解析参数
		var nodeID string
		for _, elem := range cmd {
			if elem.Key == "nodeId" {
				if id, ok := elem.Value.(string); ok {
					nodeID = id
				}
			}
		}

		if nodeID == "" {
			return nil, fmt.Errorf("nodeId is required")
		}

		// 移除节点
		if err := clusterManager.RemoveDNode(nodeID); err != nil {
			return nil, err
		}

		return bson.D{
			{"ok", 1},
		}, nil
	})
}
