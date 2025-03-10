package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
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
	"ChronoGo/pkg/model"
	"ChronoGo/pkg/protocol"
	"ChronoGo/pkg/query"
	"ChronoGo/pkg/storage"
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
			log.Fatalf("Failed to create cluster manager: %v", err)
		}

		// 启动集群管理器
		if err := clusterManager.Start(); err != nil {
			log.Fatalf("Failed to start cluster manager: %v", err)
		}
		defer clusterManager.Stop()

		// 获取集群信息
		clusterInfo, err := clusterManager.GetClusterInfo()
		if err != nil {
			log.Printf("Warning: Failed to get cluster info: %v", err)
		} else {
			log.Printf("Cluster ID: %s, Status: %v, Nodes: %d",
				clusterInfo.ClusterID,
				clusterInfo.Status,
				len(clusterInfo.DNodes))
		}

		// 注册集群相关命令
		registerClusterCommands(cmdHandler, clusterManager)
	}

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
	if *enableCluster {
		fmt.Printf("Cluster mode enabled\n")
	}

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
		// 获取所有数据库
		databases, err := storageEngine.ListDatabases()
		if err != nil {
			return nil, err
		}

		// 构建数据库列表
		dbList := make(bson.A, 0, len(databases))
		for _, db := range databases {
			dbList = append(dbList, bson.D{
				{"name", db.Name},
				{"sizeOnDisk", int64(0)}, // TODO: 实现数据库大小统计
				{"empty", len(db.Tables) == 0},
			})
		}

		return bson.D{
			{"ok", 1},
			{"databases", dbList},
			{"totalSize", int64(0)},   // TODO: 实现总大小统计
			{"totalSizeMb", int64(0)}, // TODO: 实现总大小统计(MB)
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

		// 获取数据库中的所有表
		tables, err := storageEngine.ListTables(dbName)
		if err != nil {
			return nil, err
		}

		// 构建集合列表
		collections := make(bson.A, 0, len(tables))
		for _, table := range tables {
			collections = append(collections, bson.D{
				{"name", table.Name},
				{"type", "collection"},
				{"options", bson.D{}},
				{"info", bson.D{
					{"readOnly", false},
					{"uuid", primitive.Binary{Subtype: 0x04}}, // 随机UUID
				}},
			})
		}

		return bson.D{
			{"ok", 1},
			{"cursor", bson.D{
				{"firstBatch", collections},
				{"id", int64(0)},
				{"ns", dbName + ".$cmd.listCollections"},
			}},
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
