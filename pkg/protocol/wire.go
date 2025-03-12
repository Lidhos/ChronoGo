package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"ChronoGo/pkg/logger"
	"ChronoGo/pkg/query"
)

// MongoDB操作码
const (
	OpReply       = 1
	OpUpdate      = 2001
	OpInsert      = 2002
	OpQuery       = 2004
	OpGetMore     = 2005
	OpDelete      = 2006
	OpKillCursors = 2007
	OpMsg         = 2013
)

// MongoDB响应标志
const (
	CursorNotFound   = 1 << 0
	QueryFailure     = 1 << 1
	ShardConfigStale = 1 << 2
	AwaitCapable     = 1 << 3
	Exhausted        = 1 << 4
)

// MsgHeader 表示MongoDB消息头
type MsgHeader struct {
	MessageLength int32 // 消息总长度
	RequestID     int32 // 请求ID
	ResponseTo    int32 // 响应ID
	OpCode        int32 // 操作码
}

// Message 表示MongoDB消息
type Message struct {
	Header MsgHeader
	Body   []byte
}

// Session 表示客户端会话
type Session struct {
	ID         int32
	Conn       net.Conn
	mu         sync.Mutex
	cursorMap  map[int64]*Cursor
	nextCursor int64
	lastActive time.Time // 最后活动时间
	CurrentDB  string    // 当前选择的数据库
}

// Cursor 表示查询游标
type Cursor struct {
	ID         int64
	Query      bson.D
	Database   string
	Collection string
	Batch      []bson.D
	Position   int
	Exhausted  bool
	CreatedAt  time.Time // 创建时间
	LastUsed   time.Time // 最后使用时间
}

// WireProtocolHandler 表示MongoDB协议处理器
type WireProtocolHandler struct {
	listener      net.Listener
	sessions      map[int32]*Session
	nextSession   int32
	nextRequestID int32 // 添加请求ID计数器
	cmdHandler    *CommandHandler
	queryEngine   *query.QueryEngine
	storageEngine interface{} // 使用interface{}以避免循环导入
	mu            sync.RWMutex
	closing       chan struct{}

	// 游标管理
	cursorTTL     time.Duration // 游标生存时间
	cleanupTicker *time.Ticker  // 清理定时器

	// 连接池管理
	maxConnections int           // 最大连接数
	idleTimeout    time.Duration // 空闲连接超时时间

	// 批处理
	batchSize     int           // 批处理大小
	batchInterval time.Duration // 批处理间隔
	batchQueue    chan *batchOperation
	batchWorkers  int // 批处理工作线程数
}

// batchOperation 表示批处理操作
type batchOperation struct {
	opType     string           // 操作类型：insert, update, delete
	database   string           // 数据库名
	collection string           // 集合名
	documents  []bson.D         // 文档
	filter     bson.D           // 过滤条件
	update     bson.D           // 更新内容
	resultCh   chan batchResult // 结果通道
}

// batchResult 表示批处理结果
type batchResult struct {
	count int   // 影响的文档数
	err   error // 错误
}

// SetQueryEngine 设置查询引擎
func (h *WireProtocolHandler) SetQueryEngine(engine *query.QueryEngine) {
	h.queryEngine = engine
}

// SetStorageEngine 设置存储引擎
func (h *WireProtocolHandler) SetStorageEngine(engine interface{}) {
	h.storageEngine = engine
}

// NewWireProtocolHandler 创建新的协议处理器
func NewWireProtocolHandler(addr string, cmdHandler *CommandHandler) (*WireProtocolHandler, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	h := &WireProtocolHandler{
		listener:      listener,
		sessions:      make(map[int32]*Session),
		nextSession:   1,
		nextRequestID: 1,
		cmdHandler:    cmdHandler,
		closing:       make(chan struct{}),

		// 游标管理
		cursorTTL:     time.Hour, // 默认游标生存时间为1小时
		cleanupTicker: time.NewTicker(10 * time.Minute),

		maxConnections: 1000,                   // 默认最大连接数
		idleTimeout:    30 * time.Minute,       // 默认空闲连接超时时间
		batchSize:      1000,                   // 默认批处理大小
		batchInterval:  100 * time.Millisecond, // 默认批处理间隔
		batchQueue:     make(chan *batchOperation, 10000),
		batchWorkers:   4, // 默认4个批处理工作线程
	}

	return h, nil
}

// Start 启动协议处理器
func (h *WireProtocolHandler) Start() {
	go h.acceptLoop()
	go h.cleanupLoop() // 启动游标清理循环

	// 启动批处理工作线程
	for i := 0; i < h.batchWorkers; i++ {
		go h.batchWorker()
	}
}

// acceptLoop 接受新连接
func (h *WireProtocolHandler) acceptLoop() {
	for {
		select {
		case <-h.closing:
			return
		default:
			conn, err := h.listener.Accept()
			if err != nil {
				// 检查是否是因为关闭而导致的错误
				select {
				case <-h.closing:
					return
				default:
					// 其他错误，继续尝试接受连接
					continue
				}
			}

			// 创建新会话
			sessionID := atomic.AddInt32(&h.nextSession, 1)
			session := &Session{
				ID:        sessionID,
				Conn:      conn,
				cursorMap: make(map[int64]*Cursor),
			}

			h.mu.Lock()
			h.sessions[sessionID] = session
			h.mu.Unlock()

			// 处理会话
			go h.handleSession(session)
		}
	}
}

// handleSession 处理客户端会话
func (h *WireProtocolHandler) handleSession(session *Session) {
	defer func() {
		h.mu.Lock()
		delete(h.sessions, session.ID)
		h.mu.Unlock()

		logger.Printf("Client disconnected: %s (Session ID: %d)", session.Conn.RemoteAddr(), session.ID)
	}()

	logger.Printf("handleSession: 新客户端连接: %s (Session ID: %d)", session.Conn.RemoteAddr(), session.ID)

	buffer := make([]byte, 4096)
	logger.Printf("handleSession: 创建缓冲区，大小: %d 字节", len(buffer))

	for {
		// 检查是否正在关闭
		select {
		case <-h.closing:
			logger.Printf("handleSession: 服务正在关闭，终止会话 %d", session.ID)
			return
		default:
		}

		// 读取消息头
		logger.Printf("handleSession: 等待读取消息头...")
		headerBytes := make([]byte, 16)
		_, err := io.ReadFull(session.Conn, headerBytes)
		if err != nil {
			if err != io.EOF {
				logger.Printf("handleSession: 读取消息头失败: %v", err)
			} else {
				logger.Printf("handleSession: 客户端断开连接 (EOF)")
			}
			return
		}

		// 更新会话活动时间
		session.updateActivity()
		logger.Printf("handleSession: 更新会话活动时间")

		// 解析消息头
		header := MsgHeader{
			MessageLength: int32(binary.LittleEndian.Uint32(headerBytes[0:4])),
			RequestID:     int32(binary.LittleEndian.Uint32(headerBytes[4:8])),
			ResponseTo:    int32(binary.LittleEndian.Uint32(headerBytes[8:12])),
			OpCode:        int32(binary.LittleEndian.Uint32(headerBytes[12:16])),
		}
		logger.Printf("handleSession: 解析消息头: 长度=%d, RequestID=%d, ResponseTo=%d, OpCode=%d",
			header.MessageLength, header.RequestID, header.ResponseTo, header.OpCode)

		// 检查消息长度
		if header.MessageLength < 16 {
			logger.Printf("handleSession: 无效的消息长度: %d", header.MessageLength)
			return
		}

		// 读取消息体
		bodyLength := header.MessageLength - 16
		var body []byte
		logger.Printf("handleSession: 消息体长度: %d 字节", bodyLength)

		if bodyLength > 0 {
			// 如果消息体太大，需要扩展缓冲区
			if bodyLength > int32(len(buffer)) {
				logger.Printf("handleSession: 消息体太大，创建新缓冲区，大小: %d 字节", bodyLength)
				body = make([]byte, bodyLength)
			} else {
				logger.Printf("handleSession: 使用现有缓冲区")
				body = buffer[:bodyLength]
			}

			logger.Printf("handleSession: 开始读取消息体...")
			_, err = io.ReadFull(session.Conn, body)
			if err != nil {
				logger.Printf("handleSession: 读取消息体失败: %v", err)
				return
			}
			logger.Printf("handleSession: 读取消息体成功，大小: %d 字节", len(body))
		} else {
			logger.Printf("handleSession: 消息没有消息体")
		}

		// 处理消息
		msg := &Message{
			Header: header,
			Body:   body,
		}
		logger.Printf("handleSession: 创建消息对象，准备处理")

		logger.Printf("handleSession: 开始处理消息 (RequestID: %d)", msg.Header.RequestID)
		err = h.handleMessage(session, msg)
		if err != nil {
			logger.Printf("handleSession: 处理消息失败: %v", err)
			return
		}
		logger.Printf("handleSession: 处理消息成功 (RequestID: %d)", msg.Header.RequestID)
	}
}

// handleMessage 处理单个消息
func (h *WireProtocolHandler) handleMessage(session *Session, msg *Message) error {
	logger.Printf("handleMessage: 开始处理消息 (RequestID: %d, OpCode: %d)", msg.Header.RequestID, msg.Header.OpCode)

	switch msg.Header.OpCode {
	case OpQuery:
		logger.Printf("handleMessage: 处理 OpQuery 消息")
		err := h.handleOpQuery(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpQuery 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpQuery 处理成功")
		return nil
	case OpGetMore:
		logger.Printf("handleMessage: 处理 OpGetMore 消息")
		err := h.handleOpGetMore(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpGetMore 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpGetMore 处理成功")
		return nil
	case OpKillCursors:
		logger.Printf("handleMessage: 处理 OpKillCursors 消息")
		err := h.handleOpKillCursors(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpKillCursors 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpKillCursors 处理成功")
		return nil
	case OpInsert:
		logger.Printf("handleMessage: 处理 OpInsert 消息")
		err := h.handleOpInsert(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpInsert 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpInsert 处理成功")
		return nil
	case OpUpdate:
		logger.Printf("handleMessage: 处理 OpUpdate 消息")
		err := h.handleOpUpdate(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpUpdate 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpUpdate 处理成功")
		return nil
	case OpDelete:
		logger.Printf("handleMessage: 处理 OpDelete 消息")
		err := h.handleOpDelete(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpDelete 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpDelete 处理成功")
		return nil
	case OpMsg:
		logger.Printf("handleMessage: 处理 OpMsg 消息")
		err := h.handleOpMsg(session, msg)
		if err != nil {
			logger.Printf("handleMessage: OpMsg 处理失败: %v", err)
			return err
		}
		logger.Printf("handleMessage: OpMsg 处理成功")
		return nil
	default:
		// 不支持的操作码
		logger.Printf("handleMessage: 不支持的操作码: %d", msg.Header.OpCode)
		return fmt.Errorf("unsupported opcode: %d", msg.Header.OpCode)
	}
}

// handleOpQuery 处理查询操作
func (h *WireProtocolHandler) handleOpQuery(session *Session, msg *Message) error {
	// 解析查询消息
	if len(msg.Body) < 12 {
		return fmt.Errorf("invalid query message")
	}

	// 读取标志位（暂时不使用）
	_ = binary.LittleEndian.Uint32(msg.Body[0:4])
	offset := 4

	// 读取集合名称
	collName, n, err := readCString(msg.Body[offset:])
	if err != nil {
		return err
	}
	offset += n

	// 读取跳过数量和返回数量（暂时不使用）
	_ = binary.LittleEndian.Uint32(msg.Body[offset:]) // numberToSkip
	offset += 4
	_ = binary.LittleEndian.Uint32(msg.Body[offset:]) // numberToReturn
	offset += 4

	// 读取查询文档
	var query bson.D
	err = bson.Unmarshal(msg.Body[offset:], &query)
	if err != nil {
		return err
	}

	// 解析命令
	parts := splitNamespace(collName)
	if len(parts) != 2 {
		return fmt.Errorf("invalid namespace: %s", collName)
	}

	// 这里不使用dbName，但保留它以便将来扩展
	collName = parts[1]

	// 检查是否是命令
	if collName == "$cmd" {
		// 创建上下文，并添加会话信息和引擎信息
		ctx := context.Background()
		ctx = context.WithValue(ctx, "session", session)

		// 添加查询引擎和存储引擎到上下文
		if h.queryEngine != nil {
			ctx = context.WithValue(ctx, "queryEngine", h.queryEngine)
		}

		if h.storageEngine != nil {
			ctx = context.WithValue(ctx, "storageEngine", h.storageEngine)
		}

		// 执行命令
		result, err := h.cmdHandler.Execute(ctx, query)
		if err != nil {
			// 创建错误响应
			errorDoc := bson.D{
				{"ok", 0},
				{"errmsg", err.Error()},
				{"code", 1},
			}

			// 发送错误响应
			return h.sendReply(session, msg.Header.RequestID, []bson.D{errorDoc}, QueryFailure, 0)
		}

		// 发送成功响应
		return h.sendReply(session, msg.Header.RequestID, []bson.D{result}, 0, 0)
	}

	// 处理普通查询
	// TODO: 实现普通查询处理
	// 在实际实现中，应该使用dbName和collName参数
	_ = parts[0]

	// 暂时返回空结果
	return h.sendReply(session, msg.Header.RequestID, []bson.D{}, 0, 0)
}

// handleOpGetMore 处理获取更多结果操作
func (h *WireProtocolHandler) handleOpGetMore(session *Session, msg *Message) error {
	// 解析GetMore消息
	if len(msg.Body) < 16 {
		return fmt.Errorf("invalid getMore message")
	}

	// 更新会话活动时间
	session.updateActivity()

	// 读取集合名称
	collName, n, err := readCString(msg.Body[0:])
	if err != nil {
		return err
	}
	offset := n

	// 读取返回数量
	numberToReturn := binary.LittleEndian.Uint32(msg.Body[offset:])
	offset += 4

	// 读取游标ID
	cursorID := int64(binary.LittleEndian.Uint64(msg.Body[offset:]))

	// 解析命名空间
	parts := splitNamespace(collName)
	if len(parts) != 2 {
		return fmt.Errorf("invalid namespace: %s", collName)
	}

	// 这里不使用dbName，但保留它以便将来扩展
	collName = parts[1]

	// 获取游标
	cursor := session.getCursor(cursorID)
	if cursor == nil {
		// 游标不存在，返回空结果
		return h.sendReply(session, msg.Header.RequestID, []bson.D{}, CursorNotFound, 0)
	}

	// 获取下一批数据
	batch := cursor.getNextBatch(int(numberToReturn))

	// 如果游标已耗尽，则移除
	if cursor.Exhausted {
		session.removeCursor(cursorID)
		cursorID = 0
	}

	// 发送响应
	return h.sendReply(session, msg.Header.RequestID, batch, 0, cursorID)
}

// handleOpKillCursors 处理关闭游标操作
func (h *WireProtocolHandler) handleOpKillCursors(session *Session, msg *Message) error {
	// 解析KillCursors消息
	if len(msg.Body) < 8 {
		return fmt.Errorf("invalid killCursors message")
	}

	// 读取游标数量
	numCursors := int(binary.LittleEndian.Uint32(msg.Body[0:4]))

	// 确保消息长度足够
	if len(msg.Body) < 8+numCursors*8 {
		return fmt.Errorf("invalid killCursors message length")
	}

	// 读取游标ID列表
	cursorIDs := make([]int64, numCursors)
	for i := 0; i < numCursors; i++ {
		offset := 8 + i*8
		cursorIDs[i] = int64(binary.LittleEndian.Uint64(msg.Body[offset:]))
	}

	// 删除游标
	session.mu.Lock()
	for _, cursorID := range cursorIDs {
		delete(session.cursorMap, cursorID)
	}
	session.mu.Unlock()

	// KillCursors操作不需要响应
	return nil
}

// handleOpInsert 处理插入操作
func (h *WireProtocolHandler) handleOpInsert(session *Session, msg *Message) error {
	// 解析Insert消息
	if len(msg.Body) < 4 {
		return fmt.Errorf("invalid insert message")
	}

	// 跳过标志位(4字节)
	offset := 4

	// 读取集合名称
	collName, n, err := readCString(msg.Body[offset:])
	if err != nil {
		return err
	}
	offset += n

	// 解析数据库和集合名称
	parts := splitNamespace(collName)
	if len(parts) != 2 {
		return fmt.Errorf("invalid namespace: %s", collName)
	}
	dbName, colName := parts[0], parts[1] // 使用数据库名称和集合名称

	// 解析文档列表
	documents := make([]bson.D, 0)
	for offset < len(msg.Body) {
		// 读取文档长度
		if offset+4 > len(msg.Body) {
			break
		}
		docLen := int(binary.LittleEndian.Uint32(msg.Body[offset:]))
		if offset+docLen > len(msg.Body) || docLen < 5 {
			break
		}

		// 解析文档
		var doc bson.D
		err = bson.Unmarshal(msg.Body[offset:offset+docLen], &doc)
		if err != nil {
			return err
		}
		documents = append(documents, doc)
		offset += docLen
	}

	// 创建查询上下文
	ctx := context.Background()

	// 执行insert命令
	insertCmd := bson.D{
		{"insert", colName},
		{"documents", documents},
		{"$db", dbName},
	}

	result, err := h.cmdHandler.Execute(ctx, insertCmd)
	if err != nil {
		// 发送错误响应
		errorDoc := bson.D{
			{"ok", 0},
			{"errmsg", err.Error()},
			{"code", 8000}, // 通用错误码
		}
		return h.sendOpMsg(session, msg.Header.RequestID, errorDoc)
	}

	// 发送成功响应
	return h.sendOpMsg(session, msg.Header.RequestID, result)
}

// handleOpUpdate 处理更新操作
func (h *WireProtocolHandler) handleOpUpdate(session *Session, msg *Message) error {
	// 解析Update消息
	if len(msg.Body) < 8 {
		return fmt.Errorf("invalid update message")
	}

	// 跳过标志位(4字节)
	offset := 0

	// 读取集合名称
	collName, n, err := readCString(msg.Body[offset:])
	if err != nil {
		return err
	}
	offset += n

	// 解析数据库和集合名称
	parts := splitNamespace(collName)
	if len(parts) != 2 {
		return fmt.Errorf("invalid namespace: %s", collName)
	}
	_, colName := parts[0], parts[1] // 只使用集合名称

	// 读取标志位
	flags := binary.LittleEndian.Uint32(msg.Body[offset:])
	offset += 4
	upsert := (flags & 1) != 0
	multi := (flags & 2) != 0

	// 解析选择器文档
	if offset+4 > len(msg.Body) {
		return fmt.Errorf("invalid update message")
	}
	selectorLen := int(binary.LittleEndian.Uint32(msg.Body[offset:]))
	if offset+selectorLen > len(msg.Body) || selectorLen < 5 {
		return fmt.Errorf("invalid selector document")
	}

	var selector bson.D
	err = bson.Unmarshal(msg.Body[offset:offset+selectorLen], &selector)
	if err != nil {
		return err
	}
	offset += selectorLen

	// 解析更新文档
	if offset+4 > len(msg.Body) {
		return fmt.Errorf("invalid update message")
	}
	updateLen := int(binary.LittleEndian.Uint32(msg.Body[offset:]))
	if offset+updateLen > len(msg.Body) || updateLen < 5 {
		return fmt.Errorf("invalid update document")
	}

	var update bson.D
	err = bson.Unmarshal(msg.Body[offset:offset+updateLen], &update)
	if err != nil {
		return err
	}

	// 创建查询上下文
	ctx := context.Background()

	// 执行update命令
	updateCmd := bson.D{
		{"update", colName},
		{"updates", bson.A{
			bson.D{
				{"q", selector},
				{"u", update},
				{"upsert", upsert},
				{"multi", multi},
			},
		}},
	}

	_, err = h.cmdHandler.Execute(ctx, updateCmd) // 忽略结果
	if err != nil {
		// 记录错误，但不发送响应
		// 因为旧版MongoDB协议中，update操作不返回响应
		logger.Printf("Update error: %v", err)
		return nil
	}

	// Update操作在旧版协议中不需要响应
	return nil
}

// handleOpDelete 处理删除操作
func (h *WireProtocolHandler) handleOpDelete(session *Session, msg *Message) error {
	// 解析Delete消息
	if len(msg.Body) < 8 {
		return fmt.Errorf("invalid delete message")
	}

	// 跳过标志位(4字节)
	offset := 0

	// 读取集合名称
	collName, n, err := readCString(msg.Body[offset:])
	if err != nil {
		return err
	}
	offset += n

	// 解析数据库和集合名称
	parts := splitNamespace(collName)
	if len(parts) != 2 {
		return fmt.Errorf("invalid namespace: %s", collName)
	}
	_, colName := parts[0], parts[1] // 只使用集合名称

	// 读取标志位
	flags := binary.LittleEndian.Uint32(msg.Body[offset:])
	offset += 4
	singleRemove := (flags & 1) == 0 // 如果标志位为0，则只删除一个文档

	// 解析选择器文档
	if offset+4 > len(msg.Body) {
		return fmt.Errorf("invalid delete message")
	}
	selectorLen := int(binary.LittleEndian.Uint32(msg.Body[offset:]))
	if offset+selectorLen > len(msg.Body) || selectorLen < 5 {
		return fmt.Errorf("invalid selector document")
	}

	var selector bson.D
	err = bson.Unmarshal(msg.Body[offset:offset+selectorLen], &selector)
	if err != nil {
		return err
	}

	// 创建查询上下文
	ctx := context.Background()

	// 执行delete命令
	deleteCmd := bson.D{
		{"delete", colName},
		{"deletes", bson.A{
			bson.D{
				{"q", selector},
				{"limit", singleRemove},
			},
		}},
	}

	_, err = h.cmdHandler.Execute(ctx, deleteCmd) // 忽略结果
	if err != nil {
		// 记录错误，但不发送响应
		// 因为旧版MongoDB协议中，delete操作不返回响应
		logger.Printf("Delete error: %v", err)
		return nil
	}

	// Delete操作在旧版协议中不需要响应
	return nil
}

// handleOpMsg 处理消息操作(MongoDB 3.6+)
func (h *WireProtocolHandler) handleOpMsg(session *Session, msg *Message) error {
	logger.Printf("开始处理OpMsg消息 (RequestID: %d)", msg.Header.RequestID)

	// 解析OpMsg消息
	if len(msg.Body) < 4 {
		logger.Printf("无效的OpMsg消息: 消息体长度不足，实际长度: %d", len(msg.Body))
		return fmt.Errorf("invalid opMsg message: body length too short")
	}

	// 读取标志位
	flags := binary.LittleEndian.Uint32(msg.Body[0:4])
	offset := 4
	logger.Printf("OpMsg标志位: %d", flags)

	// 检查消息格式
	if (flags & 1) != 0 {
		// 校验和存在，暂不支持
		logger.Printf("不支持带校验和的消息")
		return fmt.Errorf("checksum not supported")
	}

	// 读取第一个部分
	if offset >= len(msg.Body) {
		logger.Printf("无效的OpMsg消息: 消息体不完整，offset=%d, 消息体长度=%d", offset, len(msg.Body))
		return fmt.Errorf("invalid opMsg message: incomplete body")
	}

	// 检查部分类型
	partType := msg.Body[offset]
	offset++
	logger.Printf("OpMsg部分类型: %d", partType)

	if partType != 0 {
		logger.Printf("不支持的部分类型: %d", partType)
		return fmt.Errorf("unsupported section type: %d", partType)
	}

	// 检查剩余数据是否足够
	remainingBytes := len(msg.Body) - offset
	if remainingBytes <= 0 {
		logger.Printf("无效的OpMsg消息: 没有足够的数据用于BSON文档，剩余字节数: %d", remainingBytes)
		return fmt.Errorf("invalid opMsg message: not enough data for BSON document")
	}

	// 检查BSON文档长度
	if remainingBytes < 5 {
		logger.Printf("无效的OpMsg消息: BSON文档长度不足，剩余字节数: %d", remainingBytes)
		return fmt.Errorf("invalid opMsg message: BSON document too short")
	}

	// 获取BSON文档声明的长度
	declaredLength := int(binary.LittleEndian.Uint32(msg.Body[offset : offset+4]))
	logger.Printf("BSON文档声明的长度: %d，实际剩余字节数: %d", declaredLength, remainingBytes)

	// 打印前20个字节的十六进制值，帮助调试
	hexBytes := ""
	for i := 0; i < min(20, remainingBytes); i++ {
		hexBytes += fmt.Sprintf("%02x ", msg.Body[offset+i])
	}
	logger.Printf("BSON文档前20个字节: %s", hexBytes)

	// 尝试使用声明的长度解析文档
	var doc bson.D
	var err error

	if declaredLength <= remainingBytes && declaredLength >= 5 {
		// 正常情况：声明的长度小于等于实际剩余字节数
		err = bson.Unmarshal(msg.Body[offset:offset+declaredLength], &doc)
		if err == nil {
			logger.Printf("使用声明的长度成功解析文档: %v", doc)
		} else {
			logger.Printf("使用声明的长度解析文档失败: %v", err)
		}
	} else {
		// 异常情况：声明的长度大于实际剩余字节数或小于最小BSON文档长度
		logger.Printf("声明的长度异常，尝试使用全部剩余字节解析")
	}

	// 如果使用声明的长度解析失败，尝试使用全部剩余字节
	if err != nil || len(doc) == 0 {
		err = bson.Unmarshal(msg.Body[offset:], &doc)
		if err != nil {
			logger.Printf("使用全部剩余字节解析文档失败: %v", err)

			// 尝试查找有效的BSON文档
			logger.Printf("尝试查找有效的BSON文档...")

			// 尝试不同的偏移量
			for tryOffset := offset; tryOffset < offset+min(20, remainingBytes); tryOffset++ {
				if tryOffset+4 <= len(msg.Body) {
					tryLength := int(binary.LittleEndian.Uint32(msg.Body[tryOffset : tryOffset+4]))
					if tryLength >= 5 && tryLength <= remainingBytes-(tryOffset-offset) {
						logger.Printf("尝试偏移量 %d，长度 %d", tryOffset-offset, tryLength)
						var tryDoc bson.D
						tryErr := bson.Unmarshal(msg.Body[tryOffset:tryOffset+tryLength], &tryDoc)
						if tryErr == nil && len(tryDoc) > 0 {
							logger.Printf("在偏移量 %d 找到有效文档: %v", tryOffset-offset, tryDoc)
							doc = tryDoc
							err = nil
							break
						}
					}
				}
			}

			if err != nil {
				return fmt.Errorf("failed to unmarshal command document: %w", err)
			}
		} else {
			logger.Printf("使用全部剩余字节成功解析文档: %v", doc)
		}
	}

	logger.Printf("解析命令文档成功: %v", doc)

	// 创建查询上下文，并添加会话信息和引擎信息
	ctx := context.Background()
	ctx = context.WithValue(ctx, "session", session)

	// 添加查询引擎和存储引擎到上下文
	if h.queryEngine != nil {
		ctx = context.WithValue(ctx, "queryEngine", h.queryEngine)
	}

	if h.storageEngine != nil {
		ctx = context.WithValue(ctx, "storageEngine", h.storageEngine)
	}

	// 添加请求ID到日志
	cmdName := getCommandName(doc)
	logger.Printf("Received command: %v (RequestID: %d)", cmdName, msg.Header.RequestID)

	// 执行命令
	logger.Printf("开始执行命令: %s", cmdName)
	result, err := h.cmdHandler.Execute(ctx, doc)
	if err != nil {
		logger.Printf("Command execution error: %v", err)
		// 创建错误响应
		errorDoc := bson.D{
			{"ok", 0},
			{"errmsg", err.Error()},
			{"code", 1},
		}

		// 发送错误响应
		logger.Printf("发送错误响应: %v", errorDoc)
		return h.sendOpMsg(session, msg.Header.RequestID, errorDoc)
	}

	// 发送成功响应
	logger.Printf("命令执行成功，发送响应: %v", result)
	return h.sendOpMsg(session, msg.Header.RequestID, result)
}

// getCommandName 获取命令名称
func getCommandName(doc bson.D) string {
	if len(doc) > 0 {
		return doc[0].Key
	}
	return "unknown"
}

// sendOpMsg 发送OpMsg响应
func (h *WireProtocolHandler) sendOpMsg(session *Session, responseTo int32, document bson.D) error {
	logger.Printf("sendOpMsg: 开始发送响应 (ResponseTo: %d)", responseTo)

	// 序列化文档
	docBytes, err := bson.Marshal(document)
	if err != nil {
		logger.Printf("sendOpMsg: 序列化文档失败: %v", err)
		return err
	}
	logger.Printf("sendOpMsg: 序列化文档成功，大小: %d 字节", len(docBytes))

	// 计算消息长度
	// 消息头(16) + 标志位(4) + 部分类型(1) + 文档长度
	msgLen := 16 + 4 + 1 + len(docBytes)
	logger.Printf("sendOpMsg: 消息总长度: %d 字节", msgLen)

	// 创建响应消息
	response := Message{
		Header: MsgHeader{
			MessageLength: int32(msgLen),
			RequestID:     atomic.AddInt32(&h.nextRequestID, 1),
			ResponseTo:    responseTo,
			OpCode:        OpMsg,
		},
		Body: make([]byte, msgLen-16),
	}
	logger.Printf("sendOpMsg: 创建响应消息: RequestID=%d, ResponseTo=%d", response.Header.RequestID, response.Header.ResponseTo)

	// 写入标志位(0)
	binary.LittleEndian.PutUint32(response.Body[0:4], 0)

	// 写入部分类型(0)
	response.Body[4] = 0

	// 写入文档
	copy(response.Body[5:], docBytes)

	// 发送响应
	logger.Printf("sendOpMsg: 开始发送消息")
	err = h.sendMessage(session, &response)
	if err != nil {
		logger.Printf("sendOpMsg: 发送消息失败: %v", err)
		return err
	}

	logger.Printf("sendOpMsg: 发送消息成功")
	return nil
}

// 辅助函数

// readCString 读取C风格字符串
func readCString(data []byte) (string, int, error) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), i + 1, nil
		}
	}
	return "", 0, fmt.Errorf("invalid C string")
}

// splitNamespace 分割命名空间为数据库和集合
func splitNamespace(namespace string) []string {
	for i := 0; i < len(namespace); i++ {
		if namespace[i] == '.' {
			return []string{namespace[:i], namespace[i+1:]}
		}
	}
	return []string{namespace}
}

// getBsonValue 从BSON文档中获取值
// sendReply 发送响应
func (h *WireProtocolHandler) sendReply(session *Session, responseTo int32, documents []bson.D, flags int32, cursorID int64) error {
	logger.Printf("sendReply: 开始发送响应 (ResponseTo: %d, 文档数: %d, 标志位: %d, 游标ID: %d)",
		responseTo, len(documents), flags, cursorID)

	// 序列化文档
	docBytes := make([]byte, 0)
	for i, doc := range documents {
		b, err := bson.Marshal(doc)
		if err != nil {
			logger.Printf("sendReply: 序列化文档 #%d 失败: %v", i, err)
			return fmt.Errorf("failed to marshal document: %w", err)
		}
		docBytes = append(docBytes, b...)
		logger.Printf("sendReply: 文档 #%d 序列化成功，大小: %d 字节", i, len(b))
	}
	logger.Printf("sendReply: 所有文档序列化完成，总大小: %d 字节", len(docBytes))

	// 计算消息长度
	messageLength := 16 + 20 + len(docBytes)
	logger.Printf("sendReply: 消息总长度: %d 字节", messageLength)

	// 创建响应头
	header := MsgHeader{
		MessageLength: int32(messageLength),
		RequestID:     atomic.AddInt32(&h.nextSession, 1),
		ResponseTo:    responseTo,
		OpCode:        OpReply,
	}
	logger.Printf("sendReply: 创建响应头: 长度=%d, RequestID=%d, ResponseTo=%d, OpCode=%d",
		header.MessageLength, header.RequestID, header.ResponseTo, header.OpCode)

	// 创建响应体
	body := make([]byte, 20+len(docBytes))
	binary.LittleEndian.PutUint32(body[0:4], uint32(flags))
	binary.LittleEndian.PutUint64(body[4:12], uint64(cursorID))
	binary.LittleEndian.PutUint32(body[12:16], uint32(0)) // 起始位置
	binary.LittleEndian.PutUint32(body[16:20], uint32(len(documents)))
	copy(body[20:], docBytes)
	logger.Printf("sendReply: 创建响应体，大小: %d 字节", len(body))

	// 发送响应
	headerBytes := make([]byte, 16)
	binary.LittleEndian.PutUint32(headerBytes[0:4], uint32(header.MessageLength))
	binary.LittleEndian.PutUint32(headerBytes[4:8], uint32(header.RequestID))
	binary.LittleEndian.PutUint32(headerBytes[8:12], uint32(header.ResponseTo))
	binary.LittleEndian.PutUint32(headerBytes[12:16], uint32(header.OpCode))
	logger.Printf("sendReply: 序列化响应头完成")

	session.mu.Lock()
	defer session.mu.Unlock()

	logger.Printf("sendReply: 开始发送响应头")
	if _, err := session.Conn.Write(headerBytes); err != nil {
		logger.Printf("sendReply: 发送响应头失败: %v", err)
		return fmt.Errorf("failed to write header: %w", err)
	}
	logger.Printf("sendReply: 发送响应头成功")

	logger.Printf("sendReply: 开始发送响应体")
	if _, err := session.Conn.Write(body); err != nil {
		logger.Printf("sendReply: 发送响应体失败: %v", err)
		return fmt.Errorf("failed to write body: %w", err)
	}
	logger.Printf("sendReply: 发送响应体成功")

	logger.Printf("sendReply: 响应发送完成")
	return nil
}

// Close 关闭协议处理器
func (h *WireProtocolHandler) Close() error {
	close(h.closing)

	// 关闭所有会话
	h.mu.Lock()
	for _, session := range h.sessions {
		session.Conn.Close()
	}
	h.sessions = make(map[int32]*Session)
	h.mu.Unlock()

	// 关闭监听器
	return h.listener.Close()
}

// CommandHandler 表示命令处理器
type CommandHandler struct {
	commands map[string]CommandFunc
	mu       sync.RWMutex
}

// CommandFunc 表示命令处理函数
type CommandFunc func(ctx context.Context, cmd bson.D) (bson.D, error)

// NewCommandHandler 创建新的命令处理器
func NewCommandHandler() *CommandHandler {
	return &CommandHandler{
		commands: make(map[string]CommandFunc),
	}
}

// Register 注册命令处理函数
func (h *CommandHandler) Register(name string, fn CommandFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commands[name] = fn
}

// Execute 执行命令
func (h *CommandHandler) Execute(ctx context.Context, cmd bson.D) (bson.D, error) {
	logger.Printf("CommandHandler.Execute: 开始执行命令")

	if len(cmd) == 0 {
		logger.Printf("CommandHandler.Execute: 空命令")
		return nil, fmt.Errorf("empty command")
	}

	// 获取命令名称
	cmdName := strings.ToLower(cmd[0].Key)
	logger.Printf("CommandHandler.Execute: 原始命令名称: %s", cmdName)

	// 处理命令别名
	originalName := cmdName
	switch cmdName {
	case "ismaster":
		cmdName = "isMaster" // 标准化为驼峰命名
	case "listdatabases":
		cmdName = "listDatabases"
	case "listcollections":
		cmdName = "listCollections"
	case "dropdatabase":
		cmdName = "dropDatabase"
	case "buildinfo":
		cmdName = "buildInfo"
	case "serverstatus":
		cmdName = "serverStatus"
	case "getparameter":
		cmdName = "getParameter"
	case "getlog":
		cmdName = "getLog"
	case "getmore":
		cmdName = "getMore"
	case "killcursors":
		cmdName = "killCursors"
	case "timewindow":
		cmdName = "timeWindow"
	case "movingwindow":
		cmdName = "movingWindow"
	case "dbstats":
		cmdName = "dbStats"
	case "createcollection":
		cmdName = "createCollection"
	}

	if originalName != cmdName {
		logger.Printf("CommandHandler.Execute: 标准化命令名称: %s -> %s", originalName, cmdName)
	}

	// 查找命令处理函数
	h.mu.RLock()
	fn, ok := h.commands[cmdName]
	h.mu.RUnlock()

	if !ok {
		// 尝试查找小写版本
		logger.Printf("CommandHandler.Execute: 命令 %s 未找到，尝试查找小写版本", cmdName)
		h.mu.RLock()
		fn, ok = h.commands[strings.ToLower(cmdName)]
		h.mu.RUnlock()

		if !ok {
			logger.Printf("CommandHandler.Execute: 未知命令: %s", cmdName)
			return nil, fmt.Errorf("unknown command: %s", cmdName)
		}
	}

	// 执行命令
	logger.Printf("CommandHandler.Execute: 找到命令处理函数，开始执行: %s", cmdName)
	result, err := fn(ctx, cmd)
	if err != nil {
		logger.Printf("CommandHandler.Execute: 命令执行失败: %v", err)
		return nil, err
	}

	logger.Printf("CommandHandler.Execute: 命令执行成功: %s", cmdName)
	return result, nil
}

// RegisterStandardCommands 注册标准MongoDB命令
func (h *CommandHandler) RegisterStandardCommands() {
	// 服务器信息命令
	h.Register("isMaster", func(ctx context.Context, cmd bson.D) (bson.D, error) {
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

	// hello命令（MongoDB 5.0+中isMaster的替代命令）
	h.Register("hello", func(ctx context.Context, cmd bson.D) (bson.D, error) {
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

	// ping命令
	h.Register("ping", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{{"ok", 1}}, nil
	})

	// buildInfo命令
	h.Register("buildInfo", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"version", "1.0.0"},
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

	// 服务器状态命令
	h.Register("serverStatus", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"host", "localhost"},
			{"version", "1.0.0"},
			{"process", "ChronoGo"},
			{"pid", os.Getpid()},
			{"uptime", getUptime()}, // 使用实际的运行时间
			{"localTime", time.Now().UnixMilli()},
			{"ok", 1},
		}, nil
	})

	// 获取参数命令
	h.Register("getParameter", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		result := bson.D{{"ok", 1}}

		for _, elem := range cmd {
			if elem.Key == "getParameter" || elem.Key == "allParameters" {
				// 添加所有支持的参数
				result = append(result, bson.E{"featureCompatibilityVersion", "5.0"})
				result = append(result, bson.E{"authSchemaVersion", 5})
			} else if elem.Key != "$db" {
				// 添加请求的特定参数
				switch elem.Key {
				case "featureCompatibilityVersion":
					result = append(result, bson.E{"featureCompatibilityVersion", "5.0"})
				case "authSchemaVersion":
					result = append(result, bson.E{"authSchemaVersion", 5})
				}
			}
		}

		return result, nil
	})

	// 获取日志消息命令
	h.Register("getLog", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var logType string

		for _, elem := range cmd {
			if elem.Key == "getLog" {
				if str, ok := elem.Value.(string); ok {
					logType = str
				}
			}
		}

		if logType == "" {
			return nil, fmt.Errorf("getLog requires a log name")
		}

		// 返回空日志
		return bson.D{
			{"log", bson.A{}},
			{"totalLinesWritten", 0},
			{"ok", 1},
		}, nil
	})

	// 列出数据库命令
	h.Register("listDatabases", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"databases", bson.A{}},
			{"totalSize", int64(0)},
			{"totalSizeMb", int64(0)},
			{"ok", 1},
		}, nil
	})

	// 列出集合命令
	h.Register("listCollections", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{
			{"cursor", bson.D{
				{"id", int64(0)},
				{"ns", ""},
				{"firstBatch", bson.A{}},
			}},
			{"ok", 1},
		}, nil
	})

	// 创建集合命令
	h.Register("create", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{{"ok", 1}}, nil
	})

	// 删除集合命令
	h.Register("drop", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{{"ok", 1}}, nil
	})

	// 删除数据库命令
	h.Register("dropDatabase", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		return bson.D{{"ok", 1}}, nil
	})

	// 查找命令
	h.Register("find", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var filter bson.D
		var limit int64 = 0
		var skip int64 = 0
		var batchSize int32 = 101 // 默认批次大小

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "find":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "filter":
				if doc, ok := elem.Value.(bson.D); ok {
					filter = doc
				}
			case "limit":
				if val, ok := elem.Value.(int32); ok {
					limit = int64(val)
				} else if val, ok := elem.Value.(int64); ok {
					limit = val
				}
			case "skip":
				if val, ok := elem.Value.(int32); ok {
					skip = int64(val)
				} else if val, ok := elem.Value.(int64); ok {
					skip = val
				}
			case "batchSize":
				if val, ok := elem.Value.(int32); ok {
					batchSize = val
				} else if val, ok := elem.Value.(int64); ok {
					batchSize = int32(val)
				}
			}
		}

		if dbName == "" || collName == "" {
			return nil, fmt.Errorf("missing database or collection name")
		}

		// 获取会话
		session, ok := ctx.Value("session").(*Session)
		if !ok {
			return nil, fmt.Errorf("session not found in context")
		}

		// 获取查询引擎和存储引擎
		queryEngine, ok := ctx.Value("queryEngine").(*query.QueryEngine)
		if !ok {
			return nil, fmt.Errorf("query engine not found in context")
		}

		// 创建查询解析器
		parser := query.NewQueryParser()

		// 解析查询
		q, err := parser.ParseQuery(dbName, collName, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse query: %w", err)
		}

		// 设置分页参数
		q.Limit = int(limit)
		q.Offset = int(skip)

		// 使用batchSize限制返回的结果数量
		if batchSize > 0 && (limit == 0 || int64(batchSize) < limit) {
			q.Limit = int(batchSize)
		}

		// 执行查询
		result, err := queryEngine.Execute(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}

		// 转换结果为BSON文档
		batch := make([]bson.D, 0, len(result.Points))
		for _, point := range result.Points {
			batch = append(batch, point.ToBSON())
		}

		// 创建游标
		cursor := session.createCursor(dbName, collName, filter, batch)

		// 创建响应
		namespace := dbName + "." + collName
		return createCursorResponse(namespace, cursor.ID, batch), nil
	})

	// 获取更多结果命令
	h.Register("getMore", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var cursorID int64
		var dbName, collName string
		var batchSize int32 = 101 // 默认批次大小

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "getMore":
				if val, ok := elem.Value.(int64); ok {
					cursorID = val
				}
			case "collection":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "batchSize":
				if val, ok := elem.Value.(int32); ok {
					batchSize = val
				} else if val, ok := elem.Value.(int64); ok {
					batchSize = int32(val)
				}
			}
		}

		if dbName == "" || collName == "" || cursorID == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 获取会话
		session, ok := ctx.Value("session").(*Session)
		if !ok {
			return nil, fmt.Errorf("session not found in context")
		}

		// 获取游标
		cursor := session.getCursor(cursorID)
		if cursor == nil {
			return nil, fmt.Errorf("cursor not found: %d", cursorID)
		}

		// 获取下一批数据
		batch := cursor.getNextBatch(int(batchSize))

		// 如果游标已耗尽，则移除
		if cursor.Exhausted {
			session.removeCursor(cursorID)
			cursorID = 0
		}

		// 创建响应
		namespace := dbName + "." + collName
		return createGetMoreResponse(namespace, cursorID, batch), nil
	})

	// 关闭游标命令
	h.Register("killCursors", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var cursorIDs []int64
		var dbName, collName string

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "killCursors":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "cursors":
				if arr, ok := elem.Value.(bson.A); ok {
					for _, val := range arr {
						if id, ok := val.(int64); ok {
							cursorIDs = append(cursorIDs, id)
						}
					}
				}
			}
		}

		if dbName == "" || collName == "" {
			return nil, fmt.Errorf("missing database or collection name")
		}

		// 获取会话
		session, ok := ctx.Value("session").(*Session)
		if !ok {
			return nil, fmt.Errorf("session not found in context")
		}

		// 关闭游标
		killedCursors := bson.A{}
		for _, id := range cursorIDs {
			cursor := session.getCursor(id)
			if cursor != nil {
				session.removeCursor(id)
				killedCursors = append(killedCursors, id)
			}
		}

		// 创建响应
		return bson.D{
			{"cursorsKilled", killedCursors},
			{"cursorsNotFound", bson.A{}},
			{"cursorsAlive", bson.A{}},
			{"cursorsUnknown", bson.A{}},
			{"ok", 1},
		}, nil
	})

	// 插入命令
	h.Register("insert", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var documents bson.A

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "insert":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "documents":
				if docs, ok := elem.Value.(bson.A); ok {
					documents = docs
				}
			}
		}

		if dbName == "" || collName == "" || len(documents) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 获取协议处理器
		wireHandler, ok := ctx.Value("wireHandler").(*WireProtocolHandler)
		if !ok {
			// 如果上下文中没有协议处理器，使用传统方式处理
			return bson.D{
				{"n", len(documents)},
				{"ok", 1},
			}, nil
		}

		// 转换文档格式
		docs := make([]bson.D, 0, len(documents))
		for _, doc := range documents {
			if bsonDoc, ok := doc.(bson.D); ok {
				docs = append(docs, bsonDoc)
			}
		}

		// 创建批处理操作
		op := &batchOperation{
			opType:     "insert",
			database:   dbName,
			collection: collName,
			documents:  docs,
		}

		// 添加到批处理队列
		result := wireHandler.queueBatchOperation(op)
		if result.err != nil {
			return nil, result.err
		}

		return bson.D{
			{"n", result.count},
			{"ok", 1},
		}, nil
	})

	// 更新命令
	h.Register("update", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var updates bson.A

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "update":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "updates":
				if upds, ok := elem.Value.(bson.A); ok {
					updates = upds
				}
			}
		}

		if dbName == "" || collName == "" || len(updates) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 获取协议处理器
		wireHandler, ok := ctx.Value("wireHandler").(*WireProtocolHandler)
		if !ok {
			// 如果上下文中没有协议处理器，使用传统方式处理
			return bson.D{
				{"n", len(updates)},
				{"nModified", 0},
				{"ok", 1},
			}, nil
		}

		// 处理每个更新操作
		nModified := 0
		for _, update := range updates {
			if updateDoc, ok := update.(bson.D); ok {
				// 提取查询条件和更新内容
				var filter, updateOp bson.D

				for _, elem := range updateDoc {
					switch elem.Key {
					case "q": // 查询条件
						if q, ok := elem.Value.(bson.D); ok {
							filter = q
						}
					case "u": // 更新操作
						if u, ok := elem.Value.(bson.D); ok {
							updateOp = u
						}
					}
				}

				// 创建批处理操作
				op := &batchOperation{
					opType:     "update",
					database:   dbName,
					collection: collName,
					filter:     filter,
					update:     updateOp,
				}

				// 添加到批处理队列
				result := wireHandler.queueBatchOperation(op)
				if result.err != nil {
					return nil, result.err
				}

				nModified += result.count
			}
		}

		return bson.D{
			{"n", len(updates)},
			{"nModified", nModified},
			{"ok", 1},
		}, nil
	})

	// 删除命令
	h.Register("delete", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var deletes bson.A

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "delete":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "deletes":
				if dels, ok := elem.Value.(bson.A); ok {
					deletes = dels
				}
			}
		}

		if dbName == "" || collName == "" || len(deletes) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 获取协议处理器
		wireHandler, ok := ctx.Value("wireHandler").(*WireProtocolHandler)
		if !ok {
			// 如果上下文中没有协议处理器，使用传统方式处理
			return bson.D{
				{"n", len(deletes)},
				{"ok", 1},
			}, nil
		}

		// 处理每个删除操作
		nDeleted := 0
		for _, delete := range deletes {
			if deleteDoc, ok := delete.(bson.D); ok {
				// 提取查询条件
				var filter bson.D

				for _, elem := range deleteDoc {
					if elem.Key == "q" { // 查询条件
						if q, ok := elem.Value.(bson.D); ok {
							filter = q
						}
					}
				}

				// 创建批处理操作
				op := &batchOperation{
					opType:     "delete",
					database:   dbName,
					collection: collName,
					filter:     filter,
				}

				// 添加到批处理队列
				result := wireHandler.queueBatchOperation(op)
				if result.err != nil {
					return nil, result.err
				}

				nDeleted += result.count
			}
		}

		return bson.D{
			{"n", nDeleted},
			{"ok", 1},
		}, nil
	})
}

// RegisterTimeSeriesCommands 注册时序特有命令
func (h *CommandHandler) RegisterTimeSeriesCommands() {
	// 时间窗口聚合命令
	h.Register("timeWindow", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var timeField string
		var windowSize string
		var aggregations bson.D
		var filter bson.D
		var startTime, endTime time.Time

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "timeWindow":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "timeField":
				if str, ok := elem.Value.(string); ok {
					timeField = str
				}
			case "windowSize":
				if str, ok := elem.Value.(string); ok {
					windowSize = str
				}
			case "aggregations":
				if agg, ok := elem.Value.(bson.D); ok {
					aggregations = agg
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			case "startTime":
				if t, ok := elem.Value.(time.Time); ok {
					startTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						startTime = t
					}
				}
			case "endTime":
				if t, ok := elem.Value.(time.Time); ok {
					endTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						endTime = t
					}
				}
			}
		}

		if dbName == "" || collName == "" || timeField == "" || windowSize == "" || len(aggregations) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 如果未指定结束时间，使用当前时间
		if endTime.IsZero() {
			endTime = time.Now()
		}

		// 如果未指定开始时间，使用结束时间减去一天
		if startTime.IsZero() {
			startTime = endTime.Add(-24 * time.Hour)
		}

		// 获取查询引擎
		queryEngine, ok := ctx.Value("queryEngine").(*query.QueryEngine)
		if !ok {
			return nil, fmt.Errorf("query engine not found in context")
		}

		// 创建时间窗口聚合请求
		request := map[string]interface{}{
			"database":     dbName,
			"collection":   collName,
			"timeField":    timeField,
			"windowSize":   windowSize,
			"aggregations": aggregations,
			"filter":       filter,
			"startTime":    startTime.UnixNano(),
			"endTime":      endTime.UnixNano(),
		}

		// 执行时间窗口聚合
		result, err := queryEngine.ExecuteTimeWindowAggregation(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("failed to execute time window aggregation: %w", err)
		}

		// 获取会话
		session, ok := ctx.Value("session").(*Session)
		if !ok {
			return nil, fmt.Errorf("session not found in context")
		}

		// 转换结果为BSON文档
		batch := make([]bson.D, 0, len(result))
		for _, item := range result {
			if doc, ok := item.(bson.D); ok {
				batch = append(batch, doc)
			} else if m, ok := item.(map[string]interface{}); ok {
				// 转换map为bson.D
				doc := bson.D{}
				for k, v := range m {
					doc = append(doc, bson.E{Key: k, Value: v})
				}
				batch = append(batch, doc)
			}
		}

		// 创建游标
		cursor := session.createCursor(dbName, collName, nil, batch)

		// 创建响应
		namespace := dbName + "." + collName
		return createCursorResponse(namespace, cursor.ID, batch), nil
	})

	// 降采样命令
	h.Register("downsample", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName, targetColl string
		var timeField string
		var interval string
		var aggregations bson.D
		var filter bson.D
		var startTime, endTime time.Time
		var retention int64

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "downsample":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "targetCollection":
				if str, ok := elem.Value.(string); ok {
					targetColl = str
				}
			case "timeField":
				if str, ok := elem.Value.(string); ok {
					timeField = str
				}
			case "interval":
				if str, ok := elem.Value.(string); ok {
					interval = str
				}
			case "aggregations":
				if agg, ok := elem.Value.(bson.D); ok {
					aggregations = agg
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			case "startTime":
				if t, ok := elem.Value.(time.Time); ok {
					startTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						startTime = t
					}
				}
			case "endTime":
				if t, ok := elem.Value.(time.Time); ok {
					endTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						endTime = t
					}
				}
			case "retention":
				if val, ok := elem.Value.(int64); ok {
					retention = val
				} else if val, ok := elem.Value.(int32); ok {
					retention = int64(val)
				}
			}
		}

		if dbName == "" || collName == "" || timeField == "" || interval == "" || len(aggregations) == 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 如果未指定目标集合，使用源集合名加上降采样间隔
		if targetColl == "" {
			targetColl = collName + "_" + interval
		}

		// 如果未指定结束时间，使用当前时间
		if endTime.IsZero() {
			endTime = time.Now()
		}

		// 如果未指定开始时间，使用结束时间减去30天
		if startTime.IsZero() {
			startTime = endTime.Add(-30 * 24 * time.Hour)
		}

		// 获取查询引擎
		queryEngine, ok := ctx.Value("queryEngine").(*query.QueryEngine)
		if !ok {
			return nil, fmt.Errorf("query engine not found in context")
		}

		// 创建降采样请求
		request := map[string]interface{}{
			"database":         dbName,
			"sourceCollection": collName,
			"targetCollection": targetColl,
			"timeField":        timeField,
			"interval":         interval,
			"aggregations":     aggregations,
			"filter":           filter,
			"startTime":        startTime.UnixNano(),
			"endTime":          endTime.UnixNano(),
			"retention":        retention,
		}

		// 执行降采样
		count, err := queryEngine.ExecuteDownsample(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("failed to execute downsample: %w", err)
		}

		return bson.D{
			{"ok", 1},
			{"sourceCollection", collName},
			{"targetCollection", targetColl},
			{"count", count},
			{"startTime", startTime},
			{"endTime", endTime},
			{"interval", interval},
		}, nil
	})

	// 插值命令
	h.Register("interpolate", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var timeField, valueField string
		var method string
		var interval string
		var filter bson.D
		var startTime, endTime time.Time

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "interpolate":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "timeField":
				if str, ok := elem.Value.(string); ok {
					timeField = str
				}
			case "valueField":
				if str, ok := elem.Value.(string); ok {
					valueField = str
				}
			case "method":
				if str, ok := elem.Value.(string); ok {
					method = str
				}
			case "interval":
				if str, ok := elem.Value.(string); ok {
					interval = str
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			case "startTime":
				if t, ok := elem.Value.(time.Time); ok {
					startTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						startTime = t
					}
				}
			case "endTime":
				if t, ok := elem.Value.(time.Time); ok {
					endTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						endTime = t
					}
				}
			}
		}

		if dbName == "" || collName == "" || timeField == "" || valueField == "" {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 如果未指定插值方法，默认使用线性插值
		if method == "" {
			method = "linear"
		}

		// 如果未指定结束时间，使用当前时间
		if endTime.IsZero() {
			endTime = time.Now()
		}

		// 如果未指定开始时间，使用结束时间减去一天
		if startTime.IsZero() {
			startTime = endTime.Add(-24 * time.Hour)
		}

		// 获取查询引擎
		queryEngine, ok := ctx.Value("queryEngine").(*query.QueryEngine)
		if !ok {
			return nil, fmt.Errorf("query engine not found in context")
		}

		// 创建插值请求
		request := map[string]interface{}{
			"database":   dbName,
			"collection": collName,
			"timeField":  timeField,
			"valueField": valueField,
			"method":     method,
			"interval":   interval,
			"filter":     filter,
			"startTime":  startTime.UnixNano(),
			"endTime":    endTime.UnixNano(),
		}

		// 执行插值
		result, err := queryEngine.ExecuteInterpolation(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("failed to execute interpolation: %w", err)
		}

		// 获取会话
		session, ok := ctx.Value("session").(*Session)
		if !ok {
			return nil, fmt.Errorf("session not found in context")
		}

		// 转换结果为BSON文档
		batch := make([]bson.D, 0, len(result))
		for _, item := range result {
			if doc, ok := item.(bson.D); ok {
				batch = append(batch, doc)
			} else if m, ok := item.(map[string]interface{}); ok {
				// 转换map为bson.D
				doc := bson.D{}
				for k, v := range m {
					doc = append(doc, bson.E{Key: k, Value: v})
				}
				batch = append(batch, doc)
			}
		}

		// 创建游标
		cursor := session.createCursor(dbName, collName, nil, batch)

		// 创建响应
		namespace := dbName + "." + collName
		return createCursorResponse(namespace, cursor.ID, batch), nil
	})

	// 移动窗口命令
	h.Register("movingWindow", func(ctx context.Context, cmd bson.D) (bson.D, error) {
		var dbName, collName string
		var timeField, valueField string
		var windowSize int
		var function string
		var filter bson.D
		var startTime, endTime time.Time

		// 解析命令参数
		for _, elem := range cmd {
			switch elem.Key {
			case "movingWindow":
				if str, ok := elem.Value.(string); ok {
					collName = str
				}
			case "$db":
				if str, ok := elem.Value.(string); ok {
					dbName = str
				}
			case "timeField":
				if str, ok := elem.Value.(string); ok {
					timeField = str
				}
			case "valueField":
				if str, ok := elem.Value.(string); ok {
					valueField = str
				}
			case "windowSize":
				if val, ok := elem.Value.(int32); ok {
					windowSize = int(val)
				} else if val, ok := elem.Value.(int64); ok {
					windowSize = int(val)
				}
			case "function":
				if str, ok := elem.Value.(string); ok {
					function = str
				}
			case "filter":
				if f, ok := elem.Value.(bson.D); ok {
					filter = f
				}
			case "startTime":
				if t, ok := elem.Value.(time.Time); ok {
					startTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						startTime = t
					}
				}
			case "endTime":
				if t, ok := elem.Value.(time.Time); ok {
					endTime = t
				} else if str, ok := elem.Value.(string); ok {
					// 尝试解析时间字符串
					t, err := time.Parse(time.RFC3339, str)
					if err == nil {
						endTime = t
					}
				}
			}
		}

		if dbName == "" || collName == "" || timeField == "" || valueField == "" || windowSize <= 0 {
			return nil, fmt.Errorf("missing required parameters")
		}

		// 如果未指定聚合函数，默认使用平均值
		if function == "" {
			function = "avg"
		}

		// 如果未指定结束时间，使用当前时间
		if endTime.IsZero() {
			endTime = time.Now()
		}

		// 如果未指定开始时间，使用结束时间减去一天
		if startTime.IsZero() {
			startTime = endTime.Add(-24 * time.Hour)
		}

		// 获取查询引擎
		queryEngine, ok := ctx.Value("queryEngine").(*query.QueryEngine)
		if !ok {
			return nil, fmt.Errorf("query engine not found in context")
		}

		// 创建移动窗口请求
		request := map[string]interface{}{
			"database":   dbName,
			"collection": collName,
			"timeField":  timeField,
			"valueField": valueField,
			"windowSize": windowSize,
			"function":   function,
			"filter":     filter,
			"startTime":  startTime.UnixNano(),
			"endTime":    endTime.UnixNano(),
		}

		// 执行移动窗口
		result, err := queryEngine.ExecuteMovingWindow(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("failed to execute moving window: %w", err)
		}

		// 获取会话
		session, ok := ctx.Value("session").(*Session)
		if !ok {
			return nil, fmt.Errorf("session not found in context")
		}

		// 转换结果为BSON文档
		batch := make([]bson.D, 0, len(result))
		for _, item := range result {
			if doc, ok := item.(bson.D); ok {
				batch = append(batch, doc)
			} else if m, ok := item.(map[string]interface{}); ok {
				// 转换map为bson.D
				doc := bson.D{}
				for k, v := range m {
					doc = append(doc, bson.E{Key: k, Value: v})
				}
				batch = append(batch, doc)
			}
		}

		// 创建游标
		cursor := session.createCursor(dbName, collName, nil, batch)

		// 创建响应
		namespace := dbName + "." + collName
		return createCursorResponse(namespace, cursor.ID, batch), nil
	})
}

// sendMessage 发送消息到客户端
func (h *WireProtocolHandler) sendMessage(session *Session, msg *Message) error {
	logger.Printf("sendMessage: 开始发送消息 (RequestID: %d, ResponseTo: %d)", msg.Header.RequestID, msg.Header.ResponseTo)

	// 序列化消息头
	headerBytes := make([]byte, 16)
	binary.LittleEndian.PutUint32(headerBytes[0:], uint32(msg.Header.MessageLength))
	binary.LittleEndian.PutUint32(headerBytes[4:], uint32(msg.Header.RequestID))
	binary.LittleEndian.PutUint32(headerBytes[8:], uint32(msg.Header.ResponseTo))
	binary.LittleEndian.PutUint32(headerBytes[12:], uint32(msg.Header.OpCode))
	logger.Printf("sendMessage: 序列化消息头: 长度=%d, RequestID=%d, ResponseTo=%d, OpCode=%d",
		msg.Header.MessageLength, msg.Header.RequestID, msg.Header.ResponseTo, msg.Header.OpCode)

	// 发送消息头
	logger.Printf("sendMessage: 开始发送消息头")
	_, err := session.Conn.Write(headerBytes)
	if err != nil {
		logger.Printf("sendMessage: 发送消息头失败: %v", err)
		return err
	}
	logger.Printf("sendMessage: 发送消息头成功")

	// 发送消息体
	logger.Printf("sendMessage: 开始发送消息体，大小: %d 字节", len(msg.Body))
	_, err = session.Conn.Write(msg.Body)
	if err != nil {
		logger.Printf("sendMessage: 发送消息体失败: %v", err)
		return err
	}
	logger.Printf("sendMessage: 发送消息体成功")

	logger.Printf("sendMessage: 消息发送完成")
	return nil
}

// createCursor 创建新的游标
func (session *Session) createCursor(database, collection string, query bson.D, batch []bson.D) *Cursor {
	session.mu.Lock()
	defer session.mu.Unlock()

	// 生成新的游标ID
	cursorID := session.nextCursor
	session.nextCursor++

	// 如果批次为空，则设置为已耗尽
	exhausted := len(batch) == 0

	// 创建游标
	cursor := &Cursor{
		ID:         cursorID,
		Query:      query,
		Database:   database,
		Collection: collection,
		Batch:      batch,
		Position:   0,
		Exhausted:  exhausted,
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
	}

	// 如果游标未耗尽，则保存到映射中
	if !exhausted {
		if session.cursorMap == nil {
			session.cursorMap = make(map[int64]*Cursor)
		}
		session.cursorMap[cursorID] = cursor
	}

	return cursor
}

// getCursor 获取游标
func (session *Session) getCursor(cursorID int64) *Cursor {
	session.mu.Lock()
	defer session.mu.Unlock()

	logger.Printf("Session %d: 尝试获取游标 %d", session.ID, cursorID)

	if session.cursorMap == nil {
		logger.Printf("Session %d: 游标映射为空", session.ID)
		return nil
	}

	cursor := session.cursorMap[cursorID]
	if cursor != nil {
		// 更新游标最后使用时间
		cursor.LastUsed = time.Now()
		logger.Printf("Session %d: 找到游标 %d，更新最后使用时间为 %v",
			session.ID, cursorID, cursor.LastUsed.Format(time.RFC3339))
	} else {
		logger.Printf("Session %d: 游标 %d 不存在", session.ID, cursorID)
	}

	return cursor
}

// removeCursor 移除游标
func (session *Session) removeCursor(cursorID int64) {
	session.mu.Lock()
	defer session.mu.Unlock()

	logger.Printf("Session %d: 尝试移除游标 %d", session.ID, cursorID)

	if session.cursorMap != nil {
		if _, exists := session.cursorMap[cursorID]; exists {
			delete(session.cursorMap, cursorID)
			logger.Printf("Session %d: 成功移除游标 %d", session.ID, cursorID)
		} else {
			logger.Printf("Session %d: 游标 %d 不存在，无需移除", session.ID, cursorID)
		}
	} else {
		logger.Printf("Session %d: 游标映射为空，无法移除游标 %d", session.ID, cursorID)
	}
}

// getNextBatch 获取游标的下一批数据
func (cursor *Cursor) getNextBatch(batchSize int) []bson.D {
	if cursor.Exhausted || cursor.Position >= len(cursor.Batch) {
		cursor.Exhausted = true
		return nil
	}

	// 计算批次结束位置
	end := cursor.Position + batchSize
	if end > len(cursor.Batch) {
		end = len(cursor.Batch)
	}

	// 获取批次
	batch := cursor.Batch[cursor.Position:end]

	// 更新位置
	cursor.Position = end

	// 检查是否已耗尽
	if cursor.Position >= len(cursor.Batch) {
		cursor.Exhausted = true
	}

	return batch
}

// createCursorResponse 创建游标响应
func createCursorResponse(namespace string, cursorID int64, batch []bson.D) bson.D {
	return bson.D{
		{"cursor", bson.D{
			{"id", cursorID},
			{"ns", namespace},
			{"firstBatch", batchToBsonA(batch)},
		}},
		{"ok", 1},
	}
}

// createGetMoreResponse 创建getMore响应
func createGetMoreResponse(namespace string, cursorID int64, batch []bson.D) bson.D {
	return bson.D{
		{"cursor", bson.D{
			{"id", cursorID},
			{"ns", namespace},
			{"nextBatch", batchToBsonA(batch)},
		}},
		{"ok", 1},
	}
}

// batchToBsonA 将批次转换为BSON数组
func batchToBsonA(batch []bson.D) bson.A {
	result := make(bson.A, len(batch))
	for i, doc := range batch {
		result[i] = doc
	}
	return result
}

// updateActivity 更新会话活动时间
func (session *Session) updateActivity() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.lastActive = time.Now()
	logger.Printf("Session %d: 更新活动时间为 %v", session.ID, session.lastActive.Format(time.RFC3339))
}

// cleanupCursors 清理过期的游标
func (session *Session) cleanupCursors(maxIdleTime time.Duration) int {
	session.mu.Lock()
	defer session.mu.Unlock()

	now := time.Now()
	count := 0

	for id, cursor := range session.cursorMap {
		// 如果游标已经超时，则删除
		if now.Sub(cursor.LastUsed) > maxIdleTime {
			delete(session.cursorMap, id)
			count++
		}
	}

	return count
}

// cleanupLoop 清理过期的游标和会话
func (h *WireProtocolHandler) cleanupLoop() {
	for {
		select {
		case <-h.cleanupTicker.C:
			h.cleanupCursorsAndSessions()
		case <-h.closing:
			h.cleanupTicker.Stop()
			return
		}
	}
}

// cleanupCursorsAndSessions 清理过期的游标和会话
func (h *WireProtocolHandler) cleanupCursorsAndSessions() {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	sessionTimeout := 30 * time.Minute // 会话超时时间

	// 清理过期的会话和游标
	for id, session := range h.sessions {
		// 清理过期的游标
		count := session.cleanupCursors(h.cursorTTL)
		if count > 0 {
			logger.Printf("Cleaned up %d expired cursors for session %d", count, id)
		}

		// 如果会话已经超时，则关闭并删除
		if now.Sub(session.lastActive) > sessionTimeout {
			logger.Printf("Closing inactive session %d", id)
			session.Conn.Close()
			delete(h.sessions, id)
		}
	}
}

// batchWorker 批处理工作线程
func (h *WireProtocolHandler) batchWorker() {
	// 按操作类型和集合分组的批处理操作
	insertBatches := make(map[string][]*batchOperation)
	updateBatches := make(map[string][]*batchOperation)
	deleteBatches := make(map[string][]*batchOperation)

	// 批处理定时器
	ticker := time.NewTicker(h.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			// 处理器关闭，退出工作线程
			return

		case op := <-h.batchQueue:
			// 将操作添加到相应的批处理组
			key := op.database + "." + op.collection

			switch op.opType {
			case "insert":
				insertBatches[key] = append(insertBatches[key], op)
				// 如果批处理达到大小限制，立即处理
				if len(insertBatches[key]) >= h.batchSize {
					h.processBatch("insert", key, insertBatches[key])
					insertBatches[key] = nil
				}

			case "update":
				updateBatches[key] = append(updateBatches[key], op)
				// 如果批处理达到大小限制，立即处理
				if len(updateBatches[key]) >= h.batchSize {
					h.processBatch("update", key, updateBatches[key])
					updateBatches[key] = nil
				}

			case "delete":
				deleteBatches[key] = append(deleteBatches[key], op)
				// 如果批处理达到大小限制，立即处理
				if len(deleteBatches[key]) >= h.batchSize {
					h.processBatch("delete", key, deleteBatches[key])
					deleteBatches[key] = nil
				}
			}

		case <-ticker.C:
			// 定时处理所有批处理
			for key, batch := range insertBatches {
				if len(batch) > 0 {
					h.processBatch("insert", key, batch)
					insertBatches[key] = nil
				}
			}

			for key, batch := range updateBatches {
				if len(batch) > 0 {
					h.processBatch("update", key, batch)
					updateBatches[key] = nil
				}
			}

			for key, batch := range deleteBatches {
				if len(batch) > 0 {
					h.processBatch("delete", key, batch)
					deleteBatches[key] = nil
				}
			}
		}
	}
}

// processBatch 处理批处理
func (h *WireProtocolHandler) processBatch(opType, key string, batch []*batchOperation) {
	if len(batch) == 0 {
		return
	}

	// 解析数据库和集合
	parts := strings.Split(key, ".")
	if len(parts) != 2 {
		// 无效的键，返回错误
		for _, op := range batch {
			op.resultCh <- batchResult{
				count: 0,
				err:   fmt.Errorf("invalid key: %s", key),
			}
		}
		return
	}

	dbName, collName := parts[0], parts[1]

	// 根据操作类型处理批处理
	switch opType {
	case "insert":
		// 合并所有文档
		allDocs := make([]bson.D, 0, len(batch)*10) // 假设每个操作平均有10个文档
		for _, op := range batch {
			allDocs = append(allDocs, op.documents...)
		}

		// 执行批量插入
		ctx := context.Background()
		count, err := h.executeBulkInsert(ctx, dbName, collName, allDocs)

		// 返回结果
		for _, op := range batch {
			op.resultCh <- batchResult{
				count: count / len(batch), // 平均分配影响的文档数
				err:   err,
			}
		}

	case "update":
		// 暂时不支持批量更新，逐个处理
		for _, op := range batch {
			ctx := context.Background()
			count, err := h.executeBulkUpdate(ctx, dbName, collName, op.filter, op.update)

			op.resultCh <- batchResult{
				count: count,
				err:   err,
			}
		}

	case "delete":
		// 暂时不支持批量删除，逐个处理
		for _, op := range batch {
			ctx := context.Background()
			count, err := h.executeBulkDelete(ctx, dbName, collName, op.filter)

			op.resultCh <- batchResult{
				count: count,
				err:   err,
			}
		}
	}
}

// executeBulkInsert 执行批量插入
func (h *WireProtocolHandler) executeBulkInsert(ctx context.Context, dbName, collName string, documents []bson.D) (int, error) {
	// 创建插入命令
	cmd := bson.D{
		{"insert", collName},
		{"$db", dbName},
		{"documents", documents},
	}

	// 执行命令
	result, err := h.cmdHandler.Execute(ctx, cmd)
	if err != nil {
		return 0, err
	}

	// 解析结果
	for _, elem := range result {
		if elem.Key == "n" {
			if n, ok := elem.Value.(int); ok {
				return n, nil
			}
		}
	}

	return len(documents), nil // 假设所有文档都插入成功
}

// executeBulkUpdate 执行批量更新
func (h *WireProtocolHandler) executeBulkUpdate(ctx context.Context, dbName, collName string, filter, update bson.D) (int, error) {
	// 创建更新命令
	cmd := bson.D{
		{"update", collName},
		{"$db", dbName},
		{"updates", bson.A{
			bson.D{
				{"q", filter},
				{"u", update},
				{"multi", true},
			},
		}},
	}

	// 执行命令
	result, err := h.cmdHandler.Execute(ctx, cmd)
	if err != nil {
		return 0, err
	}

	// 解析结果
	for _, elem := range result {
		if elem.Key == "n" {
			if n, ok := elem.Value.(int); ok {
				return n, nil
			}
		}
	}

	return 0, nil
}

// executeBulkDelete 执行批量删除
func (h *WireProtocolHandler) executeBulkDelete(ctx context.Context, dbName, collName string, filter bson.D) (int, error) {
	// 创建删除命令
	cmd := bson.D{
		{"delete", collName},
		{"$db", dbName},
		{"deletes", bson.A{
			bson.D{
				{"q", filter},
				{"limit", 0}, // 0表示删除所有匹配的文档
			},
		}},
	}

	// 执行命令
	result, err := h.cmdHandler.Execute(ctx, cmd)
	if err != nil {
		return 0, err
	}

	// 解析结果
	for _, elem := range result {
		if elem.Key == "n" {
			if n, ok := elem.Value.(int); ok {
				return n, nil
			}
		}
	}

	return 0, nil
}

// queueBatchOperation 将操作添加到批处理队列
func (h *WireProtocolHandler) queueBatchOperation(op *batchOperation) batchResult {
	// 创建结果通道
	op.resultCh = make(chan batchResult, 1)

	// 将操作添加到队列
	select {
	case h.batchQueue <- op:
		// 操作已添加到队列，等待结果
		result := <-op.resultCh
		return result

	case <-time.After(1 * time.Second):
		// 队列已满或超时，直接处理
		ctx := context.Background()

		switch op.opType {
		case "insert":
			count, err := h.executeBulkInsert(ctx, op.database, op.collection, op.documents)
			return batchResult{count: count, err: err}

		case "update":
			count, err := h.executeBulkUpdate(ctx, op.database, op.collection, op.filter, op.update)
			return batchResult{count: count, err: err}

		case "delete":
			count, err := h.executeBulkDelete(ctx, op.database, op.collection, op.filter)
			return batchResult{count: count, err: err}

		default:
			return batchResult{count: 0, err: fmt.Errorf("unknown operation type: %s", op.opType)}
		}
	}
}

// 在 WireProtocolHandler 中添加一个静态变量来存储启动时间
var serverStartTime = time.Now()

// 获取服务器运行时间（秒）
func getUptime() int64 {
	return int64(time.Since(serverStartTime).Seconds())
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
