package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
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
}

// WireProtocolHandler 表示MongoDB协议处理器
type WireProtocolHandler struct {
	listener      net.Listener
	sessions      map[int32]*Session
	nextSession   int32
	nextRequestID int32 // 添加请求ID计数器
	cmdHandler    *CommandHandler
	mu            sync.RWMutex
	closing       chan struct{}
}

// NewWireProtocolHandler 创建新的协议处理器
func NewWireProtocolHandler(addr string, cmdHandler *CommandHandler) (*WireProtocolHandler, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	return &WireProtocolHandler{
		listener:   listener,
		sessions:   make(map[int32]*Session),
		cmdHandler: cmdHandler,
		closing:    make(chan struct{}),
	}, nil
}

// Start 启动协议处理器
func (h *WireProtocolHandler) Start() {
	go h.acceptLoop()
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
		session.Conn.Close()
		h.mu.Lock()
		delete(h.sessions, session.ID)
		h.mu.Unlock()
	}()

	for {
		// 读取消息头
		headerBytes := make([]byte, 16)
		if _, err := io.ReadFull(session.Conn, headerBytes); err != nil {
			if err != io.EOF {
				// 记录错误
			}
			return
		}

		// 解析消息头
		header := MsgHeader{
			MessageLength: int32(binary.LittleEndian.Uint32(headerBytes[0:4])),
			RequestID:     int32(binary.LittleEndian.Uint32(headerBytes[4:8])),
			ResponseTo:    int32(binary.LittleEndian.Uint32(headerBytes[8:12])),
			OpCode:        int32(binary.LittleEndian.Uint32(headerBytes[12:16])),
		}

		// 读取消息体
		bodyLength := header.MessageLength - 16
		if bodyLength <= 0 {
			// 无效的消息长度
			return
		}

		bodyBytes := make([]byte, bodyLength)
		if _, err := io.ReadFull(session.Conn, bodyBytes); err != nil {
			// 记录错误
			return
		}

		// 处理消息
		msg := &Message{
			Header: header,
			Body:   bodyBytes,
		}

		// 处理消息并发送响应
		if err := h.handleMessage(session, msg); err != nil {
			// 记录错误
			return
		}
	}
}

// handleMessage 处理单个消息
func (h *WireProtocolHandler) handleMessage(session *Session, msg *Message) error {
	switch msg.Header.OpCode {
	case OpQuery:
		return h.handleOpQuery(session, msg)
	case OpGetMore:
		return h.handleOpGetMore(session, msg)
	case OpKillCursors:
		return h.handleOpKillCursors(session, msg)
	case OpInsert:
		return h.handleOpInsert(session, msg)
	case OpUpdate:
		return h.handleOpUpdate(session, msg)
	case OpDelete:
		return h.handleOpDelete(session, msg)
	case OpMsg:
		return h.handleOpMsg(session, msg)
	default:
		// 不支持的操作码
		return fmt.Errorf("unsupported opcode: %d", msg.Header.OpCode)
	}
}

// handleOpQuery 处理查询操作
func (h *WireProtocolHandler) handleOpQuery(session *Session, msg *Message) error {
	// 解析查询消息
	if len(msg.Body) < 4 {
		return fmt.Errorf("invalid query message")
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
	dbName, colName := parts[0], parts[1]

	// 跳过numberToSkip(4字节)和numberToReturn(4字节)
	offset += 8

	// 解析查询文档
	var query bson.D
	err = bson.Unmarshal(msg.Body[offset:], &query)
	if err != nil {
		return err
	}

	// 处理特殊命令集合
	if colName == "$cmd" {
		// 执行命令
		ctx := context.Background()
		result, err := h.cmdHandler.Execute(ctx, query)
		if err != nil {
			// 发送错误响应
			errorDoc := bson.D{
				{"ok", 0},
				{"errmsg", err.Error()},
				{"code", 1},
			}
			return h.sendReply(session, msg.Header.RequestID, []bson.D{errorDoc}, 0, 0)
		}

		// 发送命令结果
		return h.sendReply(session, msg.Header.RequestID, []bson.D{result}, 0, 0)
	}

	// 普通查询
	// 创建查询上下文
	ctx := context.Background()

	// 执行find命令
	findCmd := bson.D{
		{"find", colName},
		{"filter", query},
	}

	result, err := h.cmdHandler.Execute(ctx, findCmd)
	if err != nil {
		// 发送错误响应
		errorDoc := bson.D{
			{"ok", 0},
			{"errmsg", err.Error()},
			{"code", 1},
		}
		return h.sendReply(session, msg.Header.RequestID, []bson.D{errorDoc}, 0, 0)
	}

	// 从结果中提取游标信息
	cursorDoc, err := getBsonValue(result, "cursor")
	if err != nil {
		return err
	}

	cursorMap, ok := cursorDoc.(bson.D)
	if !ok {
		return fmt.Errorf("invalid cursor format")
	}

	// 提取第一批结果
	firstBatch, err := getBsonValue(cursorMap, "firstBatch")
	if err != nil {
		return err
	}

	batchArray, ok := firstBatch.(bson.A)
	if !ok {
		return fmt.Errorf("invalid firstBatch format")
	}

	// 转换为bson.D数组
	documents := make([]bson.D, 0, len(batchArray))
	for _, item := range batchArray {
		if doc, ok := item.(bson.D); ok {
			documents = append(documents, doc)
		}
	}

	// 提取游标ID
	cursorIDValue, err := getBsonValue(cursorMap, "id")
	if err != nil {
		return err
	}

	var cursorID int64
	switch v := cursorIDValue.(type) {
	case int64:
		cursorID = v
	case int32:
		cursorID = int64(v)
	case float64:
		cursorID = int64(v)
	default:
		cursorID = 0
	}

	// 如果有游标ID，保存游标信息
	if cursorID != 0 {
		cursor := &Cursor{
			ID:         cursorID,
			Query:      query,
			Database:   dbName,
			Collection: colName,
			Batch:      documents,
			Position:   0,
			Exhausted:  false,
		}
		session.mu.Lock()
		session.cursorMap[cursorID] = cursor
		session.mu.Unlock()
	}

	// 发送查询结果
	return h.sendReply(session, msg.Header.RequestID, documents, 0, cursorID)
}

// handleOpGetMore 处理获取更多结果操作
func (h *WireProtocolHandler) handleOpGetMore(session *Session, msg *Message) error {
	// 解析GetMore消息
	if len(msg.Body) < 16 {
		return fmt.Errorf("invalid getMore message")
	}

	// 跳过numberToReturn(4字节)
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
	dbName, colName := parts[0], parts[1]

	// 读取游标ID
	cursorID := int64(binary.LittleEndian.Uint64(msg.Body[offset:]))

	// 查找游标
	session.mu.Lock()
	cursor, ok := session.cursorMap[cursorID]
	session.mu.Unlock()

	if !ok {
		// 游标不存在
		return h.sendReply(session, msg.Header.RequestID, nil, CursorNotFound, 0)
	}

	// 验证游标所属的数据库和集合
	if cursor.Database != dbName || cursor.Collection != colName {
		return fmt.Errorf("cursor belongs to different namespace")
	}

	// 如果游标已耗尽，返回空结果
	if cursor.Exhausted {
		return h.sendReply(session, msg.Header.RequestID, nil, Exhausted, 0)
	}

	// 创建查询上下文
	ctx := context.Background()

	// 执行getMore命令
	getMoreCmd := bson.D{
		{"getMore", cursorID},
		{"collection", colName},
	}

	result, err := h.cmdHandler.Execute(ctx, getMoreCmd)
	if err != nil {
		// 发送错误响应
		errorDoc := bson.D{
			{"ok", 0},
			{"errmsg", err.Error()},
			{"code", 1},
		}
		return h.sendReply(session, msg.Header.RequestID, []bson.D{errorDoc}, 0, 0)
	}

	// 从结果中提取游标信息
	cursorDoc, err := getBsonValue(result, "cursor")
	if err != nil {
		return err
	}

	cursorMap, ok := cursorDoc.(bson.D)
	if !ok {
		return fmt.Errorf("invalid cursor format")
	}

	// 提取下一批结果
	nextBatch, err := getBsonValue(cursorMap, "nextBatch")
	if err != nil {
		return err
	}

	batchArray, ok := nextBatch.(bson.A)
	if !ok {
		return fmt.Errorf("invalid nextBatch format")
	}

	// 转换为bson.D数组
	documents := make([]bson.D, 0, len(batchArray))
	for _, item := range batchArray {
		if doc, ok := item.(bson.D); ok {
			documents = append(documents, doc)
		}
	}

	// 提取游标ID
	newCursorIDValue, err := getBsonValue(cursorMap, "id")
	if err != nil {
		return err
	}

	var newCursorID int64
	switch v := newCursorIDValue.(type) {
	case int64:
		newCursorID = v
	case int32:
		newCursorID = int64(v)
	case float64:
		newCursorID = int64(v)
	default:
		newCursorID = 0
	}

	// 更新游标状态
	if newCursorID == 0 {
		// 游标已耗尽
		cursor.Exhausted = true
		session.mu.Lock()
		delete(session.cursorMap, cursorID)
		session.mu.Unlock()
	} else {
		// 更新游标批次
		cursor.Batch = documents
		cursor.Position = 0
	}

	// 发送查询结果
	return h.sendReply(session, msg.Header.RequestID, documents, 0, newCursorID)
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
	_, colName := parts[0], parts[1] // 只使用集合名称

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
	}

	_, err = h.cmdHandler.Execute(ctx, insertCmd) // 忽略结果
	if err != nil {
		// 记录错误，但不发送响应
		// 因为旧版MongoDB协议中，insert操作不返回响应
		fmt.Printf("Insert error: %v\n", err)
		return nil
	}

	// Insert操作在旧版协议中不需要响应
	return nil
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
		fmt.Printf("Update error: %v\n", err)
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
		fmt.Printf("Delete error: %v\n", err)
		return nil
	}

	// Delete操作在旧版协议中不需要响应
	return nil
}

// handleOpMsg 处理消息操作(MongoDB 3.6+)
func (h *WireProtocolHandler) handleOpMsg(session *Session, msg *Message) error {
	// 解析OpMsg消息
	if len(msg.Body) < 4 {
		return fmt.Errorf("invalid opMsg message")
	}

	// 读取标志位
	flags := binary.LittleEndian.Uint32(msg.Body[0:4])
	offset := 4

	// 检查消息格式
	if (flags & 1) != 0 {
		// 校验和存在，暂不支持
		return fmt.Errorf("checksum not supported")
	}

	// 读取第一个部分
	if offset >= len(msg.Body) {
		return fmt.Errorf("invalid opMsg message")
	}

	// 检查部分类型
	partType := msg.Body[offset]
	offset++

	if partType != 0 {
		return fmt.Errorf("unsupported section type: %d", partType)
	}

	// 读取文档
	var doc bson.D
	err := bson.Unmarshal(msg.Body[offset:], &doc)
	if err != nil {
		return err
	}

	// 创建查询上下文
	ctx := context.Background()

	// 执行命令
	result, err := h.cmdHandler.Execute(ctx, doc)
	if err != nil {
		// 创建错误响应
		errorDoc := bson.D{
			{"ok", 0},
			{"errmsg", err.Error()},
			{"code", 1},
		}

		// 发送错误响应
		return h.sendOpMsg(session, msg.Header.RequestID, errorDoc)
	}

	// 发送命令结果
	return h.sendOpMsg(session, msg.Header.RequestID, result)
}

// sendOpMsg 发送OpMsg响应
func (h *WireProtocolHandler) sendOpMsg(session *Session, responseTo int32, document bson.D) error {
	// 序列化文档
	docBytes, err := bson.Marshal(document)
	if err != nil {
		return err
	}

	// 计算消息长度
	// 消息头(16) + 标志位(4) + 部分类型(1) + 文档长度
	msgLen := 16 + 4 + 1 + len(docBytes)

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

	// 写入标志位(0)
	binary.LittleEndian.PutUint32(response.Body[0:4], 0)

	// 写入部分类型(0)
	response.Body[4] = 0

	// 写入文档
	copy(response.Body[5:], docBytes)

	// 发送响应
	return h.sendMessage(session, &response)
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
func getBsonValue(doc bson.D, key string) (interface{}, error) {
	for _, elem := range doc {
		if elem.Key == key {
			return elem.Value, nil
		}
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

// sendReply 发送响应
func (h *WireProtocolHandler) sendReply(session *Session, responseTo int32, documents []bson.D, flags int32, cursorID int64) error {
	// 序列化文档
	docBytes := make([]byte, 0)
	for _, doc := range documents {
		b, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}
		docBytes = append(docBytes, b...)
	}

	// 计算消息长度
	messageLength := 16 + 20 + len(docBytes)

	// 创建响应头
	header := MsgHeader{
		MessageLength: int32(messageLength),
		RequestID:     atomic.AddInt32(&h.nextSession, 1),
		ResponseTo:    responseTo,
		OpCode:        OpReply,
	}

	// 创建响应体
	body := make([]byte, 20+len(docBytes))
	binary.LittleEndian.PutUint32(body[0:4], uint32(flags))
	binary.LittleEndian.PutUint64(body[4:12], uint64(cursorID))
	binary.LittleEndian.PutUint32(body[12:16], uint32(0)) // 起始位置
	binary.LittleEndian.PutUint32(body[16:20], uint32(len(documents)))
	copy(body[20:], docBytes)

	// 发送响应
	headerBytes := make([]byte, 16)
	binary.LittleEndian.PutUint32(headerBytes[0:4], uint32(header.MessageLength))
	binary.LittleEndian.PutUint32(headerBytes[4:8], uint32(header.RequestID))
	binary.LittleEndian.PutUint32(headerBytes[8:12], uint32(header.ResponseTo))
	binary.LittleEndian.PutUint32(headerBytes[12:16], uint32(header.OpCode))

	session.mu.Lock()
	defer session.mu.Unlock()

	if _, err := session.Conn.Write(headerBytes); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if _, err := session.Conn.Write(body); err != nil {
		return fmt.Errorf("failed to write body: %w", err)
	}

	return nil
}

// Close 关闭协议处理器
func (h *WireProtocolHandler) Close() error {
	close(h.closing)
	return h.listener.Close()
}

// CommandHandler 表示命令处理器
type CommandHandler struct {
	registry map[string]CommandFunc
	mu       sync.RWMutex
}

// CommandFunc 表示命令处理函数
type CommandFunc func(ctx context.Context, cmd bson.D) (bson.D, error)

// NewCommandHandler 创建新的命令处理器
func NewCommandHandler() *CommandHandler {
	return &CommandHandler{
		registry: make(map[string]CommandFunc),
	}
}

// Register 注册命令处理函数
func (h *CommandHandler) Register(name string, fn CommandFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.registry[name] = fn
}

// Execute 执行命令
func (h *CommandHandler) Execute(ctx context.Context, cmd bson.D) (bson.D, error) {
	if len(cmd) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	cmdName := cmd[0].Key

	h.mu.RLock()
	fn, exists := h.registry[cmdName]
	h.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown command: %s", cmdName)
	}

	return fn(ctx, cmd)
}

// RegisterStandardCommands 注册标准MongoDB命令
func (h *CommandHandler) RegisterStandardCommands() {
	// TODO: 实现标准命令注册
}

// RegisterTimeSeriesCommands 注册时序特有命令
func (h *CommandHandler) RegisterTimeSeriesCommands() {
	// TODO: 实现时序特有命令注册
}

// sendMessage 发送消息到客户端
func (h *WireProtocolHandler) sendMessage(session *Session, msg *Message) error {
	// 序列化消息头
	headerBytes := make([]byte, 16)
	binary.LittleEndian.PutUint32(headerBytes[0:], uint32(msg.Header.MessageLength))
	binary.LittleEndian.PutUint32(headerBytes[4:], uint32(msg.Header.RequestID))
	binary.LittleEndian.PutUint32(headerBytes[8:], uint32(msg.Header.ResponseTo))
	binary.LittleEndian.PutUint32(headerBytes[12:], uint32(msg.Header.OpCode))

	// 发送消息头
	_, err := session.Conn.Write(headerBytes)
	if err != nil {
		return err
	}

	// 发送消息体
	_, err = session.Conn.Write(msg.Body)
	if err != nil {
		return err
	}

	return nil
}
