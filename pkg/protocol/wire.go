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
	listener    net.Listener
	sessions    map[int32]*Session
	nextSession int32
	cmdHandler  *CommandHandler
	mu          sync.RWMutex
	closing     chan struct{}
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
	// TODO: 实现查询操作处理
	return nil
}

// handleOpGetMore 处理获取更多结果操作
func (h *WireProtocolHandler) handleOpGetMore(session *Session, msg *Message) error {
	// TODO: 实现获取更多结果操作处理
	return nil
}

// handleOpKillCursors 处理关闭游标操作
func (h *WireProtocolHandler) handleOpKillCursors(session *Session, msg *Message) error {
	// TODO: 实现关闭游标操作处理
	return nil
}

// handleOpInsert 处理插入操作
func (h *WireProtocolHandler) handleOpInsert(session *Session, msg *Message) error {
	// TODO: 实现插入操作处理
	return nil
}

// handleOpUpdate 处理更新操作
func (h *WireProtocolHandler) handleOpUpdate(session *Session, msg *Message) error {
	// TODO: 实现更新操作处理
	return nil
}

// handleOpDelete 处理删除操作
func (h *WireProtocolHandler) handleOpDelete(session *Session, msg *Message) error {
	// TODO: 实现删除操作处理
	return nil
}

// handleOpMsg 处理消息操作
func (h *WireProtocolHandler) handleOpMsg(session *Session, msg *Message) error {
	// TODO: 实现消息操作处理
	return nil
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
