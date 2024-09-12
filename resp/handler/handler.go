package handler

import (
	"RediGo/database"
	databaseface "RediGo/interface/database"
	"RediGo/lib/logger"
	"RediGo/lib/sync/atomic"
	"RediGo/resp/connection"
	"RediGo/resp/parser"
	"RediGo/resp/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n") // 未知错误的默认回复
	// Default reply for unknown errors
)

// RespHandler 实现了 tcp.Handler，用作 Redis 请求的处理器
// RespHandler implements the tcp.Handler interface and acts as a handler for Redis requests
type RespHandler struct {
	activeConn sync.Map // 活跃连接的存储，key 是客户端，value 是占位符
	// Stores active connections, with client as key and placeholder as value
	db databaseface.Database // Redis 数据库实例
	// Redis database instance
	closing atomic.Boolean // 标志是否拒绝新的连接和请求
	// Indicator for whether to refuse new connections and requests
}

// MakeHandler 创建一个 RespHandler 实例
// MakeHandler creates and returns a new RespHandler instance
func MakeHandler() *RespHandler {
	var db databaseface.Database
	db = database.NewEchoDatabase() // 使用 Echo 数据库作为存储引擎
	// Use Echo database as the storage engine
	return &RespHandler{
		db: db,
	}
}

// closeClient 函数优雅地关闭客户端连接并清理相关资源
// The closeClient function gracefully closes the client connection and cleans up associated resources
func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()            // 关闭客户端连接
	h.db.AfterClientClose(client) // 通知数据库客户端已关闭
	h.activeConn.Delete(client)   // 从活跃连接中移除客户端
}

// Handle 方法接收并执行 Redis 命令
// The Handle method receives Redis commands and executes them
func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// 如果处理器正在关闭，则拒绝新的连接
		// If the handler is closing, refuse new connections
		_ = conn.Close()
	}

	client := connection.NewConn(conn) // 创建新的客户端连接
	h.activeConn.Store(client, 1)      // 将客户端存储在活跃连接中

	ch := parser.ParseStream(conn) // 开始解析 Redis 流命令
	for payload := range ch {
		if payload.Err != nil {
			// 处理错误情况，例如连接关闭或协议错误
			// Handle error cases such as connection closure or protocol errors
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 如果连接关闭，执行相应的清理操作
				// If the connection is closed, perform the necessary cleanup
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// 协议错误，向客户端返回错误信息
			// Protocol error, send error message to the client
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				// 如果写入失败，关闭客户端连接
				// If writing fails, close the client connection
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			// 处理空 payload 的情况
			// Handle case where the payload is empty
			logger.Error("empty payload")
			continue
		}
		// 确认 payload 数据是一个多块回复
		// Ensure that the payload data is a multi-bulk reply
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		// 执行数据库操作，并获取结果
		// Execute the database command and get the result
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes()) // 将结果写回客户端
		} else {
			_ = client.Write(unknownErrReplyBytes) // 如果没有结果，返回默认错误信息
		}
	}
}

// Close 方法用于停止处理器
// The Close method is used to stop the handler
func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...") // 日志记录关闭操作
	h.closing.Set(true)                     // 标志设置为关闭状态
	// TODO: 处理并发等待
	// TODO: Handle concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		// 关闭所有活跃的客户端连接
		// Close all active client connections
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close() // 关闭数据库
	return nil
}
