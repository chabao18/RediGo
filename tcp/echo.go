package tcp

import (
	"RediGo/lib/logger"
	"RediGo/lib/sync/atomic"
	"RediGo/lib/sync/wait"
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"
)

// EchoClient 代表一个客户端连接
// EchoClient represents a client connection
type EchoClient struct {
	Conn    net.Conn  // 客户端的网络连接 // The network connection for the client
	Waiting wait.Wait // 用于控制连接关闭的同步 // Used to synchronize connection closure
}

// Close 关闭客户端连接
// Close closes the client connection
func (client *EchoClient) Close() error {
	// 等待正在进行的操作完成
	// Wait for ongoing operations to complete
	client.Waiting.WaitWithTimeout(10 * time.Second)
	_ = client.Conn.Close()
	return nil
}

// EchoHandler 处理回显服务器的连接
// EchoHandler manages connections for the echo server
type EchoHandler struct {
	activeConn sync.Map       // 活跃的客户端连接 // Active client connections
	closing    atomic.Boolean // 标识处理器是否正在关闭 // Indicates whether the handler is closing
}

// MakeHandler 创建一个新的 EchoHandler 实例
// MakeHandler creates a new EchoHandler instance
func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

// Handle 处理单个客户端连接
// Handle manages a single client connection
func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 如果处理器正在关闭，立即关闭连接
	// If the handler is closing, close the connection immediately
	if handler.closing.Get() {
		_ = conn.Close()
	}

	// 创建并存储新的客户端连接
	// Create and store a new client connection
	client := &EchoClient{Conn: conn}
	handler.activeConn.Store(client, struct{}{})

	// 使用缓冲读取器读取客户端数据
	// Use a buffered reader to read data from the client
	reader := bufio.NewReader(conn)
	for {
		// 读取客户端消息
		// Read a message from the client
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// 客户端主动关闭连接
				// Client closed the connection
				logger.Info("Connection closed")
				handler.activeConn.Delete(client)
			} else {
				// 其他错误，记录日志
				// Log other errors
				logger.Warn(err)
			}
			return
		}

		// 处理客户端请求并回显消息
		// Process client request and echo back the message
		client.Waiting.Add(1)
		conn.Write([]byte(msg))
		client.Waiting.Done()
	}
}

// Close 关闭所有活跃的客户端连接
// Close closes all active client connections
func (handler *EchoHandler) Close() error {
	logger.Info("Handler shutting down")
	handler.closing.Set(true) // 设置关闭标志 // Set the closing flag
	handler.activeConn.Range(func(key, value any) bool {
		client := key.(*EchoClient)
		client.Conn.Close()
		return true
	})
	return nil
}
