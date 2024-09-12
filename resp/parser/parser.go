package parser

import (
	"RediGo/interface/resp"
	"RediGo/lib/logger"
	"RediGo/resp/reply"
	"bufio"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload 结构体用于存储 Redis 的回复或错误
// The Payload struct is used to store Redis replies or errors
type Payload struct {
	Data resp.Reply // 存储解析出的 Redis 回复
	Err  error      // 存储解析过程中产生的错误
}

// ParseStream 函数从 io.Reader 中读取数据，并通过通道发送解析后的 Payload
// The ParseStream function reads data from io.Reader and sends parsed payloads through a channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload) // 创建用于传输 Payload 的通道
	go parse0(reader, ch)     // 启动 goroutine 解析数据
	return ch                 // 返回通道
}

type readState struct {
	readingMultiLine  bool     // 标志当前是否在读取多行数据
	expectedArgsCount int      // 预期参数数量
	msgType           byte     // 消息类型
	args              [][]byte // 存储解析出的参数
	bulkLen           int64    // 当前块的长度（仅用于 bulk 类型）
}

// finished 函数检查当前的 readState 是否完成了所有参数的读取
// The finished function checks if the current readState has completed reading all expected arguments
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// parse0 是核心解析函数，读取并解释 RESP 命令
// The parse0 function is the core parsing logic that reads and interprets RESP commands
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		// Recover from panics and log stack trace in case of an unexpected error
		// 处理可能出现的异常，并记录堆栈信息
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader) // 使用 bufio 提高读取性能
	var state readState                  // 初始化读取状态
	var err error
	var msg []byte
	for {
		// Read a line from the buffer
		// 从缓冲区读取一行
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr { // Handle I/O error and stop reading
				// 处理 I/O 错误，停止读取
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// Handle protocol error, reset state
			// 处理协议错误，重置状态
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// Parse the received line
		// 解析接收到的行
		if !state.readingMultiLine {
			// Start a new message
			// 开始处理新消息
			if msg[0] == '*' {
				// Multi-bulk reply
				// 多行回复
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // Reset state
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				// Bulk reply
				// 块回复
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // Reset state
					continue
				}
				if state.bulkLen == -1 {
					// Null bulk reply
					// 空块回复
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				// Single-line reply
				// 单行回复
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			// Continue parsing multi-bulk or bulk replies
			// 继续解析多行或块回复
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // Reset state
				continue
			}
			// Check if message parsing is complete
			// 检查消息解析是否完成
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// readLine 从缓冲区读取单行或块数据
// The readLine function reads a single line or bulk data from the buffer
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 { // Read normal line
		// 读取普通行
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // Read bulk line (binary-safe)
		// 读取块行（支持二进制数据）
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// parseMultiBulkHeader 解析多行回复的头部信息
// The parseMultiBulkHeader function parses the header of a multi-bulk reply
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// Initialize multi-bulk parsing state
		// 初始化多行解析状态
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseBulkHeader 解析块回复的头部信息
// The parseBulkHeader function parses the header of a bulk reply
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // Null bulk
		// 空块回复
		return nil
	} else if state.bulkLen > 0 {
		// Initialize bulk parsing state
		// 初始化块解析状态
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseSingleLineReply 解析来自服务器的单行回复
// The parseSingleLineReply parses a single-line reply from the server
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply
		result = reply.MakeStatusReply(str[1:])
	case '-': // err reply
		result = reply.MakeErrReply(str[1:])
	case ':': // int reply
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// parseSingleLineReply 解析来自服务器的单行回复
// The parseSingleLineReply function parses a single-line reply from the server
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// bulk reply
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
