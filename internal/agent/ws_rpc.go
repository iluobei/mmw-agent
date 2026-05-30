package agent

// agent 端反向 RPC 调度器 — 接收 master 发来的 rpc_call,转成内存 HTTP 请求喂给共享 mux,
// 把响应封成 rpc_reply 回 master。
//
// 设计要点:
//   - rpcMux 跟 main.go 注册到 net.Listener 的那份 ServeMux **共享同一份 handler 实例**,
//     所以 `/api/child/inbounds` 等 endpoint 的业务逻辑只有一份代码,WS 路径自动跟进
//     handler 任何后续修改
//   - 请求带头 X-WS-RPC: 1 — handler 内部可据此跳过 Bearer 检查(WS 层已 securechan ECDH + token 认证)
//   - 响应用自实现 bufferResponseWriter,不依赖 httptest(避免拉测试包到生产二进制)
//   - agent 内部超时 = master 给的 TimeoutMs,到点直接砍掉 handler goroutine(用 ctx 通知,handler
//     不响应 ctx 也不会卡死 reply — 我们独立计时 reply 发送,等 buffer 写完就 send)

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

// WSRPCCallPayload 跟 master 端 internal/handler/ws_rpc.go 中的同名结构对齐。
type WSRPCCallPayload struct {
	RequestID string          `json:"request_id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Query     string          `json:"query,omitempty"`
	Body      json.RawMessage `json:"body,omitempty"`
	TimeoutMs int             `json:"timeout_ms,omitempty"`
	// Stream=true 时本次调用走流式路径:handler 每次 Flush() 触发一帧 rpc_stream_data,
	// handler 返回后再发 rpc_reply 作为结束帧。
	Stream bool `json:"stream,omitempty"`
}

// WSRPCReplyPayload 跟 master 端对齐。流式调用复用它作 end 帧。
type WSRPCReplyPayload struct {
	RequestID string          `json:"request_id"`
	Status    int             `json:"status"`
	Body      json.RawMessage `json:"body,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// WSRPCStreamDataPayload 流式中间数据帧。
type WSRPCStreamDataPayload struct {
	RequestID string `json:"request_id"`
	Data      string `json:"data"`
}

// bufferResponseWriter 实现 http.ResponseWriter,把所有写入累积到内存 buffer。
// 不需要标准 http 层的 chunked / hijacker / pusher 等接口 —— 我们的 /api/child/* handler
// 都是普通 JSON 响应,WriteHeader + Write(body) 就够。
type bufferResponseWriter struct {
	headers http.Header
	body    bytes.Buffer
	status  int
}

func newBufferResponseWriter() *bufferResponseWriter {
	return &bufferResponseWriter{
		headers: make(http.Header),
		status:  http.StatusOK,
	}
}

func (w *bufferResponseWriter) Header() http.Header { return w.headers }
func (w *bufferResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}
func (w *bufferResponseWriter) WriteHeader(status int) {
	w.status = status
}

// handleRPCCall 入口 — 流式路径调 handleRPCStreamCall,普通路径走下面的 buffer 模式。
// 由 message dispatcher 在 go 协程里调用,确保不堵主 read 循环。
func (c *Client) handleRPCCall(conn *websocket.Conn, p WSRPCCallPayload) {
	if p.Stream {
		c.handleRPCStreamCall(conn, p)
		return
	}
	c.handleRPCCallBuffer(conn, p)
}

// handleRPCCallBuffer 普通(非流)RPC:buffer 接住整段响应,一次 rpc_reply 发回。
func (c *Client) handleRPCCallBuffer(conn *websocket.Conn, p WSRPCCallPayload) {
	reply := WSRPCReplyPayload{RequestID: p.RequestID}

	defer func() {
		if r := recover(); r != nil {
			// handler 内部 panic — 不能让整个 agent 退出,记 reply.Error,master 不 fallback
			log.Printf("[Agent] rpc_call handler panic: %v (path=%s)", r, p.Path)
			reply.Status = http.StatusInternalServerError
			reply.Error = "agent handler panic"
		}
		c.sendRPCReply(conn, reply)
	}()

	if c.rpcMux == nil {
		// 老路径或测试场景,不该走到这里(capabilities.rpc=false 时 master 不会发 rpc_call),
		// 兜底报错。
		reply.Status = http.StatusServiceUnavailable
		reply.Error = "agent rpc mux not initialized"
		return
	}

	// 构造 *http.Request。Query 单独传是为了避免 master 那边把 query 拼进 path 又被 mux 误解析。
	u := &url.URL{Path: p.Path, RawQuery: p.Query}
	var body []byte = p.Body
	req, err := http.NewRequest(p.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		reply.Status = http.StatusBadRequest
		reply.Error = "construct request: " + err.Error()
		return
	}
	// X-WS-RPC: 1 让 handler 知道这是 WS 通道来的,跳过 Bearer 检查
	// (authenticatedOrWSRPC helper 见 management_handler.go)
	req.Header.Set("X-WS-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	// 模拟一个本地源 — 一些 handler 用 r.RemoteAddr 记日志,空字符串会让日志难看
	req.RemoteAddr = "ws-rpc"

	// 用一个独立 buffer writer 接住 handler 输出
	w := newBufferResponseWriter()

	// agent 内部超时(从 payload 来,master 端 timeout - 2s),用 channel 等待 handler 返回
	timeout := time.Duration(p.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		c.rpcMux.ServeHTTP(w, req)
		close(done)
	}()

	select {
	case <-done:
		// 正常返回
	case <-time.After(timeout):
		// handler 卡死,reply 提示超时;让 handler goroutine 自己跑完,buffer 数据已经无意义
		log.Printf("[Agent] rpc_call timeout (%v) path=%s", timeout, p.Path)
		reply.Status = http.StatusGatewayTimeout
		reply.Error = "agent handler timeout"
		return
	}

	reply.Status = w.status
	reply.Body = json.RawMessage(w.body.Bytes())
}

// sendRPCReply 用 writeEncrypted 把响应回 master。WS 已断 → 静默丢弃,master 那边会 timeout fallback HTTP。
func (c *Client) sendRPCReply(conn *websocket.Conn, reply WSRPCReplyPayload) {
	msg := map[string]any{
		"type":    WSMsgTypeRPCReply,
		"payload": reply,
	}
	if err := c.writeEncrypted(conn, msg); err != nil {
		log.Printf("[Agent] send rpc_reply failed (request_id=%s): %v", reply.RequestID, err)
	}
}

// ================== 流式 RPC(替代 SSE)==================

// streamingResponseWriter 实现 http.ResponseWriter + http.Flusher。
// 每次 handler 调 Flush() → 把 buf 累积的字节作为一帧 rpc_stream_data 发给 master,然后 reset buf。
// handler 返回后还会被 handleRPCStreamCall 兜底再 Flush 一次,保证收尾的残留 buf 也送出去。
type streamingResponseWriter struct {
	headers   http.Header
	status    int
	buf       bytes.Buffer
	requestID string
	conn      *websocket.Conn
	sender    func(conn *websocket.Conn, reqID string, data []byte)
}

func newStreamingResponseWriter(conn *websocket.Conn, reqID string, sender func(*websocket.Conn, string, []byte)) *streamingResponseWriter {
	return &streamingResponseWriter{
		headers:   make(http.Header),
		status:    http.StatusOK,
		requestID: reqID,
		conn:      conn,
		sender:    sender,
	}
}

func (w *streamingResponseWriter) Header() http.Header        { return w.headers }
func (w *streamingResponseWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *streamingResponseWriter) WriteHeader(s int)            { w.status = s }
func (w *streamingResponseWriter) Flush() {
	if w.buf.Len() == 0 {
		return
	}
	// copy 一份,避免 sender 异步执行时 buf reset 破坏数据
	data := append([]byte(nil), w.buf.Bytes()...)
	w.buf.Reset()
	w.sender(w.conn, w.requestID, data)
}

// handleRPCStreamCall 流式调用:每次 handler Flush 触发 rpc_stream_data,handler 返回后发 rpc_reply。
// timeout 来自 master payload — 5 分钟够大部分场景。
func (c *Client) handleRPCStreamCall(conn *websocket.Conn, p WSRPCCallPayload) {
	reply := WSRPCReplyPayload{RequestID: p.RequestID}

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Agent] rpc_stream_call handler panic: %v (path=%s)", r, p.Path)
			reply.Status = http.StatusInternalServerError
			reply.Error = "agent handler panic"
		}
		c.sendRPCReply(conn, reply)
	}()

	if c.rpcMux == nil {
		reply.Status = http.StatusServiceUnavailable
		reply.Error = "agent rpc mux not initialized"
		return
	}

	u := &url.URL{Path: p.Path, RawQuery: p.Query}
	req, err := http.NewRequest(p.Method, u.String(), bytes.NewReader([]byte(p.Body)))
	if err != nil {
		reply.Status = http.StatusBadRequest
		reply.Error = "construct request: " + err.Error()
		return
	}
	req.Header.Set("X-WS-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "ws-rpc"

	sw := newStreamingResponseWriter(conn, p.RequestID, c.sendRPCStreamData)

	timeout := time.Duration(p.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	done := make(chan struct{})
	go func() {
		c.rpcMux.ServeHTTP(sw, req)
		close(done)
	}()
	select {
	case <-done:
		// handler 正常返回,把残留 buf 收尾 flush 一次
		sw.Flush()
		reply.Status = sw.status
	case <-time.After(timeout):
		log.Printf("[Agent] rpc_stream_call timeout (%v) path=%s", timeout, p.Path)
		reply.Status = http.StatusGatewayTimeout
		reply.Error = "agent handler timeout"
		// 不调 sw.Flush() — 已经写过的帧 master 已收到,reply 告知超时即可
	}
}

// sendRPCStreamData 单帧 rpc_stream_data。WS 已断 → 静默丢弃(master 会通过 reply timeout 感知)。
func (c *Client) sendRPCStreamData(conn *websocket.Conn, reqID string, data []byte) {
	msg := map[string]any{
		"type": WSMsgTypeRPCStreamData,
		"payload": WSRPCStreamDataPayload{
			RequestID: reqID,
			Data:      string(data),
		},
	}
	if err := c.writeEncrypted(conn, msg); err != nil {
		log.Printf("[Agent] send rpc_stream_data failed (request_id=%s): %v", reqID, err)
	}
}
