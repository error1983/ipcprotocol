package ipc

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
)

// 消息类型与载荷类型（与 JS 版一致）。
const (
	TypeRequest  = 0
	TypeResponse = 1
	TypeEvent    = 2

	ContentText   = 0
	ContentJSON   = 1
	ContentBinary = 2
)

// Handler 服务端方法处理器；args 为展开后的参数，返回值决定响应类型。
type Handler func(args []any) (any, error)

// RpcError 对端返回的错误响应，Value 携带原始 error 值（与 JS reject(response.error) 对齐）。
type RpcError struct{ Value any }

func (e *RpcError) Error() string { return fmt.Sprintf("rpc error: %v", e.Value) }

// RpcProtocol 消息层：请求/响应/事件。构造时自动挂回调并启动读循环。
type RpcProtocol struct {
	ms *MessageStream

	handlersMu sync.Mutex
	handlers   map[string]Handler

	pendingMu sync.Mutex
	pending   map[string]chan rpcResult

	OnEvent func(contentType int, content any)
	OnError func(err error)

	closeOnce sync.Once
}

type rpcResult struct {
	value any
	err   error
}

// NewRpcProtocol 包装消息流；先注册回调再启动读循环，避免早到消息丢失。
func NewRpcProtocol(ms *MessageStream) *RpcProtocol {
	rpc := &RpcProtocol{
		ms:       ms,
		handlers: make(map[string]Handler),
		pending:  make(map[string]chan rpcResult),
	}
	ms.OnMessage = rpc.handleMessage
	ms.OnError = rpc.handleStreamError
	ms.Start()
	return rpc
}

// RegisterMethod 注册服务端方法。
func (rpc *RpcProtocol) RegisterMethod(name string, h Handler) {
	rpc.handlersMu.Lock()
	defer rpc.handlersMu.Unlock()
	rpc.handlers[name] = h
}

// SendRequest 发送 JSON 请求并阻塞等待响应。
// 返回值为 JSON 解析结果（map[string]any / []any / float64 / string / bool / nil）、
// TEXT 字符串或 BINARY []byte；对端错误响应返回 *RpcError。
func (rpc *RpcProtocol) SendRequest(method string, params any) (any, error) {
	return rpc.SendRequestTo(method, params, "")
}

// SendRequestTo 带可选 to 路由字段的请求。
func (rpc *RpcProtocol) SendRequestTo(method string, params any, to string) (any, error) {
	id := newUUID()
	ch := make(chan rpcResult, 1)
	rpc.pendingMu.Lock()
	rpc.pending[id] = ch
	rpc.pendingMu.Unlock()

	payload := map[string]any{"method": method, "params": normalizeJSON(params)}
	if to != "" {
		payload["to"] = to
	}
	body, err := json.Marshal(payload)
	if err != nil {
		rpc.removePending(id)
		return nil, err
	}
	if err := rpc.ms.Send(buildMessage(TypeRequest, ContentJSON, id, body)); err != nil {
		rpc.removePending(id)
		return nil, err
	}
	res := <-ch
	return res.value, res.err
}

// SendRequestBinary 发送二进制请求：handler 实参为 [data, ...args]。
func (rpc *RpcProtocol) SendRequestBinary(method string, data []byte, args ...any) (any, error) {
	id := newUUID()
	ch := make(chan rpcResult, 1)
	rpc.pendingMu.Lock()
	rpc.pending[id] = ch
	rpc.pendingMu.Unlock()

	body := encodeBinaryRequest(method, data, args)
	if err := rpc.ms.Send(buildMessage(TypeRequest, ContentBinary, id, body)); err != nil {
		rpc.removePending(id)
		return nil, err
	}
	res := <-ch
	return res.value, res.err
}

// SendText / SendJSON / SendBinary 发送单向事件。
func (rpc *RpcProtocol) SendText(text string) error {
	return rpc.ms.Send(buildMessage(TypeEvent, ContentText, newUUID(), []byte(text)))
}

func (rpc *RpcProtocol) SendJSON(value any) error {
	body, err := json.Marshal(normalizeJSON(value))
	if err != nil {
		return err
	}
	return rpc.ms.Send(buildMessage(TypeEvent, ContentJSON, newUUID(), body))
}

func (rpc *RpcProtocol) SendBinary(data []byte) error {
	return rpc.ms.Send(buildMessage(TypeEvent, ContentBinary, newUUID(), data))
}

// Close 销毁：拒绝所有 pending 并关闭流。
func (rpc *RpcProtocol) Close() {
	rpc.closeOnce.Do(func() {
		rpc.rejectAll(errors.New("RPC Protocol destroyed"))
		rpc.ms.Close()
	})
}

func (rpc *RpcProtocol) removePending(id string) {
	rpc.pendingMu.Lock()
	delete(rpc.pending, id)
	rpc.pendingMu.Unlock()
}

func (rpc *RpcProtocol) takePending(id string) chan rpcResult {
	rpc.pendingMu.Lock()
	defer rpc.pendingMu.Unlock()
	ch := rpc.pending[id]
	delete(rpc.pending, id)
	return ch
}

func (rpc *RpcProtocol) rejectAll(err error) {
	rpc.pendingMu.Lock()
	defer rpc.pendingMu.Unlock()
	for id, ch := range rpc.pending {
		ch <- rpcResult{err: err}
		delete(rpc.pending, id)
	}
}

func (rpc *RpcProtocol) handleStreamError(err error) {
	rpc.rejectAll(fmt.Errorf("Request failed: %w", err))
	if rpc.OnError != nil {
		rpc.OnError(err)
	}
}

func (rpc *RpcProtocol) handleMessage(data []byte) {
	if len(data) < 18 {
		log.Printf("[ipc] message too short: %d", len(data))
		return
	}
	msgType := int(data[0])
	contentType := int(data[1])
	id := bytesToUUID(data[2:18])
	payload := data[18:]

	content, err := deserialize(contentType, payload)
	if err != nil {
		log.Printf("[ipc] deserialize failed: %v", err)
		return
	}

	switch msgType {
	case TypeRequest:
		go rpc.handleRequest(id, contentType, content, payload)
	case TypeResponse:
		if ch := rpc.takePending(id); ch != nil {
			if m, ok := content.(map[string]any); ok {
				if ev, exists := m["error"]; exists && isTruthy(ev) {
					ch <- rpcResult{err: &RpcError{Value: ev}}
					return
				}
			}
			ch <- rpcResult{value: content}
		}
	case TypeEvent:
		if rpc.OnEvent != nil {
			rpc.OnEvent(contentType, content)
		}
	}
}

func (rpc *RpcProtocol) handleRequest(id string, contentType int, content any, raw []byte) {
	var method string
	var args []any

	if contentType == ContentBinary {
		m, binArgs, err := decodeBinaryRequest(raw)
		if err != nil {
			log.Printf("[ipc] bad binary request: %v", err)
			return
		}
		method, args = m, binArgs
	} else {
		m, ok := content.(map[string]any)
		if !ok {
			log.Printf("[ipc] invalid request content")
			return
		}
		method, _ = m["method"].(string)
		args = expandParams(m["params"])
	}

	rpc.handlersMu.Lock()
	h := rpc.handlers[method]
	rpc.handlersMu.Unlock()

	var result any
	var handlerErr error
	if h == nil {
		handlerErr = fmt.Errorf("Method not found: %s", method)
	} else {
		result, handlerErr = h(args)
	}

	var respType int
	var respBody []byte
	if handlerErr != nil {
		respType = ContentJSON
		respBody, _ = json.Marshal(map[string]any{"error": handlerErr.Error()})
	} else {
		respType, respBody = serializeResponse(result)
	}
	if err := rpc.ms.Send(buildMessage(TypeResponse, respType, id, respBody)); err != nil {
		log.Printf("[ipc] send response failed: %v", err)
	}
}

// ---- 序列化辅助 ----

func deserialize(contentType int, payload []byte) (any, error) {
	switch contentType {
	case ContentText:
		return string(payload), nil
	case ContentJSON:
		var v any
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		return v, nil
	case ContentBinary:
		out := make([]byte, len(payload))
		copy(out, payload)
		return out, nil
	default:
		return nil, fmt.Errorf("unknown content type: %d", contentType)
	}
}

func serializeResponse(result any) (int, []byte) {
	switch v := result.(type) {
	case nil:
		return ContentJSON, []byte("null")
	case string:
		return ContentText, []byte(v)
	case []byte:
		return ContentBinary, v
	default:
		body, err := json.Marshal(normalizeJSON(result))
		if err != nil {
			return ContentJSON, []byte("null")
		}
		return ContentJSON, body
	}
}

func expandParams(params any) []any {
	switch v := params.(type) {
	case nil:
		return nil
	case []any:
		return v
	default:
		return []any{v}
	}
}

// normalizeJSON 递归把 []byte 转为数字数组（对齐 JSON.stringify(Buffer)；
// Go 默认将 []byte 编为 Base64，必须转换）。
func normalizeJSON(v any) any {
	switch t := v.(type) {
	case []byte:
		arr := make([]any, len(t))
		for i, b := range t {
			arr[i] = float64(b)
		}
		return arr
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeJSON(val)
		}
		return out
	default:
		return v
	}
}

// isTruthy JS 真值语义：nil/false/0/""/NaN 为假，其余（含空 map/slice）为真。
func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case float64:
		return t != 0 && !math.IsNaN(t)
	case int:
		return t != 0
	case string:
		return t != ""
	default:
		return true
	}
}

func buildMessage(msgType, contentType int, id string, payload []byte) []byte {
	msg := make([]byte, 18+len(payload))
	msg[0] = byte(msgType)
	msg[1] = byte(contentType)
	copy(msg[2:18], uuidToBytes(id))
	copy(msg[18:], payload)
	return msg
}

// encodeBinaryRequest [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary]
func encodeBinaryRequest(method string, data []byte, args []any) []byte {
	methodBytes := []byte(method)
	paramsJSON, _ := json.Marshal(normalizeJSON(args))
	if args == nil {
		paramsJSON = []byte("[]")
	}
	out := make([]byte, 4+len(methodBytes)+4+len(paramsJSON)+len(data))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(methodBytes)))
	copy(out[4:], methodBytes)
	off := 4 + len(methodBytes)
	binary.BigEndian.PutUint32(out[off:off+4], uint32(len(paramsJSON)))
	copy(out[off+4:], paramsJSON)
	copy(out[off+4+len(paramsJSON):], data)
	return out
}

func decodeBinaryRequest(raw []byte) (string, []any, error) {
	if len(raw) < 4 {
		return "", nil, errors.New("binary request too short")
	}
	methodLen := int(binary.BigEndian.Uint32(raw[0:4]))
	if len(raw) < 4+methodLen+4 {
		return "", nil, errors.New("binary request truncated")
	}
	method := string(raw[4 : 4+methodLen])
	off := 4 + methodLen
	paramsLen := int(binary.BigEndian.Uint32(raw[off : off+4]))
	if len(raw) < off+4+paramsLen {
		return "", nil, errors.New("binary request params truncated")
	}
	var params []any
	if err := json.Unmarshal(raw[off+4:off+4+paramsLen], &params); err != nil {
		return "", nil, err
	}
	bin := raw[off+4+paramsLen:]
	data := make([]byte, len(bin))
	copy(data, bin)
	args := make([]any, 0, len(params)+1)
	args = append(args, data)
	args = append(args, params...)
	return method, args, nil
}

// ---- UUID（RFC 4122 大端字节序，与 JS/Python/Java 一致）----

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return bytesToUUID(b)
}

func uuidToBytes(id string) []byte {
	b, _ := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return b
}

func bytesToUUID(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}