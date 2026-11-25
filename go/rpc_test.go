package ipc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"testing"
	"time"
)

type testServer struct {
	rpc    *RpcProtocol
	ms     *MessageStream
	syncCh chan struct{}
	ready  chan struct{}
	ln     net.Listener
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts := &testServer{ln: ln, syncCh: make(chan struct{}, 4), ready: make(chan struct{})}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		ms := NewMessageStream(conn, conn)
		ms.OnSyncComplete = func() { ts.syncCh <- struct{}{} }
		rpc := NewRpcProtocol(ms)
		ts.rpc, ts.ms = rpc, ms
		rpc.OnError = func(err error) {} // 静默流错误（CRC 注入测试会触发）
		rpc.RegisterMethod("add", func(a []any) (any, error) {
			return a[0].(float64) + a[1].(float64), nil
		})
		rpc.RegisterMethod("echo", func(a []any) (any, error) { return a[0], nil })
		rpc.RegisterMethod("greet", func(a []any) (any, error) {
			return "你好, " + a[0].(string) + "!", nil
		})
		rpc.RegisterMethod("blob", func(a []any) (any, error) {
			return []byte{0, 1, 2, 250, 255}, nil
		})
		rpc.RegisterMethod("nothing", func(a []any) (any, error) { return nil, nil })
		rpc.RegisterMethod("fail", func(a []any) (any, error) {
			return nil, fmt.Errorf("boom: %v", a[0])
		})
		rpc.RegisterMethod("upload", func(a []any) (any, error) {
			data := a[0].([]byte)
			tag := a[1].(string)
			return map[string]any{"size": float64(len(data)), "tag": tag,
				"head": data[:4], "crcOK": true}, nil
		})
		rpc.RegisterMethod("pushEvents", func(a []any) (any, error) {
			_ = rpc.SendText("text-event")
			_ = rpc.SendJSON(map[string]any{"n": 42})
			_ = rpc.SendBinary([]byte{9, 8, 7})
			return "sent", nil
		})
		close(ts.ready)
	}()
	// 等待服务端就绪（连接建立后才注册完成，先 dial 再等 ready）
	return ts
}

func (ts *testServer) waitReady(t *testing.T) {
	t.Helper()
	select {
	case <-ts.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("server ready timeout")
	}
}

func (ts *testServer) close() {
	if ts.rpc != nil {
		ts.rpc.Close()
	}
	_ = ts.ln.Close()
}

func dialTest(t *testing.T, ts *testServer) (*RpcProtocol, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", ts.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ts.waitReady(t)
	rpc := NewRpcProtocol(NewMessageStream(conn, conn))
	rpc.OnError = func(err error) {}
	return rpc, conn
}

func TestJSONRequest(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("add", []any{3, 4})
	if err != nil || v.(float64) != 7 {
		t.Fatalf("add = %v, err = %v", v, err)
	}
	v, err = rpc.SendRequest("echo", map[string]any{"x": []any{1, "二", true, nil}})
	m := v.(map[string]any)
	if err != nil || m["x"].([]any)[1] != "二" {
		t.Fatalf("echo = %v, err = %v", v, err)
	}
}

func TestTextAndUnicode(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("greet", "世界")
	if err != nil || v != "你好, 世界!" {
		t.Fatalf("greet = %v, err = %v", v, err)
	}
}

func TestBinaryResponse(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("blob", nil)
	if err != nil || !bytes.Equal(v.([]byte), []byte{0, 1, 2, 250, 255}) {
		t.Fatalf("blob = %v, err = %v", v, err)
	}
}

func TestNullResponse(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	v, err := rpc.SendRequest("nothing", nil)
	if err != nil || v != nil {
		t.Fatalf("nothing = %v, err = %v", v, err)
	}
}

func TestErrorResponse(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	_, err := rpc.SendRequest("fail", "x")
	re, ok := err.(*RpcError)
	if !ok || re.Value != "boom: x" {
		t.Fatalf("fail err = %v", err)
	}
	_, err = rpc.SendRequest("nope", nil)
	re, ok = err.(*RpcError)
	if !ok || re.Value != "Method not found: nope" {
		t.Fatalf("nope err = %v", err)
	}
}

func TestBinaryRequest(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	data := make([]byte, 75*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	v, err := rpc.SendRequestBinary("upload", data, "tag-1")
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["size"].(float64) != float64(len(data)) || m["tag"] != "tag-1" {
		t.Fatalf("upload = %v", m)
	}
	head := m["head"].([]any)
	if len(head) != 4 || head[0].(float64) != 0 || head[3].(float64) != 3 {
		t.Fatalf("head = %v", head)
	}
}

func TestEvents(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	var mu sync.Mutex
	var events []any
	done := make(chan struct{})
	rpc.OnEvent = func(ct int, content any) {
		mu.Lock()
		events = append(events, content)
		if len(events) == 3 {
			close(done)
		}
		mu.Unlock()
	}
	if _, err := rpc.SendRequest("pushEvents", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("events timeout")
	}
	mu.Lock()
	defer mu.Unlock()
	if events[0] != "text-event" {
		t.Fatalf("event0 = %v", events[0])
	}
	if events[1].(map[string]any)["n"].(float64) != 42 {
		t.Fatalf("event1 = %v", events[1])
	}
	if !bytes.Equal(events[2].([]byte), []byte{9, 8, 7}) {
		t.Fatalf("event2 = %v", events[2])
	}
}

func TestConcurrentRequests(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, _ := dialTest(t, ts)
	defer rpc.Close()
	var wg sync.WaitGroup
	errCh := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v, err := rpc.SendRequest("add", []any{n, 1})
			if err != nil || v.(float64) != float64(n+1) {
				errCh <- fmt.Errorf("concurrent %d: %v %v", n, v, err)
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestCRCRecovery(t *testing.T) {
	ts := startTestServer(t)
	defer ts.close()
	rpc, conn := dialTest(t, ts)
	defer rpc.Close()

	// 注入坏长度头 CRC 帧：服务端报错→错误包→客户端回 RESET_CONFIRM→服务端 syncComplete
	payload := []byte("hello")
	frame := make([]byte, 8+len(payload)+4)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], 0xDEADBEEF)
	copy(frame[8:], payload)
	binary.LittleEndian.PutUint32(frame[8+len(payload):], crc32.ChecksumIEEE(payload))
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ts.syncCh:
	case <-time.After(3 * time.Second):
		t.Fatal("syncComplete timeout")
	}
	// 恢复后通信继续
	v, err := rpc.SendRequest("add", []any{10, 20})
	if err != nil || v.(float64) != 30 {
		t.Fatalf("after resync: %v %v", v, err)
	}
}