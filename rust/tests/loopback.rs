//! Rust 回环测试：覆盖请求/响应/事件/二进制请求/并发/CRC 重同步。

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use ipcprotocol::json::{self, Json};
use ipcprotocol::message_stream::MessageStream;
use ipcprotocol::rpc_protocol::{RpcError, RpcProtocol};

fn boxed_stream(s: &TcpStream) -> (Box<dyn std::io::Read + Send>, Box<dyn std::io::Write + Send>) {
    (Box::new(s.try_clone().unwrap()), Box::new(s.try_clone().unwrap()))
}

struct ServerHandle {
    addr: std::net::SocketAddr,
    sync_rx: mpsc::Receiver<()>,
    ready_rx: Mutex<mpsc::Receiver<Arc<RpcProtocol>>>,
}

fn start_server() -> ServerHandle {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    let (sync_tx, sync_rx) = mpsc::channel();
    let (ready_tx, ready_rx) = mpsc::channel::<Arc<RpcProtocol>>();
    thread::spawn(move || {
        let (conn, _) = ln.accept().unwrap();
        let (r, w) = boxed_stream(&conn);
        let ms = MessageStream::new(r, w);
        ms.on_sync_complete(Box::new(move || {
            let _ = sync_tx.send(());
        }));
        let rpc = RpcProtocol::new(ms);
        rpc.on_error(Box::new(|_| {}));
        register_test_methods(&rpc);
        let _ = ready_tx.send(rpc.clone());
        // 保持连接存活直到进程结束
        std::mem::forget(conn);
    });
    ServerHandle { addr, sync_rx, ready_rx: Mutex::new(ready_rx) }
}

fn register_test_methods(rpc: &Arc<RpcProtocol>) {
    rpc.register_method("add", Box::new(|a| {
        Ok(Json::Num(a[0].as_f64().unwrap() + a[1].as_f64().unwrap()))
    }));
    rpc.register_method("echo", Box::new(|a| Ok(a[0].clone())));
    rpc.register_method("greet", Box::new(|a| {
        Ok(Json::Str(format!("你好, {}!", a[0].as_str().unwrap())))
    }));
    rpc.register_method("blob", Box::new(|_| Ok(Json::Bytes(vec![0, 1, 2, 250, 255]))));
    rpc.register_method("nothing", Box::new(|_| Ok(Json::Null)));
    rpc.register_method("fail", Box::new(|a| Err(format!("boom: {}", json::stringify(&a[0])))));
    {
        let rpc2 = rpc.clone();
        rpc.register_method("pushEvents", Box::new(move |_| {
            let _ = rpc2.send_text("text-event");
            let mut v = Json::obj();
            v.insert("n", Json::Num(42.0));
            let _ = rpc2.send_json(&v);
            let _ = rpc2.send_binary(&[9, 8, 7]);
            Ok(Json::Str("sent".into()))
        }));
    }
    rpc.register_method("upload", Box::new(|a| {
        let data = a[0].as_bytes().unwrap();
        let mut m = Json::obj();
        m.insert("size", Json::Num(data.len() as f64));
        m.insert("tag", a[1].clone());
        m.insert("head", Json::Arr(data[..4].iter().map(|b| Json::Num(*b as f64)).collect()));
        Ok(m)
    }));
    rpc.register_method("slow", Box::new(|_| {
        thread::sleep(Duration::from_millis(500));
        Ok(Json::Str("late".into()))
    }));
}

fn dial(server: &ServerHandle) -> (Arc<RpcProtocol>, TcpStream) {
    let conn = TcpStream::connect(server.addr).unwrap();
    // 等服务端注册完方法再启动客户端读循环
    let _server_rpc = server.ready_rx.lock().unwrap().recv_timeout(Duration::from_secs(3)).unwrap();
    let (r, w) = boxed_stream(&conn);
    let rpc = RpcProtocol::new(MessageStream::new(r, w));
    rpc.on_error(Box::new(|_| {}));
    (rpc, conn)
}

#[test]
fn json_request_and_unicode() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(3.0), Json::Num(4.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(7.0));

    let mut obj = Json::obj();
    obj.insert("x", Json::Arr(vec![Json::Num(1.0), Json::Str("二".into()), Json::Bool(true), Json::Null]));
    let v = rpc.send_request("echo", obj).unwrap();
    assert_eq!(v.get("x").unwrap().as_arr().unwrap()[1].as_str(), Some("二"));
    rpc.close();
}

#[test]
fn text_binary_null_responses() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let v = rpc.send_request("greet", Json::Str("世界".into())).unwrap();
    assert_eq!(v.as_str(), Some("你好, 世界!"));
    let v = rpc.send_request("blob", Json::Null).unwrap();
    assert_eq!(v.as_bytes(), Some(&[0u8, 1, 2, 250, 255][..]));
    let v = rpc.send_request("nothing", Json::Null).unwrap();
    assert_eq!(v, Json::Null);
    rpc.close();
}

#[test]
fn error_responses() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let err = rpc.send_request("fail", Json::Str("x".into())).unwrap_err();
    match err {
        RpcError::Remote(v) => assert_eq!(v.as_str(), Some("boom: \"x\"")),
        _ => panic!("expected remote error"),
    }
    let err = rpc.send_request("nope", Json::Null).unwrap_err();
    match err {
        RpcError::Remote(v) => assert_eq!(v.as_str(), Some("Method not found: nope")),
        _ => panic!("expected remote error"),
    }
    rpc.close();
}

#[test]
fn binary_request_75kb() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let data: Vec<u8> = (0..75 * 1024).map(|i| (i % 251) as u8).collect();
    let v = rpc
        .send_request_binary("upload", &data, vec![Json::Str("tag-1".into())])
        .unwrap();
    assert_eq!(v.get("size").unwrap().as_f64(), Some(data.len() as f64));
    assert_eq!(v.get("tag").unwrap().as_str(), Some("tag-1"));
    let head = v.get("head").unwrap().as_arr().unwrap();
    assert_eq!(head.len(), 4);
    assert_eq!(head[3].as_f64(), Some(3.0));
    rpc.close();
}

#[test]
fn events() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let events: Arc<Mutex<Vec<Json>>> = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = mpsc::channel();
    {
        let events = events.clone();
        rpc.on_event(Box::new(move |_ct, content| {
            let mut ev = events.lock().unwrap();
            ev.push(content);
            if ev.len() == 3 {
                let _ = tx.send(());
            }
        }));
    }
    rpc.send_request("pushEvents", Json::Null).unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let ev = events.lock().unwrap();
    assert_eq!(ev[0].as_str(), Some("text-event"));
    assert_eq!(ev[1].get("n").unwrap().as_f64(), Some(42.0));
    assert_eq!(ev[2].as_bytes(), Some(&[9u8, 8, 7][..]));
    rpc.close();
}

#[test]
fn concurrent_50() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);
    let mut handles = Vec::new();
    for i in 0..50 {
        let rpc = rpc.clone();
        handles.push(thread::spawn(move || {
            let v = rpc
                .send_request("add", Json::Arr(vec![Json::Num(i as f64), Json::Num(1.0)]))
                .unwrap();
            assert_eq!(v.as_f64(), Some((i + 1) as f64));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    rpc.close();
}

#[test]
fn crc_recovery() {
    let server = start_server();
    let (rpc, mut conn) = dial(&server);

    // 注入坏长度头 CRC 帧
    let payload = b"hello";
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&0xDEADBEEFu32.to_le_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&ipcprotocol::message_stream::crc32(payload).to_le_bytes());
    conn.write_all(&frame).unwrap();

    server.sync_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // 恢复后通信继续
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(10.0), Json::Num(20.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(30.0));
    rpc.close();
}

#[test]
fn rpc_timeout() {
    let server = start_server();
    let (rpc, _conn) = dial(&server);

    assert_eq!(rpc.default_timeout(), Duration::from_secs(15));
    // 显式短超时触发 ETIMEDOUT
    let err = rpc
        .send_request_timeout("slow", Json::Null, Duration::from_millis(100))
        .unwrap_err();
    assert_eq!(err.code(), "ETIMEDOUT");
    assert!(matches!(err, RpcError::Timeout(_)));
    // pending 已清理，后续调用正常
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(1.0), Json::Num(2.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(3.0));
    // 等 slow 的迟到响应到达（应被静默丢弃）
    thread::sleep(Duration::from_millis(600));
    let v = rpc.send_request("add", Json::Arr(vec![Json::Num(5.0), Json::Num(6.0)])).unwrap();
    assert_eq!(v.as_f64(), Some(11.0));
    rpc.close();
}