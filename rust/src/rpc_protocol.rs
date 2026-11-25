//! RpcProtocol 消息层：请求/响应/事件，与 RpcProtocol.js 线格式兼容。

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, Once};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::json::{self, Json};
use crate::message_stream::MessageStream;

pub const TYPE_REQUEST: u8 = 0;
pub const TYPE_RESPONSE: u8 = 1;
pub const TYPE_EVENT: u8 = 2;

pub const CONTENT_TEXT: u8 = 0;
pub const CONTENT_JSON: u8 = 1;
pub const CONTENT_BINARY: u8 = 2;

/// 服务端方法处理器：args 为展开后的参数；Err(String) 转为 {"error": ...} 响应。
pub type Handler = Box<dyn Fn(Vec<Json>) -> Result<Json, String> + Send + Sync>;

/// RPC 错误：Remote 携带对端原始 error 值（对齐 JS reject(response.error)）。
#[derive(Debug, Clone)]
pub enum RpcError {
    Remote(Json),
    Message(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Remote(v) => write!(f, "rpc error: {}", json::stringify(v)),
            RpcError::Message(m) => write!(f, "{}", m),
        }
    }
}

impl std::error::Error for RpcError {}

type EventCb = Box<dyn Fn(u8, Json) + Send>;
type PendingMap = HashMap<String, mpsc::Sender<Result<Json, RpcError>>>;

pub struct RpcProtocol {
    ms: MessageStream,
    handlers: Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    pending: Arc<Mutex<PendingMap>>,
    on_event: Arc<Mutex<Option<EventCb>>>,
    on_error: Arc<Mutex<Option<Box<dyn Fn(String) + Send>>>>,
    close_once: Once,
}

impl RpcProtocol {
    /// 包装消息流：先注册回调再启动读循环，避免早到消息丢失。
    pub fn new(ms: MessageStream) -> Arc<Self> {
        let rpc = Arc::new(RpcProtocol {
            ms: ms.clone(),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            on_event: Arc::new(Mutex::new(None)),
            on_error: Arc::new(Mutex::new(None)),
            close_once: Once::new(),
        });

        {
            let handlers = rpc.handlers.clone();
            let pending = rpc.pending.clone();
            let on_event = rpc.on_event.clone();
            let ms2 = ms.clone();
            ms.on_message(Box::new(move |data| {
                handle_message(&data, &handlers, &pending, &on_event, &ms2);
            }));
        }
        {
            let pending = rpc.pending.clone();
            let on_error = rpc.on_error.clone();
            ms.on_error(Box::new(move |msg| {
                reject_all(&pending, RpcError::Message(format!("Request failed: {}", msg)));
                let cb = on_error.lock().unwrap();
                if let Some(f) = &*cb {
                    f(msg);
                }
            }));
        }
        ms.start();
        rpc
    }

    pub fn register_method(&self, name: &str, h: Handler) {
        self.handlers.lock().unwrap().insert(name.to_string(), Arc::new(h));
    }

    pub fn on_event(&self, f: EventCb) {
        *self.on_event.lock().unwrap() = Some(f);
    }

    pub fn on_error(&self, f: Box<dyn Fn(String) + Send>) {
        *self.on_error.lock().unwrap() = Some(f);
    }

    /// 发送 JSON 请求并阻塞等待响应。
    pub fn send_request(&self, method: &str, params: Json) -> Result<Json, RpcError> {
        self.send_request_to(method, params, None)
    }

    /// 带可选 to 路由字段的请求。
    pub fn send_request_to(&self, method: &str, params: Json, to: Option<&str>) -> Result<Json, RpcError> {
        let id = new_uuid();
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);

        let mut payload = Json::obj();
        payload.insert("method", Json::Str(method.to_string()));
        payload.insert("params", params);
        if let Some(t) = to {
            payload.insert("to", Json::Str(t.to_string()));
        }
        let body = json::stringify(&payload);
        if let Err(e) = self.ms.send(&build_message(TYPE_REQUEST, CONTENT_JSON, &id, body.as_bytes())) {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError::Message(e.to_string()));
        }
        rx.recv().map_err(|_| RpcError::Message("request channel closed".into()))?
    }

    /// 发送二进制请求：handler 实参为 [Bytes(data), ...args]。
    pub fn send_request_binary(&self, method: &str, data: &[u8], args: Vec<Json>) -> Result<Json, RpcError> {
        let id = new_uuid();
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);

        let body = encode_binary_request(method, data, &args);
        if let Err(e) = self.ms.send(&build_message(TYPE_REQUEST, CONTENT_BINARY, &id, &body)) {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError::Message(e.to_string()));
        }
        rx.recv().map_err(|_| RpcError::Message("request channel closed".into()))?
    }

    /// 发送单向事件。
    pub fn send_text(&self, text: &str) -> Result<(), RpcError> {
        self.ms
            .send(&build_message(TYPE_EVENT, CONTENT_TEXT, &new_uuid(), text.as_bytes()))
            .map_err(|e| RpcError::Message(e.to_string()))
    }

    pub fn send_json(&self, value: &Json) -> Result<(), RpcError> {
        let body = json::stringify(value);
        self.ms
            .send(&build_message(TYPE_EVENT, CONTENT_JSON, &new_uuid(), body.as_bytes()))
            .map_err(|e| RpcError::Message(e.to_string()))
    }

    pub fn send_binary(&self, data: &[u8]) -> Result<(), RpcError> {
        self.ms
            .send(&build_message(TYPE_EVENT, CONTENT_BINARY, &new_uuid(), data))
            .map_err(|e| RpcError::Message(e.to_string()))
    }

    /// 销毁：拒绝所有 pending 并关闭流。
    pub fn close(&self) {
        self.close_once.call_once(|| {
            reject_all(&self.pending, RpcError::Message("RPC Protocol destroyed".into()));
            self.ms.close();
        });
    }
}

fn reject_all(pending: &Arc<Mutex<PendingMap>>, err: RpcError) {
    let mut map = pending.lock().unwrap();
    for (_, tx) in map.drain() {
        let _ = tx.send(Err(err.clone()));
    }
}

fn handle_message(
    data: &[u8],
    handlers: &Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    pending: &Arc<Mutex<PendingMap>>,
    on_event: &Arc<Mutex<Option<EventCb>>>,
    ms: &MessageStream,
) {
    if data.len() < 18 {
        eprintln!("[ipc] message too short: {}", data.len());
        return;
    }
    let msg_type = data[0];
    let content_type = data[1];
    let id = bytes_to_uuid(&data[2..18]);
    let payload = &data[18..];

    let content = match deserialize(content_type, payload) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ipc] deserialize failed: {}", e);
            return;
        }
    };

    match msg_type {
        TYPE_REQUEST => {
            let handlers = handlers.clone();
            let ms = ms.clone();
            let raw = payload.to_vec();
            thread::spawn(move || {
                handle_request(&id, content_type, content, &raw, &handlers, &ms);
            });
        }
        TYPE_RESPONSE => {
            let tx = pending.lock().unwrap().remove(&id);
            if let Some(tx) = tx {
                if let Some(ev) = content.get("error") {
                    if ev.is_truthy() {
                        let _ = tx.send(Err(RpcError::Remote(ev.clone())));
                        return;
                    }
                }
                let _ = tx.send(Ok(content));
            }
        }
        TYPE_EVENT => {
            let cb = on_event.lock().unwrap();
            if let Some(f) = &*cb {
                f(content_type, content);
            }
        }
        _ => eprintln!("[ipc] unknown message type: {}", msg_type),
    }
}

fn handle_request(
    id: &str,
    content_type: u8,
    content: Json,
    raw: &[u8],
    handlers: &Arc<Mutex<HashMap<String, Arc<Handler>>>>,
    ms: &MessageStream,
) {
    let (method, args) = if content_type == CONTENT_BINARY {
        match decode_binary_request(raw) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[ipc] bad binary request: {}", e);
                return;
            }
        }
    } else {
        let method = content.get("method").and_then(|m| m.as_str()).unwrap_or("").to_string();
        let args = expand_params(content.get("params").cloned().unwrap_or(Json::Null));
        (method, args)
    };

    // 取出处理器引用后立即释放锁，避免长时间持锁阻塞其他请求
    let handler = handlers.lock().unwrap().get(&method).cloned();
    let result = match handler {
        Some(h) => h(args),
        None => Err(format!("Method not found: {}", method)),
    };

    let (resp_type, resp_body) = match result {
        Err(msg) => {
            let mut e = Json::obj();
            e.insert("error", Json::Str(msg));
            (CONTENT_JSON, json::stringify(&e).into_bytes())
        }
        Ok(v) => serialize_response(&v),
    };
    if let Err(e) = ms.send(&build_message(TYPE_RESPONSE, resp_type, id, &resp_body)) {
        eprintln!("[ipc] send response failed: {}", e);
    }
}

// ---- 序列化辅助 ----

fn deserialize(content_type: u8, payload: &[u8]) -> Result<Json, String> {
    match content_type {
        CONTENT_TEXT => Ok(Json::Str(String::from_utf8_lossy(payload).into_owned())),
        CONTENT_JSON => json::parse(&String::from_utf8_lossy(payload)),
        CONTENT_BINARY => Ok(Json::Bytes(payload.to_vec())),
        _ => Err(format!("unknown content type: {}", content_type)),
    }
}

fn serialize_response(v: &Json) -> (u8, Vec<u8>) {
    match v {
        Json::Null => (CONTENT_JSON, b"null".to_vec()),
        Json::Str(s) => (CONTENT_TEXT, s.as_bytes().to_vec()),
        Json::Bytes(b) => (CONTENT_BINARY, b.clone()),
        other => (CONTENT_JSON, json::stringify(other).into_bytes()),
    }
}

fn expand_params(params: Json) -> Vec<Json> {
    match params {
        Json::Null => Vec::new(),
        Json::Arr(a) => a,
        other => vec![other],
    }
}

fn build_message(msg_type: u8, content_type: u8, id: &str, payload: &[u8]) -> Vec<u8> {
    let mut msg = vec![0u8; 18 + payload.len()];
    msg[0] = msg_type;
    msg[1] = content_type;
    msg[2..18].copy_from_slice(&uuid_to_bytes(id));
    msg[18..].copy_from_slice(payload);
    msg
}

/// [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary]
fn encode_binary_request(method: &str, data: &[u8], args: &[Json]) -> Vec<u8> {
    let method_bytes = method.as_bytes();
    let params_json = json::stringify(&Json::Arr(args.to_vec()));
    let params_bytes = params_json.as_bytes();
    let mut out = Vec::with_capacity(8 + method_bytes.len() + params_bytes.len() + data.len());
    out.extend_from_slice(&(method_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(method_bytes);
    out.extend_from_slice(&(params_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(params_bytes);
    out.extend_from_slice(data);
    out
}

fn decode_binary_request(raw: &[u8]) -> Result<(String, Vec<Json>), String> {
    if raw.len() < 4 {
        return Err("binary request too short".into());
    }
    let method_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    if raw.len() < 4 + method_len + 4 {
        return Err("binary request truncated".into());
    }
    let method = String::from_utf8_lossy(&raw[4..4 + method_len]).into_owned();
    let off = 4 + method_len;
    let params_len = u32::from_be_bytes([raw[off], raw[off + 1], raw[off + 2], raw[off + 3]]) as usize;
    if raw.len() < off + 4 + params_len {
        return Err("binary request params truncated".into());
    }
    let params = json::parse(&String::from_utf8_lossy(&raw[off + 4..off + 4 + params_len]))?;
    let mut args = vec![Json::Bytes(raw[off + 4 + params_len..].to_vec())];
    if let Json::Arr(a) = params {
        args.extend(a);
    }
    Ok((method, args))
}

// ---- UUID（RFC 4122 大端字节序；无 rand 依赖，用 xorshift PRNG 保证唯一性）----

static UUID_STATE: AtomicU64 = AtomicU64::new(0);
static UUID_INIT: Once = Once::new();

fn next_rand() -> u64 {
    UUID_INIT.call_once(|| {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        let seed = nanos ^ (&UUID_STATE as *const _ as u64) ^ (std::process::id() as u64) << 32;
        UUID_STATE.store(seed | 1, Ordering::SeqCst);
    });
    let mut s = UUID_STATE.load(Ordering::SeqCst);
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    UUID_STATE.store(s, Ordering::SeqCst);
    s
}

fn new_uuid() -> String {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&next_rand().to_be_bytes());
    b[8..16].copy_from_slice(&next_rand().to_be_bytes());
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant
    bytes_to_uuid(&b)
}

fn uuid_to_bytes(id: &str) -> [u8; 16] {
    let hex: String = id.chars().filter(|c| *c != '-').collect();
    let mut b = [0u8; 16];
    for i in 0..16 {
        b[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0);
    }
    b
}

fn bytes_to_uuid(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{:02x}", x)).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}