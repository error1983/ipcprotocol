//! MessageStream 帧层：长度头分帧 + 可选 CRC32 + 错误包与重同步。
//! 回调必须在 start() 之前设置；RpcProtocol 会自动处理。

use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

pub const ERR_LENGTH_CRC_FAILED: u8 = 1;
pub const ERR_MESSAGE_CRC_FAILED: u8 = 2;
pub const ERR_BUFFER_OVERFLOW: u8 = 3;

const DEFAULT_MAX_BUFFER_SIZE: usize = 256 * 1024 * 1024;

type MessageCb = Box<dyn Fn(Vec<u8>) + Send>;
type ErrorCb = Box<dyn Fn(String) + Send>;
type SyncCb = Box<dyn Fn() + Send>;

#[derive(Default)]
struct Callbacks {
    on_message: Option<MessageCb>,
    on_error: Option<ErrorCb>,
    on_sync_complete: Option<SyncCb>,
}

struct Shared {
    reader: Mutex<Option<Box<dyn Read + Send>>>,
    writer: Mutex<Box<dyn Write + Send>>,
    enable_crc: bool,
    max_buffer_size: usize,
    callbacks: Mutex<Callbacks>,
    closed: AtomicBool,
    started: AtomicBool,
}

#[derive(Clone)]
pub struct MessageStream {
    shared: Arc<Shared>,
}

impl MessageStream {
    /// 创建消息流（默认启用 CRC）。不会自动开始读取。
    pub fn new(reader: Box<dyn Read + Send>, writer: Box<dyn Write + Send>) -> Self {
        MessageStream {
            shared: Arc::new(Shared {
                reader: Mutex::new(Some(reader)),
                writer: Mutex::new(writer),
                enable_crc: true,
                max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
                callbacks: Mutex::new(Callbacks::default()),
                closed: AtomicBool::new(false),
                started: AtomicBool::new(false),
            }),
        }
    }

    pub fn set_max_buffer_size(&mut self, size: usize) {
        // 仅在 start 前调用有效（Arc 独占时）
        if let Some(s) = Arc::get_mut(&mut self.shared) {
            s.max_buffer_size = size;
        }
    }

    pub fn set_enable_crc(&mut self, enable: bool) {
        if let Some(s) = Arc::get_mut(&mut self.shared) {
            s.enable_crc = enable;
        }
    }

    pub fn on_message(&self, f: MessageCb) {
        self.shared.callbacks.lock().unwrap().on_message = Some(f);
    }

    pub fn on_error(&self, f: ErrorCb) {
        self.shared.callbacks.lock().unwrap().on_error = Some(f);
    }

    pub fn on_sync_complete(&self, f: SyncCb) {
        self.shared.callbacks.lock().unwrap().on_sync_complete = Some(f);
    }

    /// 启动读循环（幂等）。必须先设置回调。
    pub fn start(&self) {
        if self.shared.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let reader = self.shared.reader.lock().unwrap().take();
        let shared = self.shared.clone();
        thread::spawn(move || {
            if let Some(r) = reader {
                read_loop(r, shared);
            }
        });
    }

    /// 发送一个完整帧（并发安全）。
    pub fn send(&self, data: &[u8]) -> io::Result<()> {
        if self.shared.closed.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "stream closed"));
        }
        if data.len() > self.shared.max_buffer_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data size exceeds maximum buffer size",
            ));
        }
        let header_size = if self.shared.enable_crc { 8 } else { 4 };
        let mut frame = vec![0u8; header_size + data.len() + if self.shared.enable_crc { 4 } else { 0 }];
        frame[0..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
        if self.shared.enable_crc {
            let header_crc = crc32(&frame[0..4]).to_le_bytes();
            frame[4..8].copy_from_slice(&header_crc);
        }
        frame[header_size..header_size + data.len()].copy_from_slice(data);
        if self.shared.enable_crc {
            let off = header_size + data.len();
            frame[off..off + 4].copy_from_slice(&crc32(data).to_le_bytes());
        }
        let mut w = self.shared.writer.lock().unwrap();
        w.write_all(&frame)
    }

    /// 发送错误包（按正常帧封装的 EEEEEEEE+code）。
    pub fn send_error(&self, code: u8) -> io::Result<()> {
        self.send(&[0xEE, 0xEE, 0xEE, 0xEE, code])
    }

    /// 发送裸协议帧（FFFFFFFF+长度+数据，无 CRC）。
    pub fn send_protocol_message(&self, data: &[u8]) -> io::Result<()> {
        let mut frame = vec![0u8; 8 + data.len()];
        frame[0..4].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        frame[4..8].copy_from_slice(&(data.len() as u32).to_le_bytes());
        frame[8..].copy_from_slice(data);
        let mut w = self.shared.writer.lock().unwrap();
        w.write_all(&frame)
    }

    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        // 关闭底层 writer（若是 TcpStream 等可 shutdown 的类型，由调用方处理）
    }

    fn emit_error(&self, msg: &str) {
        let cb = self.shared.callbacks.lock().unwrap();
        if let Some(f) = &cb.on_error {
            f(msg.to_string());
        }
    }
}

// ---- 读循环（单线程持有缓冲区状态）----

struct ReadState {
    buf: Vec<u8>,
    head: usize, // 逻辑读游标：消费只前移游标，避免 O(n^2) 头部删除
    protocol_mode: bool,
    expected_len: Option<usize>,
}

impl ReadState {
    fn len(&self) -> usize {
        self.buf.len() - self.head
    }
    fn data(&self) -> &[u8] {
        &self.buf[self.head..]
    }
    fn consume(&mut self, n: usize) {
        self.head += n;
        if self.head >= 1024 * 1024 || self.head * 2 >= self.buf.len() {
            self.buf.drain(..self.head);
            self.head = 0;
        }
    }
    fn clear(&mut self) {
        self.buf.clear();
        self.head = 0;
    }
    fn find_protocol_header(&self) -> Option<usize> {
        let window = self.len().min(64 * 1024);
        if window < 4 {
            return None;
        }
        let d = self.data();
        (0..=window - 4).find(|&i| d[i..i + 4] == [0xFF, 0xFF, 0xFF, 0xFF])
    }
}

fn read_loop(mut reader: Box<dyn Read + Send>, shared: Arc<Shared>) {
    let mut st = ReadState { buf: Vec::new(), head: 0, protocol_mode: false, expected_len: None };
    let mut tmp = [0u8; 65536];
    loop {
        match reader.read(&mut tmp) {
            Ok(0) => {
                if !shared.closed.load(Ordering::SeqCst) {
                    emit_error(&shared, "stream ended");
                }
                return;
            }
            Ok(n) => {
                st.buf.extend_from_slice(&tmp[..n]);
                process_buffer(&mut st, &shared);
            }
            Err(e) => {
                if !shared.closed.load(Ordering::SeqCst) {
                    emit_error(&shared, &e.to_string());
                }
                return;
            }
        }
    }
}

fn emit_error(shared: &Shared, msg: &str) {
    let cb = shared.callbacks.lock().unwrap();
    if let Some(f) = &cb.on_error {
        f(msg.to_string());
    }
}

fn emit_message(shared: &Shared, data: Vec<u8>) {
    let cb = shared.callbacks.lock().unwrap();
    if let Some(f) = &cb.on_message {
        f(data);
    }
}

fn reset(st: &mut ReadState) {
    st.protocol_mode = true;
    st.clear();
    st.expected_len = None;
}

fn le32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn process_buffer(st: &mut ReadState, shared: &Arc<Shared>) {
    let ms = MessageStream { shared: shared.clone() };
    loop {
        if st.len() > shared.max_buffer_size {
            emit_error(shared, "buffer overflow");
            let _ = ms.send_error(ERR_BUFFER_OVERFLOW);
            reset(st);
            return;
        }
        if st.protocol_mode {
            // 扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM）
            if let Some(idx) = st.find_protocol_header() {
                st.consume(idx);
                if st.len() < 8 {
                    return; // 等待长度字段
                }
                let length = le32(&st.data()[4..8]);
                if length > 50 {
                    st.consume(8); // 伪头：跳过继续扫描
                    continue;
                }
                let length = length as usize;
                if st.len() < 8 + length {
                    return; // 协议消息未收全
                }
                let text = String::from_utf8_lossy(&st.data()[8..8 + length]).trim().to_string();
                st.consume(8 + length);
                if text == "RESET_CONFIRM" {
                    st.protocol_mode = false;
                    st.clear();
                    let cb = shared.callbacks.lock().unwrap();
                    if let Some(f) = &cb.on_sync_complete {
                        f();
                    }
                }
                continue;
            }
            // 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头）
            let keep = st.len().min(3);
            st.consume(st.len() - keep);
            return;
        }

        let header_size = if shared.enable_crc { 8 } else { 4 };
        if st.expected_len.is_none() {
            if st.len() < header_size {
                return;
            }
            let length = le32(&st.data()[0..4]);
            if shared.enable_crc {
                let expected = le32(&st.data()[4..8]);
                if crc32(&st.data()[0..4]) != expected {
                    let _ = ms.send_error(ERR_LENGTH_CRC_FAILED);
                    emit_error(shared, "length header CRC check failed");
                    reset(st);
                    return;
                }
            }
            // 长度头强制校验：0 或超限视为坏帧（CRC 合法也可能是伪造/损坏的长度）
            if length == 0 || length as u64 > shared.max_buffer_size as u64 {
                let code = if length as u64 > shared.max_buffer_size as u64 {
                    ERR_BUFFER_OVERFLOW
                } else {
                    ERR_LENGTH_CRC_FAILED
                };
                let _ = ms.send_error(code);
                emit_error(shared, &format!("invalid frame length {}", length));
                reset(st);
                return;
            }
            st.expected_len = Some(length as usize);
            st.consume(header_size);
        }

        let expected_len = st.expected_len.unwrap();
        let total_size = expected_len + if shared.enable_crc { 4 } else { 0 };
        if st.len() < total_size {
            return;
        }
        let data = st.data()[..expected_len].to_vec();
        if shared.enable_crc {
            let expected = le32(&st.data()[expected_len..expected_len + 4]);
            if crc32(&data) != expected {
                let _ = ms.send_error(ERR_MESSAGE_CRC_FAILED);
                emit_error(shared, "message CRC check failed");
                reset(st);
                return;
            }
        }
        st.consume(total_size);
        st.expected_len = None;
        // 错误包可能 reset 清空缓冲区，故先消费再分发
        if data.len() == 5 && data[0..4] == [0xEE, 0xEE, 0xEE, 0xEE] {
            reset(st);
            let _ = ms.send_protocol_message(b"RESET_CONFIRM");
            st.protocol_mode = false;
        } else {
            emit_message(shared, data);
        }
        if st.protocol_mode {
            return;
        }
    }
}

// ---- CRC32（反射多项式 0xEDB88320，与 zlib 一致）----

pub fn crc32(data: &[u8]) -> u32 {
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, entry) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB88320 ^ (c >> 1) } else { c >> 1 };
            }
            *entry = c;
        }
        t
    });
    let mut crc = 0xFFFFFFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFFFFFF
}