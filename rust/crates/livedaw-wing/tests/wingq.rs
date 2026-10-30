//! WING-Q (Behringer's personal monitoring app) on Sidecar: the Native
//! protocol over a WebSocket on TCP 2222, as recorded from the app (iPhone,
//! iOS 18.7, October 2026), next to plain Native clients, and under load.
//!
//! The stress test runs for WINGQ_STRESS_SECS seconds (default 20).

use livedaw_engine::{Engine, StripKind};
use livedaw_wing::native::{self, Deframer, Event};
use livedaw_wing::server::{CloseReason, Config, EngineHandle, WingServer};
use livedaw_wing::{schema, Identity, Value, WingState};
use std::io::{ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PATIENCE: Duration = Duration::from_secs(30);

/// WING-Q's request head, as captured.
const WINGQ_REQUEST: &str = "GET / HTTP/1.1\r\nHost: 192.168.100.247:2222\r\nUser-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148\r\nCache-Control: no-cache\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Extensions: permessage-deflate\r\nOrigin: null\r\nPragma: no-cache\r\nAccept: */*\r\nAccept-Language: en-US,en;q=0.9\r\nPriority: u=3, i\r\nAccept-Encoding: gzip, deflate\r\nSec-WebSocket-Key: kSupAm7XAvs1TNerJqcWNw==\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
/// What a server must answer to that key (RFC 6455's computation).
const WINGQ_ACCEPT: &str = "Sec-WebSocket-Accept: ";

fn start(name: &str) -> (Arc<Engine>, WingServer) {
    let e = Arc::new(Engine::new(0));
    e.set_num_channels(40);
    let config = Config {
        identity: Identity { name: name.into(), ..Identity::default() },
        bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
        discovery_port: 0,
        osc_port: 0,
        native_port: 0,
        ..Config::default()
    };
    let handle: Arc<dyn EngineHandle> = Arc::new(e.clone());
    let server = WingServer::start(handle, Arc::new(WingState::default()), &config);
    assert!(server.status().errors.is_empty(), "{:?}", server.status().errors);
    (e, server)
}

fn id(path: &str) -> u32 {
    let s = schema();
    s.id(s.resolve(path, &livedaw_wing::schema::NoModels).unwrap_or_else(|| panic!("{path}")))
}

fn set_tokens(id: u32, v: f32) -> Vec<u8> {
    let mut t = Vec::new();
    native::encode_data(&mut t, id, &Value::Float(v));
    t
}

/// A masked client frame.
fn client_frame(opcode: u8, fin: bool, payload: &[u8], mask: [u8; 4]) -> Vec<u8> {
    let mut f = vec![if fin { 0x80 } else { 0 } | opcode];
    if payload.len() < 126 {
        f.push(0x80 | payload.len() as u8);
    } else if payload.len() <= 0xffff {
        f.push(0x80 | 126);
        f.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        f.push(0x80 | 127);
        f.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    f.extend_from_slice(&mask);
    f.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    f
}

/// A small deterministic random source.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// A client speaking as WING-Q: a WebSocket carrying the Native stream.
struct WingQ {
    tcp: TcpStream,
    /// Server bytes not yet decoded as frames.
    raw: Vec<u8>,
    de: Deframer,
    /// The Native Audio Engine channel's bytes received.
    data: Vec<u8>,
    closed: bool,
    pongs: usize,
    rng: Rng,
}

impl WingQ {
    fn connect(addr: SocketAddr, seed: u64) -> Self {
        Self::try_connect(addr, seed).expect("closed during the handshake")
    }

    /// None if the server closed the connection instead of answering (it
    /// serves MAX_NATIVE_CLIENTS at once, as the console does).
    fn try_connect(addr: SocketAddr, seed: u64) -> Option<Self> {
        let mut tcp = TcpStream::connect(addr).unwrap();
        tcp.set_nodelay(true).unwrap();
        tcp.write_all(WINGQ_REQUEST.as_bytes()).unwrap();
        tcp.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        let mut c =
            Self { tcp, raw: Vec::new(), de: Deframer::default(), data: Vec::new(), closed: false, pongs: 0, rng: Rng(seed | 1) };
        // The response head.
        let deadline = Instant::now() + PATIENCE;
        let mut buf = [0u8; 4096];
        loop {
            if let Some(p) = c.raw.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8(c.raw[..p + 4].to_vec()).unwrap();
                assert!(head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"), "{head}");
                assert!(head.contains(WINGQ_ACCEPT), "{head}");
                c.raw.drain(..p + 4);
                break;
            }
            assert!(Instant::now() < deadline, "no handshake response");
            match c.tcp.read(&mut buf) {
                Ok(0) => return None,
                Ok(n) => c.raw.extend_from_slice(&buf[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(_) => return None,
            }
        }
        Some(c)
    }

    fn mask(&mut self) -> [u8; 4] {
        (self.rng.next() as u32).to_be_bytes()
    }

    /// Sends one binary message, written in random pieces.
    fn send(&mut self, payload: &[u8]) {
        let m = self.mask();
        let f = client_frame(2, true, payload, m);
        self.write_split(&f);
    }

    fn write_split(&mut self, f: &[u8]) {
        let mut at = 0;
        while at < f.len() {
            let n = (1 + self.rng.below(f.len() as u64) as usize).min(f.len() - at);
            if self.tcp.write_all(&f[at..at + n]).is_err() {
                self.closed = true;
                return;
            }
            at += n;
        }
    }

    /// Sends a message as fragments (and a ping between them).
    fn send_fragmented(&mut self, payload: &[u8]) {
        let half = payload.len() / 2;
        let (m1, m2, m3) = (self.mask(), self.mask(), self.mask());
        let mut f = client_frame(2, false, &payload[..half], m1);
        f.extend(client_frame(9, true, b"keepalive", m2));
        f.extend(client_frame(0, true, &payload[half..], m3));
        self.write_split(&f);
    }

    /// Reads what's there (up to the read timeout).
    fn pump(&mut self) {
        let mut buf = vec![0u8; 65536];
        match self.tcp.read(&mut buf) {
            Ok(0) => self.closed = true,
            Ok(n) => self.raw.extend_from_slice(&buf[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(_) => self.closed = true,
        }
        // Server frames: unmasked; binary carries the Native stream.
        loop {
            if self.raw.len() < 2 {
                break;
            }
            assert_eq!(self.raw[1] & 0x80, 0, "server frames are never masked");
            let (len, at) = match self.raw[1] & 0x7f {
                126 if self.raw.len() >= 4 => (u16::from_be_bytes([self.raw[2], self.raw[3]]) as usize, 4),
                127 if self.raw.len() >= 10 => (u64::from_be_bytes(self.raw[2..10].try_into().unwrap()) as usize, 10),
                126 | 127 => break,
                n => (n as usize, 2),
            };
            if self.raw.len() < at + len {
                break;
            }
            let op = self.raw[0] & 0x0f;
            assert_ne!(self.raw[0] & 0x80, 0, "server frames are whole");
            let payload: Vec<u8> = self.raw[at..at + len].to_vec();
            self.raw.drain(..at + len);
            match op {
                2 => {
                    let mut ev = Vec::new();
                    for &b in &payload {
                        self.de.push(b, &mut ev);
                    }
                    for e in ev {
                        if let Event::Data(native::CH_AUDIO, b) = e {
                            self.data.push(b);
                        }
                    }
                }
                10 => self.pongs += 1,
                8 => self.closed = true,
                other => panic!("unexpected server opcode {other}"),
            }
        }
    }

    fn wait_for(&mut self, what: &str, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + PATIENCE;
        while !done(self) {
            assert!(!self.closed, "{what}: closed");
            assert!(Instant::now() < deadline, "{what}: timed out");
            self.pump();
        }
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// A plain Native client (WING Edit's way).
struct Native {
    tcp: TcpStream,
    de: Deframer,
    data: Vec<u8>,
}

impl Native {
    fn connect(addr: SocketAddr, speak_first: bool) -> Self {
        let mut tcp = TcpStream::connect(addr).unwrap();
        tcp.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        if speak_first {
            tcp.write_all(&[0xdf, 0xd1]).unwrap();
        }
        Self { tcp, de: Deframer::default(), data: Vec::new() }
    }
    fn send(&mut self, tokens: &[u8]) {
        self.tcp.write_all(&native::frame(native::CH_AUDIO, tokens)).unwrap();
    }
    fn pump(&mut self) -> bool {
        let mut buf = vec![0u8; 65536];
        match self.tcp.read(&mut buf) {
            Ok(0) => return false,
            Ok(n) => {
                let mut ev = Vec::new();
                for &b in &buf[..n] {
                    self.de.push(b, &mut ev);
                }
                for e in ev {
                    if let Event::Data(native::CH_AUDIO, b) = e {
                        self.data.push(b);
                    }
                }
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(_) => return false,
        }
        true
    }
    fn wait_for(&mut self, what: &str, mut done: impl FnMut(&[u8]) -> bool) {
        let deadline = Instant::now() + PATIENCE;
        while !done(&self.data) {
            assert!(Instant::now() < deadline, "{what}: timed out");
            assert!(self.pump(), "{what}: closed");
        }
    }
}

fn fader(e: &Engine, ch: usize) -> f32 {
    e.strip_at(StripKind::Channel, ch).unwrap().params.fader_db.load()
}

fn wait_until(what: &str, f: impl FnMut() -> bool) {
    wait_until_or(what, f, String::new);
}

/// As wait_until; `why` says what was there instead when it times out.
fn wait_until_or(what: &str, mut f: impl FnMut() -> bool, why: impl Fn() -> String) {
    let deadline = Instant::now() + PATIENCE;
    while !f() {
        assert!(Instant::now() < deadline, "{what}: timed out {}", why());
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn wingq_connects_and_reads_the_console() {
    let (_e, server) = start("WINGQ-TEST");
    let addr = server.status().native.unwrap();
    let mut q = WingQ::connect(addr, 1);
    // WING-Q's first messages of a session, as captured: the channel
    // selection, then ROOT DATA? (the whole tree).
    q.send(&[0xdf, 0xd1]);
    q.send(&[0xda, 0xdc]);
    q.wait_for("the tree", |q| contains(&q.data, b"WINGQ-TEST") && q.data.len() > 100_000);
    // A ping is answered.
    let m = q.mask();
    let ping = client_frame(9, true, b"are you there", m);
    q.write_split(&ping);
    q.wait_for("a pong", |q| q.pongs == 1);
    // A clean close is answered and ends the connection.
    let m = q.mask();
    let close = client_frame(8, true, &[0x03, 0xe8], m);
    q.write_split(&close);
    let deadline = Instant::now() + PATIENCE;
    while !q.closed {
        assert!(Instant::now() < deadline, "the close: timed out");
        q.pump();
    }
    wait_until("the client is forgotten", || server.status().clients.is_empty());
    let d = server.status().disconnects;
    assert_eq!(d.last().map(|d| d.reason.clone()), Some(CloseReason::ClientClosed), "{d:?}");
}

#[test]
fn wingq_and_native_clients_see_each_others_changes() {
    let (e, server) = start("WINGQ-TEST");
    let addr = server.status().native.unwrap();
    let mut q = WingQ::connect(addr, 2);
    q.send(&[0xdf, 0xd1]);
    let mut n = Native::connect(addr, true);
    // Both registered (a get answered).
    let get = |id: u32| {
        let mut t = vec![0xd7];
        t.extend_from_slice(&id.to_be_bytes());
        t.push(0xdc);
        t
    };
    let ch5 = id("/ch/5/fdr");
    q.send(&get(ch5));
    q.wait_for("the get", |q| contains(&q.data, &ch5.to_be_bytes()));
    n.send(&get(ch5));
    n.wait_for("the get", |d| contains(d, &ch5.to_be_bytes()));
    q.data.clear();
    n.data.clear();

    // WING-Q moves channel 1's fader: the engine follows, WING Edit hears it.
    let ch1 = id("/ch/1/fdr");
    q.send(&set_tokens(ch1, -12.5));
    wait_until("the engine", || (fader(&e, 0) - -12.5).abs() < 0.01);
    n.wait_for("the change", |d| contains(d, &ch1.to_be_bytes()));
    // As fragments, too.
    q.send_fragmented(&set_tokens(ch1, -20.0));
    wait_until("the engine", || (fader(&e, 0) - -20.0).abs() < 0.01);
    // WING Edit moves channel 2's: WING-Q hears it, framed.
    let ch2 = id("/ch/2/fdr");
    n.send(&set_tokens(ch2, -6.0));
    wait_until("the engine", || (fader(&e, 1) - -6.0).abs() < 0.01);
    q.wait_for("the change", |q| contains(&q.data, &ch2.to_be_bytes()));
    // The UI moves channel 3's: both hear it.
    let ch3 = id("/ch/3/fdr");
    e.strip_at(StripKind::Channel, 2).unwrap().params.fader_db.store(-3.0);
    q.wait_for("the UI's change", |q| contains(&q.data, &ch3.to_be_bytes()));
    n.wait_for("the UI's change", |d| contains(d, &ch3.to_be_bytes()));
}

#[test]
fn a_native_client_that_listens_first_still_gets_changes() {
    let (e, server) = start("WINGQ-TEST");
    let addr = server.status().native.unwrap();
    let mut n = Native::connect(addr, false);
    // Registered (the server knows it once the grace for a WebSocket's
    // request is over), then a UI change reaches it.
    wait_until("registered", || server.status().clients.len() == 1);
    std::thread::sleep(Duration::from_millis(1300));
    let ch4 = id("/ch/4/fdr");
    e.strip_at(StripKind::Channel, 3).unwrap().params.fader_db.store(-9.0);
    n.wait_for("the change", |d| contains(d, &ch4.to_be_bytes()));
}

fn http(addr: SocketAddr, request: &[u8]) -> String {
    let mut tcp = TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(PATIENCE)).unwrap();
    tcp.write_all(request).unwrap();
    let mut resp = Vec::new();
    let _ = tcp.read_to_end(&mut resp);
    String::from_utf8_lossy(&resp).into_owned()
}

#[test]
fn other_http_requests_are_answered_and_closed() {
    let (_e, server) = start("WINGQ-TEST");
    let addr = server.status().native.unwrap();
    assert!(http(addr, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").starts_with("HTTP/1.1 426 Upgrade Required\r\n"));
    assert!(http(addr, b"GET /index.html HTTP/1.0\r\n\r\n").starts_with("HTTP/1.1 426"));
    let mut huge = b"GET / HTTP/1.1\r\nX: ".to_vec();
    huge.resize(10_000, b'a');
    assert!(http(addr, &huge).starts_with("HTTP/1.1 431"));
    // A version-8 WebSocket is refused.
    let old = WINGQ_REQUEST.replace("Sec-WebSocket-Version: 13", "Sec-WebSocket-Version: 8");
    assert!(http(addr, old.as_bytes()).starts_with("HTTP/1.1 426"));
    wait_until("all forgotten", || server.status().clients.is_empty());
    // The server still serves WING-Q.
    let mut q = WingQ::connect(addr, 3);
    q.send(&[0xdf, 0xd1, 0xda, 0xdc]);
    q.wait_for("the tree", |q| contains(&q.data, b"WINGQ-TEST"));
}

#[test]
fn a_broken_websocket_client_only_closes_itself() {
    let (e, server) = start("WINGQ-TEST");
    let addr = server.status().native.unwrap();
    let mut good = WingQ::connect(addr, 4);
    good.send(&[0xdf, 0xd1]);
    for bad in [
        vec![0x82, 0x02, 0xdf, 0xd1],              // not masked
        client_frame(3, true, b"x", [1, 2, 3, 4]), // unknown opcode
        client_frame(0, true, b"x", [1, 2, 3, 4]), // a continuation out of nowhere
        {
            let mut f = vec![0x82, 0xff]; // 4 GB announced
            f.extend_from_slice(&(1u64 << 32).to_be_bytes());
            f.extend_from_slice(&[0; 4]);
            f
        },
    ] {
        let mut q = WingQ::connect(addr, 5);
        q.tcp.write_all(&bad).unwrap();
        let deadline = Instant::now() + PATIENCE;
        while !q.closed {
            assert!(Instant::now() < deadline, "not closed for {bad:02x?}");
            q.pump();
        }
    }
    wait_until("the bad ones are forgotten", || server.status().clients.len() == 1);
    let reasons: Vec<_> = server.status().disconnects.into_iter().map(|d| d.reason).collect();
    assert_eq!(reasons.iter().filter(|r| matches!(r, CloseReason::Protocol(_))).count(), 4, "{reasons:?}");
    let ch1 = id("/ch/1/fdr");
    good.send(&set_tokens(ch1, -30.0));
    wait_until("the good one still works", || (fader(&e, 0) - -30.0).abs() < 0.01);
}

#[test]
fn stress_wingq_clients_with_native_ones_and_churn() {
    // Shorter under ThreadSanitizer (CI), which is several times slower.
    // ThreadSanitizer (CI) runs everything several times slower: a shorter
    // run, and clients that send at a pace it can serve (else the server
    // rightly drops clients that can't keep up with what it sends them).
    let tsan = std::env::var_os("TSAN_OPTIONS").is_some();
    let default_secs = if tsan { 8 } else { 20 };
    let pace: u64 = if tsan { 20 } else { 1 };
    let secs: u64 = std::env::var("WINGQ_STRESS_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(default_secs);
    let (e, server) = start("WINGQ-STRESS");
    let addr = server.status().native.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sets = Arc::new(AtomicU64::new(0));
    let churned = Arc::new(AtomicU64::new(0));
    const WINGQS: usize = 10;
    const NATIVES: usize = 3;

    // Each WING-Q client owns a channel: it moves that fader all the time
    // (whole messages, fragments, pings), asks for the whole tree now and
    // then, and keeps reading. At the end its channel holds its last value.
    let mut handles = Vec::new();
    for k in 0..WINGQS {
        let (stop, sets, e) = (stop.clone(), sets.clone(), e.clone());
        handles.push(std::thread::spawn(move || {
            let mut q = WingQ::connect(addr, 100 + k as u64);
            q.send(&[0xdf, 0xd1]);
            let ids: Vec<u32> = (0..WINGQS).map(|c| id(&format!("/ch/{}/fdr", c + 1))).collect();
            let my = ids[k];
            let mut last = 0.0f32;
            let mut heard_others = std::collections::HashSet::new();
            let mut i = 0u64;
            while !stop.load(Relaxed) {
                i += 1;
                last = -((q.rng.below(600) as f32) / 10.0);
                let t = set_tokens(my, last);
                match q.rng.below(10) {
                    0 => q.send_fragmented(&t),
                    1 => {
                        // Several requests in one message, as WING-Q batches them.
                        let mut batch = t.clone();
                        batch.extend(set_tokens(my, last));
                        q.send(&batch);
                    }
                    _ => q.send(&t),
                }
                sets.fetch_add(1, Relaxed);
                if i % 400 == 0 {
                    q.send(&[0xda, 0xdc]); // ROOT DATA?: ~230 KB back
                }
                if i % 50 == 0 {
                    let m = q.mask();
                    let ping = client_frame(9, true, b"k", m);
                    q.write_split(&ping);
                }
                q.pump();
                assert!(!q.closed, "WING-Q client {k} was closed");
                if i % 100 == 0 {
                    for (other, oid) in ids.iter().enumerate() {
                        if other != k && contains(&q.data, &oid.to_be_bytes()) {
                            heard_others.insert(other);
                        }
                    }
                    q.data.clear();
                }
                std::thread::sleep(Duration::from_micros((500 + q.rng.below(1500)) * pace));
            }
            // Keep reading until the last move is applied: under this load
            // the server reads a client's requests only while less than
            // 1 MB waits for it (TCP backpressure, as on the console), so
            // leaving at once could drop requests still in the socket.
            let until = Instant::now() + PATIENCE;
            while (fader(&e, k) - last).abs() >= 0.01 && Instant::now() < until {
                assert!(!q.closed, "WING-Q client {k} was closed before its last move arrived");
                q.pump();
                q.data.clear();
            }
            (k, last, heard_others.len(), q.pongs)
        }));
    }
    // WING Edit-like clients moving channels 21-23 and reading everything.
    let mut natives = Vec::new();
    for k in 0..NATIVES {
        let (stop, sets, e) = (stop.clone(), sets.clone(), e.clone());
        natives.push(std::thread::spawn(move || {
            let mut n = Native::connect(addr, true);
            let my = id(&format!("/ch/{}/fdr", 21 + k));
            let mut last = 0.0;
            let mut i = 0u64;
            while !stop.load(Relaxed) {
                i += 1;
                last = -((i % 500) as f32) / 10.0;
                n.send(&set_tokens(my, last));
                sets.fetch_add(1, Relaxed);
                assert!(n.pump(), "native client {k} was closed");
                if n.data.len() > 1 << 20 {
                    n.data.clear();
                }
                std::thread::sleep(Duration::from_millis(2 * pace));
            }
            // As the WING-Q clients: stay until the last move is applied.
            let until = Instant::now() + PATIENCE;
            while (fader(&e, 20 + k) - last).abs() >= 0.01 && Instant::now() < until {
                n.pump();
                n.data.clear();
            }
            (20 + k, last)
        }));
    }
    // Churn: WING-Q connections that come and go (dropped mid-message,
    // half-sent handshakes, clean closes), as a phone on Wi-Fi does.
    let refused = Arc::new(AtomicU64::new(0));
    let churn = {
        let (stop, churned, refused) = (stop.clone(), churned.clone(), refused.clone());
        let sh = server.shared().clone();
        std::thread::spawn(move || {
            let mut rng = Rng(77);
            let connect = |seed: u64| {
                let q = WingQ::try_connect(addr, seed);
                if q.is_none() {
                    refused.fetch_add(1, Relaxed);
                    eprintln!("churn: refused with {} clients connected", sh.status().0.len());
                }
                q
            };
            while !stop.load(Relaxed) {
                match rng.below(4) {
                    0 => {
                        // Half a handshake, then gone.
                        let mut t = TcpStream::connect(addr).unwrap();
                        let _ = t.write_all(&WINGQ_REQUEST.as_bytes()[..100]);
                        let _ = t.shutdown(Shutdown::Both);
                    }
                    1 => {
                        // Half a frame, then gone.
                        let Some(mut q) = connect(rng.next()) else { continue };
                        let f = client_frame(2, true, &[0xdf, 0xd1, 0xda, 0xdc], [9, 9, 9, 9]);
                        let _ = q.tcp.write_all(&f[..3]);
                        let _ = q.tcp.shutdown(Shutdown::Both);
                    }
                    2 => {
                        // Asks for the tree and leaves without reading it.
                        let Some(mut q) = connect(rng.next()) else { continue };
                        q.send(&[0xdf, 0xd1, 0xda, 0xdc]);
                        drop(q);
                    }
                    _ => {
                        // A clean session.
                        let Some(mut q) = connect(rng.next()) else { continue };
                        q.send(&[0xdf, 0xd1]);
                        q.send(&set_tokens(id("/ch/40/fdr"), -40.0));
                        let m = q.mask();
                        let close = client_frame(8, true, &[], m);
                        q.write_split(&close);
                        let until = Instant::now() + Duration::from_millis(300);
                        while !q.closed && Instant::now() < until {
                            q.pump();
                        }
                    }
                }
                churned.fetch_add(1, Relaxed);
                std::thread::sleep(Duration::from_millis(5 * pace));
            }
        })
    };

    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(secs) {
        std::thread::sleep(Duration::from_millis(250));
        // The UI moves faders too (changes pushed to everyone).
        let i = (t0.elapsed().as_millis() / 250) as usize;
        e.strip_at(StripKind::Channel, 30 + i % 8).unwrap().params.fader_db.store(-((i % 60) as f32));
    }
    stop.store(true, Relaxed);
    churn.join().unwrap();
    let mut results = Vec::new();
    for h in handles {
        results.push(h.join().unwrap());
    }
    let mut native_results = Vec::new();
    for h in natives {
        native_results.push(h.join().unwrap());
    }

    // Every client's last move is what the engine holds.
    for &(k, last, heard, pongs) in &results {
        wait_until_or(
            &format!("channel {} = {last}", k + 1),
            || (fader(&e, k) - last).abs() < 0.01,
            || format!("(it is {})", fader(&e, k)),
        );
        assert!(heard >= WINGQS - 1 - 2, "WING-Q client {k} heard only {heard} of the other clients' channels");
        assert!(pongs > 0, "WING-Q client {k} got no pongs");
    }
    for &(ch, last) in &native_results {
        wait_until(&format!("channel {} = {last}", ch + 1), || (fader(&e, ch) - last).abs() < 0.01);
    }
    // Nothing left behind: every connection is gone, none for a reason other
    // than the client leaving (or its protocol misbehaving on purpose).
    wait_until_or("all clients forgotten", || server.status().clients.is_empty(), || format!("{:?}", server.status().clients));
    let bad: Vec<_> = server
        .status()
        .disconnects
        .into_iter()
        .filter(|d| !matches!(d.reason, CloseReason::ClientClosed | CloseReason::ReadError(..) | CloseReason::WriteError(..)))
        .collect();
    assert!(bad.is_empty(), "unexpected disconnects: {bad:?}");
    // A refusal only happens at the 16-client limit, while connections the
    // churn just dropped are still being noticed: rare.
    let (refused, churned_n) = (refused.load(Relaxed), churned.load(Relaxed));
    assert!(refused * 10 <= churned_n.max(10), "{refused} of {churned_n} churned connections refused");
    let total = sets.load(Relaxed);
    eprintln!(
        "stress: {secs} s, {total} sets ({:.0}/s) from {WINGQS} WING-Q + {NATIVES} Native clients, {} churned connections",
        total as f64 / secs as f64,
        churned.load(Relaxed)
    );
    // A floor slow CI machines (debug builds, sanitizers) still clear.
    assert!(total > 50 * secs, "too few sets went through: {total}");
}
