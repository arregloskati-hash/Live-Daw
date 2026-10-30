//! The Native protocol over a WebSocket, as WING-Q uses it.
//!
//! WING-Q (Behringer's personal monitoring app, a web view) can't open a raw
//! TCP connection: it opens a WebSocket on the Native port (TCP 2222,
//! `GET /` with `Upgrade: websocket`) and sends the Native byte stream in
//! binary messages, one or more requests each; the console answers the same
//! way. Captured from WING-Q 2026-10: the requests and the answers are the
//! Native protocol's bytes unchanged, so the console's side only adds the
//! handshake and the framing. A connection whose first byte isn't `G` is a
//! plain Native client, as before.
//!
//! The handshake's accept key and the server frame header come from the web
//! remote's WebSocket code (`livedaw_web::ws`); the reader here is
//! incremental, as the Native reader is (on Windows its socket doesn't block).

use livedaw_web::ws::{accept_key, frame_header, Opcode};

/// The request head may be this long at most (WING-Q's is under 500 bytes).
pub const MAX_REQUEST_HEAD: usize = 8 * 1024;
/// Largest message accepted from a client (WING-Q's are a few bytes; a whole
/// snapshot sent as one message stays far below this).
pub const MAX_MESSAGE: usize = 1 << 20;
/// Server messages carry at most this much of the Native stream each.
pub const MAX_FRAME_PAYLOAD: usize = 64 << 10;

/// What the first bytes of a connection on the Native port are.
#[derive(Debug, PartialEq, Eq)]
pub enum Sniff {
    /// Native protocol bytes (anything but an HTTP request).
    Native,
    /// An HTTP request whose head (up to and including the blank line) is
    /// this many bytes.
    Http(usize),
    /// An HTTP request whose head hasn't all arrived yet.
    NeedMore,
    /// An HTTP request head longer than [`MAX_REQUEST_HEAD`].
    TooLong,
}

pub fn sniff(bytes: &[u8]) -> Sniff {
    // A Native client starts with a channel selection (0xdf) or Audio Engine
    // tokens, never with ASCII 'G'; an HTTP request starts with "GET ".
    let probe = b"GET ";
    let n = bytes.len().min(probe.len());
    if n == 0 || bytes[..n] != probe[..n] {
        return if bytes.is_empty() { Sniff::NeedMore } else { Sniff::Native };
    }
    match bytes.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(p) => Sniff::Http(p + 4),
        None if bytes.len() > MAX_REQUEST_HEAD => Sniff::TooLong,
        None => Sniff::NeedMore,
    }
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n").skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

fn has_token(value: Option<&str>, token: &str) -> bool {
    value.is_some_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
}

/// The answer to an HTTP request head: `Ok` with the `101 Switching Protocols`
/// response for a WebSocket upgrade, `Err` with an error response (the
/// connection then closes).
pub fn handshake(head: &[u8]) -> Result<Vec<u8>, Vec<u8>> {
    let reject = |status: &str, extra: &str| {
        Err(format!("HTTP/1.1 {status}\r\n{extra}Content-Length: 0\r\nConnection: close\r\n\r\n").into_bytes())
    };
    let Ok(head) = std::str::from_utf8(head) else { return reject("400 Bad Request", "") };
    let request_line = head.split("\r\n").next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let (method, _path, version) = (parts.next(), parts.next(), parts.next());
    if method != Some("GET") || !version.is_some_and(|v| v.starts_with("HTTP/1.")) {
        return reject("400 Bad Request", "");
    }
    // Any path: WING-Q asks for "/".
    if !has_token(header(head, "Upgrade"), "websocket") || !has_token(header(head, "Connection"), "upgrade") {
        return reject("426 Upgrade Required", "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n");
    }
    if header(head, "Sec-WebSocket-Version") != Some("13") {
        return reject("426 Upgrade Required", "Sec-WebSocket-Version: 13\r\n");
    }
    let Some(key) = header(head, "Sec-WebSocket-Key").filter(|k| !k.is_empty()) else {
        return reject("400 Bad Request", "");
    };
    // No extension (permessage-deflate is offered, not needed) and no
    // subprotocol: WING-Q asks for neither.
    Ok(format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(key)
    )
    .into_bytes())
}

/// Server frames (binary, unmasked) carrying `data`, split into messages of
/// at most [`MAX_FRAME_PAYLOAD`]: the Native stream doesn't care where a
/// message ends (WING-Q reassembles it as a console's own stream).
pub fn encode_binary(data: &[u8], out: &mut Vec<u8>) {
    for chunk in data.chunks(MAX_FRAME_PAYLOAD) {
        out.extend_from_slice(&frame_header(Opcode::Binary, chunk.len()));
        out.extend_from_slice(chunk);
    }
}

/// A control frame from the server.
pub fn control_frame(opcode: Opcode, payload: &[u8]) -> Vec<u8> {
    let payload = &payload[..payload.len().min(125)];
    let mut f = frame_header(opcode, payload.len());
    f.extend_from_slice(payload);
    f
}

/// What a client's frames amount to.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    /// Native protocol bytes (a data frame's payload, text or binary).
    Data(Vec<u8>),
    /// A ping: answer with a pong carrying this.
    Ping(Vec<u8>),
    /// The client closes the connection.
    Close,
    /// A protocol error: close the connection.
    Error(&'static str),
}

/// Decodes a client's frames as they arrive, a byte stream at a time.
#[derive(Default)]
pub struct Decoder {
    buf: Vec<u8>,
    /// Inside a fragmented message: what it has carried so far.
    fragmented: Option<usize>,
    failed: bool,
}

impl Decoder {
    /// Feeds `bytes`, appending what they complete to `out`. After an
    /// `Error` or a `Close` the rest is ignored.
    pub fn push(&mut self, bytes: &[u8], out: &mut Vec<Event>) {
        if self.failed {
            return;
        }
        let mut buf = std::mem::take(&mut self.buf);
        buf.extend_from_slice(bytes);
        let mut at = 0;
        while let Some((event, used)) = self.next(&buf[at..]) {
            at += used;
            let end = matches!(event, Some(Event::Close) | Some(Event::Error(_)));
            if let Some(e) = event {
                out.push(e);
            }
            if end {
                self.failed = true;
                return;
            }
        }
        buf.drain(..at);
        self.buf = buf;
    }

    /// The next frame in `b`: its event (None for a pong or an empty data
    /// frame) and its length, or None if it hasn't all arrived.
    fn next(&mut self, b: &[u8]) -> Option<(Option<Event>, usize)> {
        if b.len() < 2 {
            return None;
        }
        let fin = b[0] & 0x80 != 0;
        if b[0] & 0x70 != 0 {
            return Some((Some(Event::Error("reserved bits set")), 0));
        }
        let opcode = b[0] & 0x0f;
        let masked = b[1] & 0x80 != 0;
        let (len, mut at) = match b[1] & 0x7f {
            126 => {
                if b.len() < 4 {
                    return None;
                }
                (u16::from_be_bytes([b[2], b[3]]) as u64, 4)
            }
            127 => {
                if b.len() < 10 {
                    return None;
                }
                (u64::from_be_bytes(b[2..10].try_into().unwrap()), 10)
            }
            n => (n as u64, 2),
        };
        if !masked {
            // RFC 6455 5.1: a server must close on an unmasked client frame.
            return Some((Some(Event::Error("client frame not masked")), 0));
        }
        let control = opcode >= 8;
        if control && (len > 125 || !fin) {
            return Some((Some(Event::Error("bad control frame")), 0));
        }
        let carried = self.fragmented.unwrap_or(0) as u64;
        if len > MAX_MESSAGE as u64 || carried + len > MAX_MESSAGE as u64 {
            return Some((Some(Event::Error("message too big")), 0));
        }
        if b.len() < at + 4 + len as usize {
            return None;
        }
        let mask = [b[at], b[at + 1], b[at + 2], b[at + 3]];
        at += 4;
        let payload: Vec<u8> = b[at..at + len as usize].iter().enumerate().map(|(i, x)| x ^ mask[i % 4]).collect();
        let used = at + len as usize;
        let event = match opcode {
            0x8 => Some(Event::Close),
            0x9 => Some(Event::Ping(payload)),
            0xa => None,
            0x0 => {
                let Some(so_far) = self.fragmented else {
                    return Some((Some(Event::Error("continuation without a start")), used));
                };
                self.fragmented = if fin { None } else { Some(so_far + payload.len()) };
                (!payload.is_empty()).then_some(Event::Data(payload))
            }
            0x1 | 0x2 => {
                if self.fragmented.is_some() {
                    return Some((Some(Event::Error("new message inside a fragmented one")), used));
                }
                if !fin {
                    self.fragmented = Some(payload.len());
                }
                (!payload.is_empty()).then_some(Event::Data(payload))
            }
            _ => Some(Event::Error("unknown opcode")),
        };
        Some((event, used))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A client frame as browsers send it (masked).
    pub fn client_frame(opcode: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
        let mask = [0x37, 0xfa, 0x21, 0x3d];
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

    /// WING-Q's request head, as captured (iPhone, iOS 18.7).
    pub const WINGQ_REQUEST: &str = "GET / HTTP/1.1\r\nHost: 192.168.100.247:2222\r\nUser-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148\r\nCache-Control: no-cache\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Extensions: permessage-deflate\r\nOrigin: null\r\nPragma: no-cache\r\nAccept: */*\r\nAccept-Language: en-US,en;q=0.9\r\nPriority: u=3, i\r\nAccept-Encoding: gzip, deflate\r\nSec-WebSocket-Key: kSupAm7XAvs1TNerJqcWNw==\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";

    #[test]
    fn sniffs_native_and_http() {
        assert_eq!(sniff(b""), Sniff::NeedMore);
        assert_eq!(sniff(&[0xdf, 0xd1]), Sniff::Native);
        assert_eq!(sniff(b"G"), Sniff::NeedMore);
        assert_eq!(sniff(b"GE"), Sniff::NeedMore);
        assert_eq!(sniff(b"Gx"), Sniff::Native);
        assert_eq!(sniff(b"GET / HTTP/1.1\r\nHost: x\r\n"), Sniff::NeedMore);
        assert_eq!(sniff(WINGQ_REQUEST.as_bytes()), Sniff::Http(WINGQ_REQUEST.len()));
        let mut long = b"GET / HTTP/1.1\r\n".to_vec();
        long.resize(MAX_REQUEST_HEAD + 1, b'a');
        assert_eq!(sniff(&long), Sniff::TooLong);
    }

    #[test]
    fn answers_wingq_handshake() {
        let resp = String::from_utf8(handshake(WINGQ_REQUEST.as_bytes()).unwrap()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
        assert!(resp.contains(&format!("Sec-WebSocket-Accept: {}\r\n", accept_key("kSupAm7XAvs1TNerJqcWNw=="))));
        assert!(!resp.contains("Sec-WebSocket-Extensions"), "no compression is negotiated");
        assert!(resp.ends_with("\r\n\r\n"));
    }

    #[test]
    fn rejects_other_http() {
        let plain = handshake(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap_err();
        assert!(String::from_utf8(plain).unwrap().starts_with("HTTP/1.1 426"));
        let post = handshake(b"POST / HTTP/1.1\r\n\r\n").unwrap_err();
        assert!(String::from_utf8(post).unwrap().starts_with("HTTP/1.1 400"));
        let no_key =
            handshake(b"GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n")
                .unwrap_err();
        assert!(String::from_utf8(no_key).unwrap().starts_with("HTTP/1.1 400"));
        let old = handshake(b"GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Version: 8\r\nSec-WebSocket-Key: a\r\n\r\n")
            .unwrap_err();
        assert!(String::from_utf8(old).unwrap().starts_with("HTTP/1.1 426"));
        assert!(handshake(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn decodes_wingq_messages_split_anywhere() {
        // WING-Q's first two messages, as captured.
        let mut stream = client_frame(2, true, &[0xdf, 0xd1]);
        stream.extend(client_frame(2, true, &[0xd7, 0xf8, 0x98, 0x18, 0xa6, 0xdc]));
        stream.extend(client_frame(9, true, b"hi"));
        stream.extend(client_frame(10, true, b""));
        for split in 0..=stream.len() {
            let mut d = Decoder::default();
            let mut ev = Vec::new();
            d.push(&stream[..split], &mut ev);
            d.push(&stream[split..], &mut ev);
            assert_eq!(
                ev,
                vec![
                    Event::Data(vec![0xdf, 0xd1]),
                    Event::Data(vec![0xd7, 0xf8, 0x98, 0x18, 0xa6, 0xdc]),
                    Event::Ping(b"hi".to_vec())
                ],
                "split at {split}"
            );
        }
        // A byte at a time.
        let mut d = Decoder::default();
        let mut ev = Vec::new();
        for b in &stream {
            d.push(std::slice::from_ref(b), &mut ev);
        }
        assert_eq!(ev.len(), 3);
    }

    #[test]
    fn decodes_fragments_and_long_frames() {
        let big: Vec<u8> = (0..70_000u32).map(|i| i as u8).collect();
        let mut s = client_frame(2, false, &big[..300]);
        s.extend(client_frame(9, true, b"p")); // a control frame between fragments
        s.extend(client_frame(0, false, &big[300..66_000]));
        s.extend(client_frame(0, true, &big[66_000..]));
        let mut d = Decoder::default();
        let mut ev = Vec::new();
        d.push(&s, &mut ev);
        let mut data = Vec::new();
        let mut pings = 0;
        for e in ev {
            match e {
                Event::Data(b) => data.extend(b),
                Event::Ping(_) => pings += 1,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(data, big);
        assert_eq!(pings, 1);
    }

    #[test]
    fn errors_end_the_stream() {
        for (bad, why) in [
            (vec![0x82, 0x02, 0xdf, 0xd1], "client frame not masked"),
            (client_frame(0, true, b"x"), "continuation without a start"),
            (client_frame(3, true, b"x"), "unknown opcode"),
            (client_frame(9, false, b"x"), "bad control frame"),
            ([0xc2u8].iter().copied().chain(client_frame(2, true, b"x").into_iter().skip(1)).collect(), "reserved bits set"),
        ] {
            let mut d = Decoder::default();
            let mut ev = Vec::new();
            d.push(&bad, &mut ev);
            d.push(&client_frame(2, true, b"after"), &mut ev);
            assert_eq!(ev, vec![Event::Error(why)]);
        }
        let mut too_big = vec![0x82, 0xff];
        too_big.extend_from_slice(&((MAX_MESSAGE as u64) + 1).to_be_bytes());
        let mut ev = Vec::new();
        Decoder::default().push(&too_big, &mut ev);
        assert_eq!(ev, vec![Event::Error("message too big")]);
        let mut ev = Vec::new();
        let mut d = Decoder::default();
        d.push(&client_frame(8, true, &[0x03, 0xe8]), &mut ev);
        d.push(&client_frame(2, true, b"after"), &mut ev);
        assert_eq!(ev, vec![Event::Close]);
    }

    #[test]
    fn encodes_server_frames() {
        let mut out = Vec::new();
        encode_binary(&[0xdf, 0xd1, 0x01], &mut out);
        assert_eq!(out, vec![0x82, 0x03, 0xdf, 0xd1, 0x01]);
        let big = vec![7u8; MAX_FRAME_PAYLOAD * 2 + 10];
        let mut out = Vec::new();
        encode_binary(&big, &mut out);
        // Three messages: 64 KiB (8-byte length), 64 KiB, 10 bytes.
        assert_eq!(out.len(), big.len() + 10 + 10 + 2);
        assert_eq!(&out[..2], &[0x82, 127]);
        assert_eq!(control_frame(Opcode::Pong, b"hi"), vec![0x8a, 0x02, b'h', b'i']);
    }
}
