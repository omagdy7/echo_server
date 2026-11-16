//! Black-box test suite for an echo server.
//!
//! The suite only assumes a TCP echo server is already listening somewhere —
//! it knows nothing about how the server is implemented. Run your server,
//! then point the tests at it:
//!
//! ```sh
//! # terminal 1
//! cargo run -- 127.0.0.1:8080   # however your server binds
//! # terminal 2
//! ECHO_ADDR=127.0.0.1:8080 cargo test
//! ```
//!
//! (Omit `ECHO_ADDR` to use the default 127.0.0.1:8080.)
//!
//! Covered matrix:
//!   - basic echo, multiple messages, rapid-fire sends
//!   - arbitrary binary data (incl. invalid UTF-8, NUL bytes)
//!   - large payloads / full-duplex streaming (write and read concurrently)
//!   - fragmented sends (data trickled in byte-by-byte)
//!   - half-close semantics (data flushed before EOF, then clean EOF back)
//!   - abrupt client disconnects; server keeps accepting afterwards
//!   - empty connection (connect + immediate close)
//!   - server sends nothing spontaneously
//!   - concurrency: many simultaneous connections

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::thread;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const DEFAULT_ADDR: &str = "127.0.0.1:8080";

/// The address of the server under test, as a raw string.
fn server_addr() -> String {
    std::env::var("ECHO_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string())
}

/// Open a client connection with sane timeouts so a broken server
/// makes tests fail fast instead of hanging.
fn connect() -> TcpStream {
    let stream = TcpStream::connect(server_addr())
        .expect("connect failed — is the echo server running? (see tests/blackbox.rs header)");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    stream
}

/// Helper for tests that need the port as a number.
fn server_port() -> u16 {
    server_addr()
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("ECHO_ADDR must end in ':<port>'")
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

#[test]
fn echoes_simple_message() {
    let mut s = connect();
    s.write_all(b"hello, world\n").unwrap();

    let mut buf = vec![0u8; 13];
    s.read_exact(&mut buf).unwrap();
    assert_eq!(buf, b"hello, world\n");
}

#[test]
fn echoes_multiple_messages_on_same_connection() {
    let mut s = connect();

    for i in 0..10 {
        let msg = format!("message {i}\n");
        s.write_all(msg.as_bytes()).unwrap();

        let mut buf = vec![0u8; msg.len()];
        s.read_exact(&mut buf).unwrap();
        assert_eq!(buf, msg.as_bytes(), "round-trip {i} mismatch");
    }
}

#[test]
fn echoes_rapid_fire_messages() {
    let mut s = connect();

    // Write 100 messages back-to-back before reading anything, then drain
    // and compare the full stream. Exercises buffering/ordering, and allows
    // the server to coalesce reads (bytes must match, framing need not).
    let mut sent = Vec::new();
    for i in 0..100 {
        sent.extend_from_slice(format!("msg-{i:03};").as_bytes());
    }
    s.write_all(&sent).unwrap();

    let mut received = Vec::new();
    let mut buf = [0u8; 1024];
    while received.len() < sent.len() {
        let n = s.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed early at {}/{}", received.len(), sent.len());
        received.extend_from_slice(&buf[..n]);
    }
    assert_eq!(received, sent);
}

#[test]
fn echoes_arbitrary_binary_data() {
    let mut s = connect();

    // All 256 byte values, several times over — includes NULs and bytes that
    // are invalid UTF-8 in isolation.
    let payload: Vec<u8> = (0..3).flat_map(|round| (0..=255u8).map(move |b| b.wrapping_add(round))).collect();
    s.write_all(&payload).unwrap();

    let mut received = vec![0u8; payload.len()];
    s.read_exact(&mut received).unwrap();
    assert_eq!(received, payload);
}

#[test]
fn echoes_large_payload_full_duplex() {
    let mut s = connect();

    // 1 MiB written in chunks while reading back concurrently. A naive
    // "buffer everything, then echo" server deadlocks or stalls here because
    // the client's write blocks once socket buffers fill up while the server
    // still hasn't echoed anything.
    let payload: Vec<u8> = (0..1_048_576usize).map(|i| (i % 251) as u8).collect();
    let mut sent = 0usize;
    let mut received = Vec::with_capacity(payload.len());
    let mut buf = [0u8; 16 * 1024];
    let mut writer = s.try_clone().expect("clone stream");

    let mut progress = |sent: &mut usize, received: &mut Vec<u8>| -> bool {
        if *sent < payload.len() {
            let end = (*sent + 32 * 1024).min(payload.len());
            if writer.write_all(&payload[*sent..end]).is_err() {
                return false;
            }
            *sent = end;
        }
        if received.len() < payload.len() {
            match s.read(&mut buf) {
                Ok(0) => return false, // EOF before full echo
                Ok(n) => received.extend_from_slice(&buf[..n]),
                Err(_) => return false,
            }
        }
        *sent >= payload.len() && received.len() >= payload.len()
    };

    for _ in 0..100_000 {
        if progress(&mut sent, &mut received) {
            break;
        }
    }
    assert_eq!(sent, payload.len(), "failed to send full payload");
    assert_eq!(received.len(), payload.len(), "did not receive full echo");
    assert_eq!(received, payload, "echoed data corrupted");
}

#[test]
fn echoes_data_sent_fragmented_byte_by_byte() {
    let mut s = connect();

    // Trickle "fragmented" in one byte at a time with pauses. The server must
    // reassemble whatever arrives and echo it; it must never wait for a
    // "complete" message before responding.
    let msg = b"fragmented";
    for byte in msg {
        s.write_all(std::slice::from_ref(byte)).unwrap();
        thread::sleep(Duration::from_millis(10));
    }

    let mut received = Vec::new();
    let mut buf = [0u8; 64];
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received.len() < msg.len() && std::time::Instant::now() < deadline {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => received.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    assert_eq!(received, msg);
}

#[test]
fn half_close_flushes_pending_data_then_eof() {
    let mut s = connect();

    // Send data, then close only our write side. The server must echo
    // everything already sent and then deliver EOF (not a hang, not a reset).
    s.write_all(b"final message").unwrap();
    s.shutdown(Shutdown::Write).unwrap();

    let mut received = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = s.read(&mut buf).expect("read after half-close");
        if n == 0 {
            break; // clean EOF
        }
        received.extend_from_slice(&buf[..n]);
    }
    assert_eq!(received, b"final message");
}

#[test]
fn handles_abrupt_disconnect_and_keeps_accepting() {
    // Slam the door mid-conversation several times.
    for i in 0..5 {
        let mut s = connect();
        s.write_all(format!("abrupt-{i}").as_bytes()).unwrap();
        drop(s); // no graceful shutdown, no reading the echo
    }

    // The server must still be alive and serving new connections.
    let mut s = connect();
    s.write_all(b"still there?").unwrap();
    let mut buf = vec![0u8; 11];
    s.read_exact(&mut buf).unwrap();
    assert_eq!(buf, b"still there");
}

#[test]
fn empty_connection_is_harmless() {
    // Connect and immediately close without sending anything.
    drop(connect());
    drop(TcpStream::connect(server_addr()).unwrap());

    // Server must still accept and echo correctly.
    let mut s = connect();
    s.write_all(b"alive").unwrap();
    let mut buf = vec![0u8; 5];
    s.read_exact(&mut buf).unwrap();
    assert_eq!(buf, b"alive");
}

#[test]
fn sends_nothing_spontaneously() {
    let mut s = connect();

    // An idle connection must not receive any unprompted bytes.
    s.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let mut buf = [0u8; 64];
    match s.read(&mut buf) {
        Ok(0) => panic!("server closed an idle connection"),
        Ok(n) => panic!("server sent unprompted data: {buf:?} ({n} bytes)"),
        Err(e) => {
            // Expect a timeout; anything else (e.g. connection reset) is a bug.
            assert!(
                e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut,
                "unexpected error on idle connection: {e}"
            );
        }
    }
}

#[test]
fn handles_many_concurrent_connections() {
    let addr = server_addr();

    let handles: Vec<_> = (0..20)
        .map(|i| {
            let addr = addr.clone();
            thread::spawn(move || {
                let mut s = TcpStream::connect(addr).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();

                let msg = format!("client-{i:02}-payload\n");
                for _ in 0..5 {
                    s.write_all(msg.as_bytes()).unwrap();
                    let mut buf = vec![0u8; msg.len()];
                    s.read_exact(&mut buf).unwrap();
                    assert_eq!(buf, msg.as_bytes(), "client {i} got wrong echo");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("client thread panicked");
    }
}

#[test]
fn echoes_multibyte_utf8_across_chunk_boundaries() {
    let mut s = connect();

    // Multibyte characters sent in tiny, misaligned chunks: the echo must be
    // byte-identical regardless of where reads/writes split the characters.
    let msg = "héllo — ünïcode 🦀 日本語\n";
    for chunk in msg.as_bytes().chunks(3) {
        s.write_all(chunk).unwrap();
    }

    let mut received = vec![0u8; msg.len()];
    s.read_exact(&mut received).unwrap();
    assert_eq!(received, msg.as_bytes());
}
