//! Black-box test suite for the **UDP** echo server (RFC 862, UDP flavor).
//!
//! The suite only assumes a UDP echo server is already listening: it sends a
//! datagram and expects the same bytes back in a datagram addressed to the
//! sender's source address/port. Nothing else — no connection, no EOF, no
//! half-close. Run your UDP server, then:
//!
//! ```sh
//! ECHO_UDP_ADDR=127.0.0.1:8087 cargo test --test udp_blackbox
//! ```
//!
//! (Omit `ECHO_UDP_ADDR` to use the default 127.0.0.1:8087.)
//!
//! Covered matrix:
//!   - single datagram echo, binary data, empty datagram
//!   - message boundary preservation (N datagrams in → N datagrams out)
//!   - rapid-fire + concurrent clients from different source ports
//!   - reply addressed to the correct source
//!   - near-max-size datagram (catches `recv_from` truncation bugs!)
//!   - server sends nothing spontaneously
//!
//! Notes for the implementer:
//!   - UDP payloads max out at 65507 bytes. `recv_from` **truncates** to the
//!     caller's buffer size, so a server whose buffer is smaller than the
//!     datagram silently corrupts the echo — the max-size test catches this.
//!   - On loopback you will practically never see loss or reordering, but the
//!     RFC does not guarantee order — the multi-datagram tests assert a
//!     multiset (count + contents) rather than strict sequence.

use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const DEFAULT_ADDR: &str = "127.0.0.1:8087";

/// Address of the UDP server under test.
fn server_addr() -> SocketAddr {
    std::env::var("ECHO_UDP_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()
        .expect("ECHO_UDP_ADDR must be a valid socket address")
}

/// Open a "connected" UDP client socket with sane timeouts. Connecting means
/// `recv` only accepts datagrams from the server — stray packets from other
/// ports can't confuse a test.
fn connect_client() -> UdpSocket {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral port");
    sock.connect(server_addr()).expect("connect UDP");
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    sock.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    sock
}

/// Receive one datagram into a correctly-sized buffer (never truncated).
fn recv_datagram(sock: &UdpSocket) -> Vec<u8> {
    let mut buf = vec![0u8; 65536]; // 65507 max payload + slack
    let n = sock
        .recv(&mut buf)
        .expect("receiving echo datagram failed/timed out");
    buf.truncate(n);
    buf
}

/// Send one datagram, receive one back, assert byte equality.
fn echo_roundtrip(sock: &UdpSocket, payload: &[u8]) {
    sock.send(payload).expect("send failed");
    let back = recv_datagram(sock);
    assert_eq!(back, payload, "echoed datagram corrupted");
}

/// Multiset comparison helper: same count, same contents, any order.
fn assert_same_datagrams(mut got: Vec<Vec<u8>>, mut want: Vec<Vec<u8>>) {
    assert_eq!(got.len(), want.len(), "datagram count mismatch");
    got.sort();
    want.sort();
    assert_eq!(got, want, "datagram contents mismatch (order ignored)");
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------
#[test]
fn echoes_single_datagram() {
    let sock = connect_client();
    echo_roundtrip(&sock, b"hello over udp\n");
}

#[test]
fn echoes_binary_datagram() {
    let sock = connect_client();
    // All 256 byte values, several rounds — NULs and invalid UTF-8 included.
    let payload: Vec<u8> = (0..3)
        .flat_map(|round| (0..=255u8).map(move |b| b.wrapping_add(round)))
        .collect();
    echo_roundtrip(&sock, &payload);
}

#[test]
fn echoes_empty_datagram() {
    let sock = connect_client();
    // A 0-byte datagram is a real datagram — it must echo back as one.
    // (This is distinct from "nothing happened": with a read timeout set,
    // silence shows up as a timeout error, not Ok(0).)
    sock.send(&[]).expect("send of empty datagram failed");
    let back = recv_datagram(&sock);
    assert!(back.is_empty(), "empty datagram echoed back non-empty");
}

#[test]
fn preserves_message_boundaries() {
    let sock = connect_client();

    // 10 distinct datagrams sent back-to-back. UDP preserves boundaries:
    // exactly 10 datagrams must come back, each byte-identical. A TCP-style
    // "just keep reading bytes" merge would show up here as fewer/merged
    // datagrams with the right total byte count.
    let sent: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("dgram-{i:02}-{}", "x".repeat(i * 3)).into_bytes())
        .collect();
    for payload in &sent {
        sock.send(payload).expect("send failed");
    }

    let got: Vec<Vec<u8>> = (0..10).map(|_| recv_datagram(&sock)).collect();
    assert_same_datagrams(got, sent);
}

#[test]
fn echoes_rapid_fire_datagrams() {
    let sock = connect_client();

    let sent: Vec<Vec<u8>> = (0..100).map(|i| format!("msg-{i:03};").into_bytes()).collect();
    for payload in &sent {
        sock.send(payload).expect("send failed");
    }

    let got: Vec<Vec<u8>> = (0..100).map(|_| recv_datagram(&sock)).collect();
    assert_same_datagrams(got, sent);
}

#[test]
fn replies_to_the_correct_source() {
    let a = connect_client();
    let b = connect_client();

    // Interleave sends from two different source ports; each must get back
    // its own marker and nothing else. Catches servers that reply to a fixed
    // port or to the "last" client seen.
    a.send(b"marker-A").unwrap();
    b.send(b"marker-B").unwrap();
    b.send(b"marker-B2").unwrap();
    a.send(b"marker-A2").unwrap();

    assert_eq!(recv_datagram(&a), b"marker-A");
    assert_eq!(recv_datagram(&b), b"marker-B");
    assert_eq!(recv_datagram(&b), b"marker-B2");
    assert_eq!(recv_datagram(&a), b"marker-A2");
}

#[test]
fn handles_many_concurrent_clients() {
    let handles: Vec<_> = (0..8)
        .map(|i| {
            thread::spawn(move || {
                let sock = connect_client();
                for round in 0..5 {
                    echo_roundtrip(&sock, format!("client-{i:02}-round-{round}").as_bytes());
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("client thread panicked");
    }
}

#[test]
fn echoes_near_max_size_datagram() {
    let sock = connect_client();

    // 65507 = largest possible UDP payload (65535 - 8 UDP header - 20 IP).
    // recv_from TRUNCATES to the receiver's buffer: a server with a small
    // recv buffer echoes a truncated datagram and the rest is lost forever.
    // Passing this requires a receive buffer sized for the max payload.
    let payload: Vec<u8> = (0..65507).map(|i| (i % 251) as u8).collect();
    echo_roundtrip(&sock, &payload);
}

#[test]
fn sends_nothing_spontaneously() {
    let sock = connect_client();

    // An idle connected socket must not receive any unprompted datagram.
    sock.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let mut buf = [0u8; 64];
    match sock.recv(&mut buf) {
        Ok(n) => panic!("server sent unprompted data: {buf:?} ({n} bytes)"),
        Err(e) => {
            assert!(
                e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut,
                "unexpected error on idle socket: {e}"
            );
        }
    }
}
