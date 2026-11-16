/// An Implementation of Echo Protocol
/// See RFC: 862 (https://www.rfc-editor.org/rfc/rfc862.html)
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream, UdpSocket},
    thread::{self},
};

fn handle_tcp_client(mut stream: TcpStream) {
    thread::spawn(move || {
        let mut buf = [0; 4096];
        loop {
            match stream.read(&mut buf) {
                // Peer closed its write side: mirror the half-close with our
                // own FIN (best effort — the peer may already be gone), then
                // leave the loop so the thread ends and the socket is dropped.
                Ok(0) => {
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    break;
                }
                Ok(n) => {
                    // Echo exactly the bytes that arrived. A failed write
                    // means the client vanished mid-conversation — same
                    // outcome as any other broken connection: stop.
                    if stream.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                // Read error: connection is broken, stop.
                Err(_) => break,
            }
        }
    });
}

fn run_udp_server(socket: UdpSocket) {
    // Sized for the largest possible UDP payload (65507 = 65535 total IP
    // packet − 20 IP header − 8 UDP header). recv_from TRUNCATES to this
    // buffer's size and silently discards the rest, so it must cover the max.
    let mut buf = [0u8; 65536];

    loop {
        match socket.recv_from(&mut buf) {
            Ok((amt, src)) => {
                if let Err(e) = socket.send_to(&buf[..amt], src) {
                    eprintln!("failed to reply to {src}: {e}");
                }
            }
            // A single failed receive is not fatal: log it and keep serving.
            // The service only dies with the process, never on one datagram.
            Err(e) => eprintln!("udp recv error: {e}"),
        }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    let socket = UdpSocket::bind("127.0.0.1:8087")?;

    let tcp_handle = thread::spawn(move || {
        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => handle_tcp_client(stream),
                // One failed accept is not fatal: log it and keep serving.
                // Only errors that make the listener itself unusable should
                // ever take the service down.
                Err(e) => eprintln!("accept error: {e}"),
            }
        }
    });
    let udp_handle = thread::spawn(move || run_udp_server(socket));

    // If main returns, the process exits and its threads die with it.
    // Joining keeps main alive while the servers run.
    tcp_handle.join().unwrap();
    udp_handle.join().unwrap();

    Ok(())
}
