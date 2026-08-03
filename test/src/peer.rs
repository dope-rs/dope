use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::{JoinHandle, spawn};
use std::time::Duration;

use crate::GUARD;

pub fn reserve_addr() -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").expect("reserve address");
    socket.local_addr().expect("local address")
}

pub fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect_timeout(&addr, GUARD).expect("connect")
}

pub fn connect_with_read_timeout(addr: SocketAddr, timeout: Duration) -> TcpStream {
    let stream = connect(addr);
    stream
        .set_read_timeout(Some(timeout))
        .expect("read timeout");
    stream
}

pub fn spawn_peer<T: Send + 'static>(
    addr: SocketAddr,
    script: impl FnOnce(&mut TcpStream) -> T + Send + 'static,
) -> JoinHandle<T> {
    spawn(move || {
        let mut stream = connect(addr);
        script(&mut stream)
    })
}

/// Accepts and holds `count` silent loopback connections until all have arrived.
pub fn hold_connections(count: usize) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let handle = spawn(move || {
        let mut held = Vec::with_capacity(count);
        for _ in 0..count {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            held.push(stream);
        }
    });
    (addr, handle)
}

pub fn request_reply(addr: SocketAddr, request: Vec<u8>) -> JoinHandle<Vec<u8>> {
    spawn_peer(addr, move |stream| {
        stream.write_all(&request).expect("write request");
        read_all(stream)
    })
}

pub fn read_all(stream: &mut TcpStream) -> Vec<u8> {
    let mut got = Vec::new();
    stream.read_to_end(&mut got).expect("read to eof");
    got
}

pub fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}
