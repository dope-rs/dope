use std::{
    io::{Read as _, Write as _},
    net, thread, time,
};

use crate::checks::Outcome as _;

#[derive(Clone, Copy)]
pub struct Peer {
    addr: net::SocketAddr,
}

impl Peer {
    pub const fn at(addr: net::SocketAddr) -> Self {
        Self { addr }
    }

    pub fn reserve() -> Self {
        let listener =
            net::TcpListener::bind("127.0.0.1:0").or_abort("bind loopback peer listener");
        let addr = listener.local_addr().or_abort("read loopback peer address");
        Self { addr }
    }

    pub const fn addr(self) -> net::SocketAddr {
        self.addr
    }

    pub fn connect(self) -> net::TcpStream {
        net::TcpStream::connect_timeout(&self.addr, crate::GUARD).or_abort("connect loopback peer")
    }

    pub fn connect_with_read_timeout(self, timeout: time::Duration) -> net::TcpStream {
        let stream = self.connect();
        stream
            .set_read_timeout(Some(timeout))
            .or_abort("set peer read timeout");
        stream
    }

    pub fn spawn<T: Send + 'static>(
        self,
        script: impl FnOnce(&mut net::TcpStream) -> T + Send + 'static,
    ) -> thread::JoinHandle<T> {
        thread::spawn(move || {
            let mut stream = self.connect();
            script(&mut stream)
        })
    }

    pub fn request_reply(self, request: Vec<u8>) -> thread::JoinHandle<Vec<u8>> {
        self.spawn(move |stream| {
            stream.write_all(&request).or_abort("write peer request");
            Self::read_all(stream)
        })
    }

    pub fn read_all(stream: &mut net::TcpStream) -> Vec<u8> {
        let mut got = Vec::new();
        stream.read_to_end(&mut got).or_abort("read peer response");
        got
    }

    /// Accepts and holds `count` silent loopback connections until all have arrived.
    pub fn hold(count: usize) -> (net::SocketAddr, thread::JoinHandle<()>) {
        let listener = net::TcpListener::bind("127.0.0.1:0").or_abort("bind held peer listener");
        let addr = listener.local_addr().or_abort("read held peer address");
        let handle = thread::spawn(move || {
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
}

pub struct Pattern {
    bytes: Vec<u8>,
}

impl Pattern {
    pub fn with_len(len: usize) -> Self {
        Self {
            bytes: (0..len).map(|i| (i % 251) as u8).collect(),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}
