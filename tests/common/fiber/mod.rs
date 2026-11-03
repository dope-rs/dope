use dope_tls::tls::Tls;
use dope_tls::tls::roles::Server;
type Wire = Tls<Server>;
mod bound;
pub use bound::Bound;
pub mod credit;
pub mod retain;
use crate::common::native;
use crate::common::state::{name, signing_key};
use dope::core::driver::route::KeyTag;
use dope::net::{tcp::Tcp, wire::open};
use dope_fiber::net::server::Listener;
use dope_test::harness::peer;
use dope_tls::{config, tls::endpoint};
pub use native::Error as Connect;
use std::{
    io::{self, BufRead as _, Write},
    sync::Arc,
    time::Duration,
};

pub const FIRST: &[u8] = b"fragmented";
pub const SECOND: &[u8] = b"-provided";
pub const TAIL_RECORDS: usize = 33;
pub const MAX_CONNECTIONS: usize = 4;

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'n, 'p, 's, 'scope, 'w, 'd: 's + 'w, F: FnMut(open::Error<'w, Wire>)> {
    #[pin]
    #[manifold]
    pub pool: Listener<'n, 'p, 's, 'scope, 'w, 'd, KeyTag<0>, Tcp, Wire, F>,
}

pub struct Peer<'socket> {
    pub socket: &'socket peer::Stream<'socket>,
    pub tls: rustls::Connection,
}

impl<'socket> Peer<'socket> {
    pub fn try_server() -> Result<(endpoint::Config, [u8; 32]), dope_tls::Error> {
        let key = signing_key()?;
        let pubkey = key.public_key();
        let end = endpoint::Config::from(Arc::new(key.server()?));
        Ok((end, pubkey))
    }

    pub fn establish(
        socket: &'socket peer::Stream<'socket>,
        pubkey: [u8; 32],
    ) -> Result<Self, Connect> {
        socket.set_nodelay(true).map_err(Connect::Peer)?;
        socket
            .read_wait(Some(Duration::from_secs(3)))
            .map_err(Connect::Peer)?;
        let mut peer = Self {
            socket,
            tls: rustls::ClientConnection::new(
                Arc::new(config::raw::Identity::client(pubkey).map_err(Connect::Config)?),
                name(),
            )
            .map(rustls::Connection::Client)
            .map_err(Connect::Tls)?,
        };

        for _ in 0..64 {
            native::write(&mut peer.tls, peer.socket)?;
            if !peer.tls.is_handshaking() {
                break;
            }
            native::read(&mut peer.tls, peer.socket)?;
        }
        if peer.tls.is_handshaking() {
            return Err(Connect::Handshake);
        }
        native::write(&mut peer.tls, peer.socket)?;
        Ok(peer)
    }

    pub fn seal(&mut self, body: &[u8]) -> Result<Vec<u8>, Connect> {
        self.tls.writer().write_all(body).map_err(Connect::Io)?;
        native::output(&mut self.tls)
    }

    pub fn try_send(&mut self, body: &[u8]) -> Result<(), Connect> {
        let mut rest = body;
        while !rest.is_empty() {
            let used = self.tls.writer().write(rest).map_err(Connect::Io)?;
            native::write(&mut self.tls, self.socket)?;
            if used == 0 {
                return Err(Connect::WriteZero);
            }
            rest = &rest[used..];
        }
        Ok(())
    }

    pub fn try_recv(&mut self) -> Result<Vec<u8>, Connect> {
        loop {
            let data = match self.tls.reader().fill_buf() {
                Ok([]) => return Err(Connect::Closed),
                Ok(data) => data.to_vec(),
                Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => {
                    native::read(&mut self.tls, self.socket)?;
                    native::write(&mut self.tls, self.socket)?;
                    continue;
                }
                Err(cause) => return Err(Connect::Io(cause)),
            };
            self.tls.reader().consume(data.len());
            return Ok(data);
        }
    }
}

pub mod client;
