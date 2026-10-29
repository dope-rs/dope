//! Fiber client round-trip scenarios.
use dope::net::wire::terminal;
use dope_core::driver::route::KeyTag;
use dope_fiber::net::error::{self, read};

pub mod fault;
pub mod gated;
pub mod shutdown;

use std::{
    io,
    io::{Read, Write},
    time::Duration,
};

use dope::{
    manifold::client::conn,
    net::{
        tcp::Tcp,
        wire::{Identity, Spec, failure, receive},
    },
    runtime::executor::{self, drive, session},
};
use dope_fiber::{
    BlockOn as _,
    net::{Io, client::Machine, read::Lease},
};
use dope_test::harness::peer;
use o3::{
    buffer,
    buffer::write::{consume, consume::Consume as _},
};

pub const MAX_CONN: usize = 8;

pub trait Collect {
    fn try_into_vec(self) -> Result<Vec<u8>, consume::Error>;
}

impl<'w, 'd: 'w, W: Spec + 'w> Collect for Lease<'_, 'w, 'd, W>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Cursor + consume::Consume,
{
    fn try_into_vec(mut self) -> Result<Vec<u8>, consume::Error> {
        use dope::core::io::transfer::Length;

        let mut bytes = Vec::with_capacity(Length::len(&*self).into_raw());
        while !self.is_empty() {
            let chunk = self.as_ref();
            let amount = chunk.len();
            bytes.extend_from_slice(chunk);
            self.try_consume(amount)?;
        }
        Ok(bytes)
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'p, 'scope, 'd> {
    #[pin]
    #[manifold]
    pub(crate) connector: Machine<'p, 'scope, 'd, 'd, KeyTag<0>, Tcp, Identity>,
}

pub fn try_reply(peer: &mut peer::TcpAcceptor<'_>, reply: &[u8], tries: usize) -> io::Result<()> {
    for _ in 0..tries {
        let mut stream = peer.accept()?;
        stream.read_wait(Some(Duration::from_secs(2)))?;
        let mut req = [0u8; 64];
        match stream.read(&mut req) {
            Ok(n) if n > 0 => {
                stream.write_all(reply)?;
                return Ok(());
            }
            _ => continue,
        }
    }
    Ok(())
}

pub enum Exchange<'d, W: Spec + 'd> {
    Write(error::Write<'d>),
    Read(read::Read<terminal::Error<'d, W>>),
    Consume(consume::Error),
}

pub trait Ping<'d> {
    fn try_ping<'scope, W>(
        &mut self,
        io: Io<'scope, 'd, 'd, W, conn::Id<'d, KeyTag<0>>>,
    ) -> Result<Result<Vec<u8>, Exchange<'d, W>>, drive::Error>
    where
        'd: 'scope,
        W: Spec<TerminalFailure = failure::Never> + 'd,
        <W::Receive as receive::Policy<W>>::Cursor<'d, 'd>: buffer::Cursor + consume::Consume;
}

impl<'app, 'life: 'app, 'd: 'life, D: executor::raw::App<'d>> Ping<'d>
    for session::App<'app, 'life, 'd, D>
{
    fn try_ping<'scope, W>(
        &mut self,
        mut io: Io<'scope, 'd, 'd, W, conn::Id<'d, KeyTag<0>>>,
    ) -> Result<Result<Vec<u8>, Exchange<'d, W>>, drive::Error>
    where
        'd: 'scope,
        W: Spec<TerminalFailure = failure::Never> + 'd,
        <W::Receive as receive::Policy<W>>::Cursor<'d, 'd>: buffer::Cursor + consume::Consume,
    {
        let sent = self.block_on(io.write_all(b"ping"))?;
        if let Err(error) = sent {
            return Ok(Err(Exchange::Write(error)));
        }
        let mut got = Vec::new();
        loop {
            let received = self.block_on(io.read())?;
            match received {
                Ok(Some(lease)) => match lease.try_into_vec() {
                    Ok(bytes) => got.extend_from_slice(&bytes),
                    Err(error) => return Ok(Err(Exchange::Consume(error))),
                },
                Ok(None) => break,
                Err(error) => return Ok(Err(Exchange::Read(error))),
            }
        }
        Ok(Ok(got))
    }
}
