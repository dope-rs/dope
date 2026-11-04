//! Fiber client round-trip scenarios.
use dope::net::wire::terminal;
use dope_core::driver::route::KeyTag;
use dope_fiber::net::error::{self, read};

pub mod fault;
pub mod gated;
pub mod shutdown;

use std::time::Duration;

use dope::{
    manifold::client::conn,
    net::wire::{Spec, failure, receive},
    runtime::executor::{self, drive, session},
};
use dope_fiber::{
    BlockOn as _,
    net::{Io, read::Lease},
};
use dope_test::harness::peer;
use o3::{
    buffer,
    buffer::write::{consume, consume::Consume as _},
};

pub const MAX_CONN: usize = 8;

pub fn try_into_vec<'w, 'd: 'w, W: Spec + 'w>(
    mut lease: Lease<'_, 'w, 'd, W>,
) -> Result<Vec<u8>, consume::Error>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Cursor + consume::Consume,
{
    use dope::core::io::transfer::Length;

    let mut bytes = Vec::with_capacity(Length::len(&*lease).into_raw());
    while !lease.is_empty() {
        let chunk = lease.as_ref();
        let amount = chunk.len();
        bytes.extend_from_slice(chunk);
        lease.try_consume(amount)?;
    }
    Ok(bytes)
}

pub fn try_reply(
    peer: &mut peer::TcpAcceptor<'_>,
    reply: &[u8],
    tries: usize,
) -> Result<(), peer::Error> {
    for _ in 0..tries {
        let stream = peer.accept()?;
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

pub fn try_ping<'scope, 'app, 'life: 'app, 'd: 'life + 'scope, D, W>(
    app: &mut session::App<'app, 'life, 'd, D>,
    mut io: Io<'scope, 'd, 'd, W, conn::Id<'d, KeyTag<0>>>,
) -> Result<Result<Vec<u8>, Exchange<'d, W>>, drive::Error>
where
    D: executor::raw::App<'d>,
    W: Spec<TerminalFailure = failure::Never> + 'd,
    <W::Receive as receive::Policy<W>>::Cursor<'d, 'd>: buffer::Cursor + consume::Consume,
{
    let sent = app.block_on(io.write_all(b"ping"))?;
    if let Err(error) = sent {
        return Ok(Err(Exchange::Write(error)));
    }
    let mut got = Vec::new();
    loop {
        let received = app.block_on(io.read())?;
        match received {
            Ok(Some(lease)) => match try_into_vec(lease) {
                Ok(bytes) => got.extend_from_slice(&bytes),
                Err(error) => return Ok(Err(Exchange::Consume(error))),
            },
            Ok(None) => break,
            Err(error) => return Ok(Err(Exchange::Read(error))),
        }
    }
    Ok(Ok(got))
}
