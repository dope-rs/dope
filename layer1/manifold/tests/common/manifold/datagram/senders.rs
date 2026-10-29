use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    num::NonZero,
    pin::Pin,
};

use dope_core::{
    dope_sys::io::socket::msg::source, driver::route::KeyTag, io::datagram::source::Source,
};
use dope_manifold::network::datagram::{
    self, Endpoint, Socket,
    hook::{self, app::Schedule, ingress},
    send::{self, owners},
};
use dope_test::harness::{peer::udp, signals::Gate};
use o3::buffer::pool;

use crate::common::manifold::datagram::handler;

pub struct Collected {
    pub source: SocketAddr,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum Collect {
    Io(io::Error),
    Source {
        expected: SocketAddr,
        actual: SocketAddr,
    },
}

pub fn selected_source(peer: SocketAddr) -> Result<Source, source::Error> {
    Source::try_new((Ipv4Addr::new(127, 0, 0, 2), 0).into(), peer)
}

type Pending<A> = (Vec<u8>, Option<NonZero<u16>>, A);

pub struct State<M: send::Segmented = send::Plain> {
    pub queued: Gate,
    pub received: Gate,
    pub(crate) pending: Option<Pending<M::Address>>,
}

impl<M: send::Segmented> handler::State for State<M> {
    type Hooks = hook::Set<
        hook::Disabled,
        hook::Disabled,
        hook::Disabled,
        Schedule,
        hook::Disabled,
        pool::Cursor,
        M,
    >;
    type Mode = ingress::Access;
}

impl<M: send::Segmented> State<M> {
    pub fn queue<'d>(
        socket: Pin<&mut Socket<'d, KeyTag<0>, pool::Cursor, M>>,
        (payload, size, address): Pending<M::Address>,
    ) -> Result<owners::Queued<'d, KeyTag<0>>, datagram::Rejected<Vec<u8>>> {
        match size {
            Some(size) => socket.queue_gso(payload, size, address),
            None => socket.queue_vec(payload, address),
        }
    }
}

impl<M: send::Segmented> handler::Progress for State<M> {
    fn is_runnable(&self) -> bool {
        self.pending.is_some()
    }
}

pub struct Busy {
    pub addr: SocketAddr,
    pub pending: [Option<Vec<u8>>; 3],
    pub next: usize,
    pub handled: Gate,
    pub received: Gate,
    pub settled: Gate,
    pub rejected: Gate,
}

impl handler::State for Busy {
    type Hooks = hook::Set<hook::Disabled, hook::Disabled, hook::Disabled, Schedule, hook::Enabled>;
    type Mode = ingress::Time;
}

impl handler::Progress for Busy {
    fn is_runnable(&self) -> bool {
        self.next < self.pending.len()
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct App<'d, H, M: send::Mode = send::Plain> {
    #[pin]
    #[manifold]
    pub(crate) sender: Endpoint<'d, KeyTag<0>, H, pool::Cursor, M>,
}

pub fn collect(socket: &udp::Socket<'_>, count: NonZero<usize>) -> Result<Collected, Collect> {
    let mut got = Vec::new();
    let mut buf = [0u8; 2048];
    let (received, source) = socket.recv_from(&mut buf).map_err(Collect::Io)?;
    got.extend_from_slice(&buf[..received]);
    for _ in 1..count.get() {
        let (received, current) = socket.recv_from(&mut buf).map_err(Collect::Io)?;
        if current != source {
            return Err(Collect::Source {
                expected: source,
                actual: current,
            });
        }
        got.extend_from_slice(&buf[..received]);
    }
    Ok(Collected { source, bytes: got })
}
