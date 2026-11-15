use std::{mem, net::SocketAddr, ops::ControlFlow, pin::Pin};

use dope::{
    core::driver::{route, schedule::clock},
    manifold::network::datagram::{self, packet, send},
};
use o3::{alloc, buffer, buffer::resident::Shared, depot::capacity};

use crate::{
    conn::id,
    runtime::{
        endpoint::{Payload, Udp, reactor::queue::Queue},
        mux::{
            auth, handler,
            handler::hook::{
                set::Dispatch,
                wire::{cert, packet::Mode, udp},
            },
            router,
        },
        receive::Storage,
    },
    stream,
};

enum Phase<'d, const ID: u8> {
    Idle,
    Reply {
        source: SocketAddr,
        data: Vec<u8>,
        blocked: bool,
    },
    Sending(send::Owner<'d, route::KeyTag<ID>>),
    Stopped,
}

pub struct State<'d, const ID: u8> {
    backlog: packet::Backlog<'d>,
    phase: Phase<'d, ID>,
    next: bool,
}

impl<'d, const ID: u8> State<'d, ID> {
    pub(in crate::runtime) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            backlog: packet::Backlog::try_new(capacity)?,
            phase: Phase::Idle,
            next: true,
        })
    }

    pub(in crate::runtime) fn runnable(&self) -> bool {
        match self.phase {
            Phase::Idle => !self.backlog.is_empty(),
            Phase::Reply { blocked, .. } => !blocked,
            Phase::Sending(_) | Phase::Stopped => false,
        }
    }

    pub(in crate::runtime::endpoint) fn select(&mut self, output: bool) -> bool {
        if self.runnable() && (self.next || !output) {
            self.next = false;
            true
        } else {
            self.next = true;
            false
        }
    }

    pub(in crate::runtime) fn clear(&mut self) {
        self.phase = Phase::Stopped;
        self.backlog.clear();
    }

    pub(super) fn transferred(&mut self, transfer: send::Transfer<'d, route::KeyTag<ID>>) {
        match &mut self.phase {
            Phase::Sending(owner) if *owner == send::Owner::Queued(transfer.queued()) => {
                *owner = send::Owner::Flight(transfer.flight());
            }
            Phase::Reply { blocked, .. } => *blocked = false,
            _ => {}
        }
    }

    pub(super) fn settled<Q>(
        &mut self,
        done: &send::Settlement<'_, 'd, route::KeyTag<ID>, Q>,
    ) -> bool {
        match &mut self.phase {
            Phase::Sending(owner) if *owner == done.owner() => {
                let failed = !matches!(done.outcome(), send::Outcome::Sent(_));
                self.phase = if failed { Phase::Stopped } else { Phase::Idle };
                failed
            }
            Phase::Reply { blocked, .. } => {
                *blocked = false;
                false
            }
            _ => false,
        }
    }

    pub(super) fn intercept<'turn, H, B>(
        &mut self,
        source: SocketAddr,
        packet: packet::Packet<'turn, 'd>,
        handler: Pin<&mut H>,
    ) -> ControlFlow<(), packet::Packet<'turn, 'd>>
    where
        H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
        B: stream::Recv,
    {
        if matches!(self.phase, Phase::Stopped) {
            return ControlFlow::Break(());
        }
        if !matches!(self.phase, Phase::Idle) || !self.backlog.is_empty() {
            drop(self.backlog.try_push(source, packet));
            return ControlFlow::Break(());
        }
        Self::classify(&mut self.phase, source, packet, handler)
    }

    fn classify<'turn, H, B>(
        phase: &mut Phase<'d, ID>,
        source: SocketAddr,
        packet: packet::Packet<'turn, 'd>,
        handler: Pin<&mut H>,
    ) -> ControlFlow<(), packet::Packet<'turn, 'd>>
    where
        H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
        B: stream::Recv,
    {
        match <Udp<'d, H, ID, B> as udp::Mode<H, ID, B, Shared<'d>, id::Driver<'d>>>::call(
            handler,
            source,
            packet.as_ref(),
        ) {
            ControlFlow::Continue(()) => ControlFlow::Continue(packet),
            ControlFlow::Break(reply) => {
                if let Some(data) = reply {
                    *phase = Phase::Reply {
                        source,
                        data,
                        blocked: false,
                    };
                }
                ControlFlow::Break(())
            }
        }
    }

    pub(in crate::runtime::endpoint) fn step<H, B, M, Q>(
        &mut self,
        mut mux: Pin<&mut router::Router<H, ID, B, M>>,
        mut socket: Pin<&mut datagram::Socket<'d, route::KeyTag<ID>, Payload<Q>>>,
        now: clock::Moment<'d>,
    ) -> ControlFlow<()>
    where
        H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>> + cert::Sink<ID, id::Driver<'d>>,
        B: Storage<'d, Shared<'d>>,
        M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
        Q: buffer::Residency + buffer::raw::Source,
        <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Packet:
            Mode<H, ID, B, Shared<'d>, id::Driver<'d>, Output = ()>,
    {
        if matches!(self.phase, Phase::Idle) {
            if let Some((source, packet)) = self.backlog.pop_front()
                && let ControlFlow::Continue(packet) =
                    Self::classify(&mut self.phase, source, packet, mux.as_mut().handler_mut())
            {
                let keep = socket.as_ref().retainer();
                mux.observe(now, |mut turn| turn.receive(source, packet, keep));
            }
            return ControlFlow::Continue(());
        }
        let Phase::Reply {
            source,
            data,
            blocked: false,
        } = mem::replace(&mut self.phase, Phase::Stopped)
        else {
            unreachable!("reply")
        };
        let bytes = data.len();
        match socket.as_mut().queue_vec(data, source) {
            Ok(owner) => {
                self.phase = Phase::Sending(send::Owner::Queued(owner));
                let domain = mux.as_ref().get_ref().domain();
                Queue::<ID, H, B, M, Q>::notify(
                    mux,
                    handler::net::Accepted::new(
                        handler::net::Owner::queued(domain, owner),
                        source,
                        bytes,
                        handler::net::Shape::Plain,
                    ),
                );
                ControlFlow::Continue(())
            }
            Err(rejected) => {
                let (data, cause) = rejected.into_parts();
                self.phase = Phase::Reply {
                    source,
                    data,
                    blocked: true,
                };
                Queue::<ID, H, B, M, Q>::rejection(mux, cause)
            }
        }
    }
}
