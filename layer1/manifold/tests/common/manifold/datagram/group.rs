use std::{io, net, num::NonZero, pin::Pin, task::Poll};

use dope_core::{
    driver::route::{KeyTag, indices::SlotIndex},
    io::datagram::ReceiveError,
};
use dope_manifold::network::datagram::{group, packet::Packet, send::Outcome};
use dope_runtime::executor::root;
use dope_test::harness::signals::Gate;

pub struct State {
    pub client: net::SocketAddr,
    pub upstream: net::SocketAddr,
    pub received: Gate,
}

pub struct Handler<F, S> {
    pub state: State,
    pub callback: F,
    pub sent: S,
    pub dropped: Gate,
}

impl<F, S> Handler<F, S> {
    pub fn new<'d>(state: State, callback: F, sent: S, dropped: Gate) -> Self
    where
        F: for<'turn> FnMut(
                &mut State,
                Result<(net::SocketAddr, Packet<'turn, 'd>), ReceiveError>,
                SlotIndex,
                group::Access<'turn, 'd, KeyTag<0>>,
            ) + Unpin,
        S: FnMut(Outcome) + Unpin,
    {
        Self {
            state,
            callback,
            sent,
            dropped,
        }
    }
}

impl<'d, F, S> group::Handler<'d, KeyTag<0>> for Handler<F, S>
where
    F: for<'turn> FnMut(
            &mut State,
            Result<(net::SocketAddr, Packet<'turn, 'd>), ReceiveError>,
            SlotIndex,
            group::Access<'turn, 'd, KeyTag<0>>,
        ) + Unpin,
    S: FnMut(Outcome) + Unpin,
{
    fn packet<'turn>(
        self: Pin<&mut Self>,
        received: Result<(net::SocketAddr, Packet<'turn, 'd>), ReceiveError>,
        source: SlotIndex,
        sockets: group::Access<'turn, 'd, KeyTag<0>>,
    ) {
        let this = self.get_mut();
        (this.callback)(&mut this.state, received, source, sockets);
    }

    fn sent(self: Pin<&mut Self>, outcome: Outcome) {
        (self.get_mut().sent)(outcome);
    }
}

impl<F, S> Drop for Handler<F, S> {
    fn drop(&mut self) {
        self.dropped.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct App<'d, H> {
    #[pin]
    #[manifold]
    pub endpoint: group::raw::Endpoint<'d, KeyTag<0>, H>,
}

pub struct Read<'a> {
    pub socket: &'a net::UdpSocket,
    pub bytes: &'a mut [u8],
    pub credits: NonZero<usize>,
}

impl<'d> root::Root<'d> for Read<'_> {
    type Output = io::Result<(usize, net::SocketAddr, usize)>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, work, _) = context.into_parts();
        let this = this.get_mut();
        while work.remaining() > this.credits.get() {
            if let Some(permit) = work.permit() {
                permit.run(|| ());
            }
        }
        match this.socket.recv_from(this.bytes) {
            Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => {
                wake.wake();
                Poll::Pending
            }
            result => {
                Poll::Ready(result.map(|(length, source)| (length, source, work.remaining())))
            }
        }
    }
}
