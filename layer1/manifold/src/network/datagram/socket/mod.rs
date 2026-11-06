use std::{num, ops, pin};

use dope_core::{
    driver,
    driver::{
        life::finish,
        retained::flight,
        route,
        route::{Identity, kind},
        schedule,
        schedule::{clock, maint, ready},
    },
    io,
    io::{datagram, datagram::source, event},
};
use o3::{
    base::cell::region,
    buffer::{self, pool},
};
use send::{kernel, owners};

use crate::network::datagram::{Rejected, hook, packet, send};

pub(in crate::network::datagram) mod binding;
pub(in crate::network::datagram) mod priority;
pub(in crate::network::datagram) mod receive;
mod retire;
pub(in crate::network::datagram) mod sender;

#[pin_project::pin_project(!Unpin, PinnedDrop)]
pub struct Socket<'d, ID: Identity, B = pool::Cursor, M: send::Mode = send::Plain> {
    #[pin]
    pub(in crate::network::datagram) receive: receive::Receive<'d, ID>,
    pub(in crate::network::datagram) recv_flights:
        flight::Slots<'d, <ID as route::Materialize<kind::Recv>>::With>,
    pub(in crate::network::datagram) sender: sender::Sender<'d, ID, B, M>,
    pub(in crate::network::datagram) binding: binding::Binding<'d, ID>,
    pub(in crate::network::datagram) priority: priority::Priority,
}

#[pin_project::pinned_drop]
impl<ID: Identity, B, M: send::Mode> PinnedDrop for Socket<'_, ID, B, M> {
    fn drop(self: pin::Pin<&mut Self>) {
        let this = self.as_ref().get_ref();
        this.binding.advance_to(this.sender.next_generation());
    }
}

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: send::Segmented>
    Socket<'d, ID, B, M>
{
    /// Queues owned bytes and returns their identity for settlement tracking.
    pub fn queue_vec<P>(
        self: pin::Pin<&mut Self>,
        data: P,
        addr: M::Address,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        P: buffer::Residency + buffer::raw::Source + buffer::raw::Convert<Vec<u8>>,
    {
        self.project().sender.try_enqueue(data, addr, |data| {
            kernel::Payload::Owned(data.into_storage())
        })
    }

    pub fn queue_pooled<P>(
        self: pin::Pin<&mut Self>,
        data: P,
        addr: M::Address,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        P: buffer::Residency + buffer::raw::Source + buffer::raw::Convert<B>,
    {
        self.project().sender.try_enqueue(data, addr, |data| {
            kernel::Payload::Buffer(data.into_storage())
        })
    }

    pub fn queue_packet<'turn>(
        self: pin::Pin<&mut Self>,
        packet: packet::Packet<'turn, 'd>,
        addr: M::Address,
    ) -> Result<(), Rejected<packet::Packet<'turn, 'd>>> {
        let view = packet.into_lease();
        self.project()
            .sender
            .try_receive(view, M::address(&addr), M::control(addr, None))
            .map_err(|error| error.map(packet::Packet::new))
    }

    pub fn queue_gso<P>(
        self: pin::Pin<&mut Self>,
        data: P,
        size: num::NonZero<u16>,
        addr: M::Address,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        P: buffer::Residency + buffer::raw::Source + buffer::raw::Convert<Vec<u8>>,
    {
        let data = datagram::GsoPayload::try_new(data, size).map_err(|rejected| {
            let (data, cause) = rejected.into_parts();
            Rejected::new(data, cause.into())
        })?;
        self.queue_gso_payload(data, addr)
            .map_err(|rejected| rejected.map(|data| data.into_payload().into_inner()))
    }

    /// Queues an admitted GSO owner, returning the same proof on rejection.
    /// The conversion only removes an ownership wrapper after queue admission.
    pub fn queue_gso_payload<P, T>(
        self: pin::Pin<&mut Self>,
        data: P,
        addr: M::Address,
    ) -> Result<owners::Queued<'d, ID>, Rejected<P>>
    where
        P: buffer::Residency + buffer::raw::Convert<datagram::GsoPayload<T>>,
        T: buffer::raw::Convert<Vec<u8>>,
    {
        self.project().sender.gso(data, addr)
    }
}

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source>
    Socket<'d, ID, B, send::Sourced>
{
    /// Queues bytes using the source and peer selected together during setup.
    pub fn queue(
        self: pin::Pin<&mut Self>,
        data: B,
        source: source::Source,
    ) -> Result<(), Rejected<B>> {
        self.project().sender.try_queue(
            data,
            source.peer(),
            source.control(),
            kernel::Payload::Buffer,
        )?;
        Ok(())
    }
}

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    Socket<'d, ID, B, M>
{
    pub fn retainer(self: pin::Pin<&Self>) -> packet::Retainer<'d> {
        self.get_ref().receive.retainer()
    }

    pub fn wake_target(self: pin::Pin<&Self>) -> ready::Target<'d> {
        self.get_ref().receive.wake_target()
    }

    pub(in crate::network::datagram) fn release(
        mut self: pin::Pin<&mut Self>,
        event: io::Event<'d>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let done = match event.into_kind() {
            event::Kind::Send(done) => done,
            event => return ops::ControlFlow::Break(event.into()),
        };
        match self.as_mut().project().sender.complete(done) {
            Ok(_) => ops::ControlFlow::Continue(()),
            Err(done) => ops::ControlFlow::Break(event::Kind::Send(done).into()),
        }
    }

    pub(in crate::network::datagram) fn settle<H>(
        mut self: pin::Pin<&mut Self>,
        token: &mut region::Token<region::Identity<'d, 1>>,
        event: io::Event<'d>,
        mut sink: pin::Pin<&mut H>,
        now: clock::Moment<'d>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        H: super::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        let done = match event.into_kind() {
            event::Kind::Send(done) => done,
            event => return ops::ControlFlow::Break(event.into()),
        };
        match self
            .as_mut()
            .project()
            .sender
            .settle(token, done, now, sink.as_mut())
        {
            Ok(()) => ops::ControlFlow::Continue(()),
            Err(done) => ops::ControlFlow::Break(event::Kind::Send(done).into()),
        }
    }

    pub(in crate::network::datagram) fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        self.sender
            .progress(region)
            .reduce(self.receive.progress(region))
    }

    pub(in crate::network::datagram) fn stop(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.project();
        this.sender.stop();
        this.receive.stop(driver);
    }

    pub(in crate::network::datagram) fn drain(
        self: pin::Pin<&mut Self>,
        work: maint::Work<'_, 'd>,
    ) {
        self.project().sender.drain(work, drop);
    }

    pub(in crate::network::datagram) fn drain_to<H>(
        self: pin::Pin<&mut Self>,
        token: &mut region::Token<region::Identity<'d, 1>>,
        work: maint::Work<'_, 'd>,
        now: clock::Moment<'d>,
        sink: pin::Pin<&mut H>,
    ) where
        H: super::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        self.project().sender.drain_event(token, work, now, sink);
    }

    pub(in crate::network::datagram) fn finish(
        self: pin::Pin<&mut Self>,
        ctx: &mut finish::Finish<'_, 'd>,
    ) {
        let mut this = self.project();
        if ctx.is_forced() || !this.sender.is_empty() {
            this.sender.finish(drop);
        }
        this.receive.as_mut().finish(ctx);
    }

    pub(in crate::network::datagram) fn finish_to<H>(
        self: pin::Pin<&mut Self>,
        mut sink: pin::Pin<&mut H>,
        ctx: &mut finish::Finish<'_, 'd>,
    ) where
        H: super::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        let now = ctx.clock().now();
        let mut this = self.project();
        if ctx.is_forced() || !this.sender.is_empty() {
            this.sender
                .finish_event(ctx.state_token(), now, sink.as_mut());
        }
        this.receive.as_mut().finish(ctx);
    }
}
