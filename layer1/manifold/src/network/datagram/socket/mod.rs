use std::{num, ops, pin};

use dope_core::{
    driver,
    driver::{
        life::finish,
        result, retained,
        retained::flight,
        route,
        route::{Identity, kind},
        schedule,
        schedule::{clock, maint, ready},
    },
    io,
    io::{datagram, datagram::source, event, fd::handles},
};
use o3::{
    base::cell::region,
    buffer::{self, bytes::bounded, pool},
};
use send::{kernel, owners};

use crate::network::datagram::{RejectCause, Rejected, hook, packet, send};

pub(in crate::network::datagram) mod binding;
pub(in crate::network::datagram) mod priority;
pub(in crate::network::datagram) mod receive;
mod retire;
pub(in crate::network::datagram) mod sender;

#[pin_project::pin_project(!Unpin)]
pub struct Socket<'d, ID: Identity, B = pool::Cursor, M: send::Mode = send::Plain> {
    #[pin]
    pub(in crate::network::datagram) receive: receive::Receive<'d, ID>,
    pub(in crate::network::datagram) recv_flights:
        flight::Slots<'d, <ID as route::Materialize<kind::Recv>>::With>,
    pub(in crate::network::datagram) sender: sender::Sender<'d, ID, B, M>,
    pub(in crate::network::datagram) binding: binding::Binding<'d, ID>,
    pub(in crate::network::datagram) priority: priority::Priority,
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
        let Some(limits) = datagram::GSO_LIMITS else {
            return Err(Rejected::new(data, RejectCause::GsoUnavailable));
        };
        let data = match bounded::Value::try_new(data) {
            Ok(data) => data,
            Err(rejected) => {
                let (data, cause) = rejected.into_parts();
                return Err(Rejected::new(
                    data,
                    RejectCause::GsoShape(datagram::Shape::Transfer(cause)),
                ));
            }
        };
        let plan = match limits.payload(data, size) {
            Ok(plan) => plan,
            Err(rejected) => {
                let (data, cause) = rejected.into_parts();
                return Err(Rejected::new(
                    data.into_inner(),
                    RejectCause::GsoShape(cause),
                ));
            }
        };
        let Some(gso) = datagram::Gso::acquire() else {
            return Err(Rejected::new(
                plan.into_payload().into_inner(),
                RejectCause::GsoUnavailable,
            ));
        };
        self.project().sender.gso(gso.payload(plan), addr)
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
        self.project().sender.sourced(data, source)
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
        match self.as_mut().project().sender.retire().complete(done) {
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
            .retire()
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
        self.project().sender.retire().drain(work, drop);
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
        self.project()
            .sender
            .retire()
            .drain_event(token, work, now, sink);
    }

    pub(in crate::network::datagram) fn finish(
        self: pin::Pin<&mut Self>,
        ctx: &mut finish::Finish<'_, 'd>,
    ) {
        let mut this = self.project();
        if ctx.is_forced() || !this.sender.is_empty() {
            this.sender.retire().finish(drop);
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
                .retire()
                .finish_event(ctx.state_token(), now, sink.as_mut());
        }
        this.receive.as_mut().finish(ctx);
    }
}

fn try_submit<'a, 'owner, 'd, ID: Identity, B: AsRef<[u8]>, M: send::Mode>(
    fd: &'a handles::Datagram<'d>,
    send: &'a mut send::Draft<'_, 'd, ID, B, M>,
    target: route::Target<'d, <ID as route::Materialize<kind::Send>>::With>,
    driver: retained::Context<'_, 'owner, 'd>,
    slots: &flight::Slots<'d, <ID as route::Materialize<kind::Send>>::With>,
) -> Result<flight::Flight<'d, kind::Send>, result::SubmitError>
where
    'd: 'owner,
    'owner: 'a,
{
    let op = retained::raw::Submission::send_msg(fd, send.fill_message(), target);
    // SAFETY: the reserved slab slot makes the draft's complete pointer
    // graph stable; publication follows only a successful submission.
    unsafe { op.try_submit(driver, slots) }
}
