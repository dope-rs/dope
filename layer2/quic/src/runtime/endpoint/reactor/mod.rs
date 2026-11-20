#![deny(unsafe_code)]

use crate::runtime::endpoint::Payload;
use dope::core::driver::{result::SubmitCause, route};
use o3::mem::quota;

use dope::core::io::datagram::{ReceiveError, Residency};
use o3::buffer;

use crate::proto::{cid, datagram::Sealed};

pub(super) mod buffers;
pub mod input;
pub(super) mod queue;

use std::{net::SocketAddr, ops::ControlFlow, pin, time};

use dope::{
    core::driver::schedule::{self, app, clock},
    manifold::network::datagram::{self, hook, packet, send},
};
use handler::{
    hook::{
        set::{Dispatch, Socket as _},
        wire::{
            cert,
            endpoint::{self, Event},
            packet::Mode,
            socket,
            udp::Storage as _,
        },
        writable::Mode as _,
    },
    net,
};
use hook::ingress::Coupled;
use manage::config;
use o3::{
    base::{Call as _, cell::region},
    buffer::resident::Shared,
};
use output::{gate::Gate, quantum::Quantum};

use crate::{
    conn::id,
    runtime::{
        endpoint::{Idle, Input, Interest, Udp, control, owner},
        mux::{
            app::{Accepted, Receipt, Silent},
            auth, handler, manage, output, router,
        },
        receive::Storage,
        routing,
    },
    stream,
};

#[derive(dope::WorkGraph)]
pub(in crate::runtime::endpoint) enum Net {
    Output,
    Transport,
}

#[derive(dope::WorkGraph)]
pub(in crate::runtime::endpoint) enum Work {
    Application,
    Output,
    Transport,
}

#[pin_project::pin_project(!Unpin)]
pub struct Engine<
    'd,
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    const ID: u8,
    B: stream::Recv,
    A = Idle,
    M = auth::Owned<routing::Plain, id::Driver<'d>>,
    Q: buffer::Residency + buffer::raw::Source = Sealed<'d>,
> where
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
{
    #[pin]
    pub(super) mux: router::Router<H, ID, B, M>,
    pub(super) rotation: clock::Bound<'d, cid::Schedule>,
    pub(super) app: A,
    pub(super) io: Io<'d, H, ID, B, Q>,
}

pub(super) struct Io<'d, H, const ID: u8, B, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: stream::Recv,
{
    pooled: buffers::Pending<Q, Interest<'d, H, ID, B>>,
    pub(super) input: Input<'d, H, ID, B>,
    flush: Gate,
    stopping: bool,
}

impl<'d, H, const ID: u8, B, A, M, Q: buffer::Residency + buffer::raw::Source>
    Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: stream::Recv,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
{
    pub(super) fn new(
        mux: router::Router<H, ID, B, M>,
        app: A,
        rotation: clock::Bound<'d, cid::Schedule>,
        input: Input<'d, H, ID, B>,
    ) -> Self {
        Self {
            mux,
            rotation,
            app,
            io: Io {
                pooled: buffers::Pending::new(),
                input,
                flush: Gate::new(),
                stopping: false,
            },
        }
    }

    pub(super) fn mux_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut router::Router<H, ID, B, M>> {
        self.project().mux
    }

    pub(super) fn handler_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut H> {
        self.project().mux.handler_mut()
    }

    pub(super) fn config(self: pin::Pin<&mut Self>) -> config::Config<'_, '_, H, ID, B, M> {
        self.project().mux.config()
    }

    pub(super) fn try_sender<N: owner::Binding<'d, ID, Mode = M>>(
        self: pin::Pin<&mut Self>,
        now: time::Instant,
    ) -> Result<control::Sender<'_, 'd, ID, H, B, N, Q>, control::SendError> {
        let this = self.project();
        if this.io.stopping {
            return Err(control::SendError::Closed);
        }
        let slot = this.io.pooled.vacant().ok_or(control::SendError::Full)?;
        Ok(control::Sender::new(slot, this.mux, now))
    }

    pub(super) fn transport(
        mux: pin::Pin<&mut router::Router<H, ID, B, M>>,
        now: clock::Moment<'d>,
        work: app::Work<'_, 'd>,
    ) -> quota::Admission<bool> {
        if !mux.as_ref().schedule(now.into()).is_break() {
            return quota::Admission::Empty;
        }
        let Some(permit) = work.permit() else {
            return quota::Admission::Exhausted;
        };
        mux.drive_net(permit, now);
        quota::Admission::Item(false)
    }

    pub(super) fn progress(
        self: pin::Pin<&Self>,
        clock: clock::Clock<'_, 'd>,
        ready: impl FnOnce(schedule::Progress<'d>) -> schedule::Progress<'d>,
    ) -> schedule::Progress<'d>
    where
        B: Storage<'d, Shared<'d>>,
    {
        let this = self.project_ref();
        if this.io.stopping {
            return if this.mux.is_done() {
                schedule::Progress::Quiescent
            } else {
                schedule::Progress::Runnable
            };
        }
        let mut state = schedule::Progress::Quiescent;
        if <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Writable::is_pending(
            this.mux.handler(),
        ) {
            state = state.reduce(schedule::Progress::Runnable);
        }
        if Udp::<'d, H, ID, B>::pending::<ID>(&this.io.input)
            || ((this.io.pooled.is_some() || this.mux.has_output()) && this.io.flush.is_open())
        {
            state = state.reduce(schedule::Progress::Runnable);
        }
        let now = clock.now();
        state = state.reduce(
            now.at(this.mux.as_ref())
                .apply(|mux, raw| match mux.schedule(raw) {
                    ControlFlow::Break(_) => schedule::Progress::Runnable,
                    ControlFlow::Continue(Some(at)) => {
                        schedule::Progress::until(now.deadline_at(at))
                    }
                    ControlFlow::Continue(None) => schedule::Progress::Quiescent,
                }),
        );
        if let Some(until) = this.rotation.deadline() {
            state = state.reduce(schedule::Progress::until(clock.deadline_at(until)));
        }
        ready(state)
    }
}

impl<'d, H, const ID: u8, B, Q> Io<'d, H, ID, B, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: stream::Recv,
{
    fn stop<M>(
        &mut self,
        mut mux: pin::Pin<&mut router::Router<H, ID, B, M>>,
        rotation: &mut clock::Bound<'d, cid::Schedule>,
    ) where
        M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
    {
        rotation.seal();
        self.pooled.clear();
        if let Some(input) = Udp::<'d, H, ID, B>::state::<ID>(&mut self.input) {
            input.clear();
        }
        if let Some(clear) =
            <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Writable::clear()
        {
            clear.call(mux.as_mut().handler_mut());
        }
        self.stopping = true;
        manage::Shutdown::new(mux).begin();
    }

    pub(super) fn begin<M>(
        &mut self,
        mux: pin::Pin<&mut router::Router<H, ID, B, M>>,
        now: clock::Moment<'d>,
        work: app::Work<'_, 'd>,
    ) -> bool
    where
        M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
    {
        if self.stopping {
            let mut shutdown = manage::Shutdown::new(mux);
            while !shutdown.is_complete() {
                let Some(permit) = work.permit() else { break };
                shutdown.step(permit, now.into());
            }
            return false;
        }
        self.flush.open();
        true
    }

    pub(super) fn output<R, A, M>(
        &mut self,
        mut mux: pin::Pin<&mut router::Router<H, ID, B, M>>,
        socket: pin::Pin<&mut datagram::Socket<'d, route::KeyTag<ID>, Payload<Q>>>,
        rotation: &mut clock::Bound<'d, cid::Schedule>,
        now: clock::Moment<'d>,
        work: app::Work<'_, 'd>,
        quantum: &mut Quantum,
        app: &mut A,
    ) -> quota::Admission<bool>
    where
        B: Storage<'d, Shared<'d>>,
        M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
        Q: buffer::Residency + buffer::raw::Source,
        H: cert::Sink<ID, id::Driver<'d>>,
        <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Packet:
            Mode<H, ID, B, Shared<'d>, id::Driver<'d>, Output = ()>,
        R: Receipt<'d, H, ID, B, Shared<'d>, A>,
    {
        let output =
            (self.pooled.is_some() || !mux.as_mut().output().is_empty()) && self.flush.is_open();
        if !output && !Udp::<'d, H, ID, B>::pending::<ID>(&self.input) {
            return quota::Admission::Empty;
        }
        let Some(permit) = work.permit() else {
            return quota::Admission::Exhausted;
        };
        if let Some(input) = Udp::<'d, H, ID, B>::state::<ID>(&mut self.input)
            && input.select(output)
        {
            let failed = permit
                .run(|| input.step(mux.as_mut(), socket, now))
                .is_break();
            if failed {
                self.stop(mux, rotation);
            }
            return quota::Admission::Item(failed);
        }
        let Some(credit) = quantum.credit() else {
            return quota::Admission::Item(true);
        };
        match queue::Queue::new(socket, &mut self.pooled, mux.as_mut()).one(permit, credit) {
            output::Submit::Empty => quota::Admission::Item(false),
            output::Submit::Deferred => quota::Admission::Item(true),
            output::Submit::Accepted((), grant) => {
                if let Some(observer) = R::observer(app) {
                    observer.accepted();
                }
                quota::Admission::Item(grant.commit().is_exhausted())
            }
            output::Submit::Rejected(cause) => {
                self.flush.block();
                if queue::Queue::<ID, H, B, M, Q>::rejection(mux.as_mut(), cause).is_break() {
                    self.stop(mux, rotation);
                }
                quota::Admission::Item(self.stopping)
            }
        }
    }
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source>
    datagram::Ingress<'d, route::KeyTag<ID>> for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
    H: cert::Sink<ID, id::Driver<'d>>,
    <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Packet:
        Mode<H, ID, B, Shared<'d>, id::Driver<'d>, Output = ()>,
{
    type Hooks = hook::Set<
        <<H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Endpoint as endpoint::Mode<H, ID, B, Shared<'d>, id::Driver<'d>>>::Install<'d, ID, Self>,
        hook::Enabled,
        <<H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Endpoint as endpoint::Mode<H, ID, B, Shared<'d>, id::Driver<'d>>>::Finalize<'d, ID, Self>,
        A,
        hook::Enabled,
        Payload<Q>,
    >;
    type Mode = Coupled;
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source>
    datagram::Coupled<'d, route::KeyTag<ID>> for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
    H: cert::Sink<ID, id::Driver<'d>>,
    <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Packet:
        Mode<H, ID, B, Shared<'d>, id::Driver<'d>, Output = ()>,
{
    fn packet<'turn>(
        mut self: pin::Pin<&mut Self>,
        received: Result<(SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>,
        socket: pin::Pin<&'turn mut datagram::Socket<'d, route::KeyTag<ID>, Payload<Q>>>,
        now: clock::Moment<'d>,
    ) {
        let (addr, packet) = match received {
            Ok(packet) => packet,
            Err(error) => {
                let mut this = self.as_mut().project();
                let endpoint = this.mux.as_ref().get_ref().domain();
                let handler = this.mux.as_mut().handler_mut();
                <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Socket::call(|| {
                    socket::Event::new((handler, net::Event::Receive { endpoint, error }))
                });
                if matches!(error, ReceiveError::Submit(error)
                    if error.cause() == SubmitCause::IdentityExhausted)
                    && !this.io.stopping
                {
                    this.io.stop(this.mux, this.rotation);
                }
                return;
            }
        };
        let keep = socket.as_ref().retainer();
        let mut this = self.project();
        let packet = if let Some(input) = Udp::<'d, H, ID, B>::state::<ID>(&mut this.io.input) {
            match input.intercept(addr, packet, this.mux.as_mut().handler_mut()) {
                ControlFlow::Continue(packet) => packet,
                ControlFlow::Break(()) => return,
            }
        } else {
            packet
        };
        this.mux.observe(now, |mut turn| {
            let () = turn.receive(addr, packet, keep);
        });
    }
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source>
    datagram::Settlement<'d, route::KeyTag<ID>> for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
    H: cert::Sink<ID, id::Driver<'d>>,
    <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Packet:
        Mode<H, ID, B, Shared<'d>, id::Driver<'d>, Output = ()>,
{
    fn settle<'turn>(
        self: pin::Pin<&mut Self>,
        done: send::Settlement<'turn, 'd, route::KeyTag<ID>, Payload<Q>>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let mut this = self.project();
        let failed = Udp::<'d, H, ID, B>::state::<ID>(&mut this.io.input)
            .is_some_and(|input| input.settled(&done))
            || matches!(done.outcome(), send::Outcome::Submit(error)
            if error.cause() == SubmitCause::IdentityExhausted);
        let gso_failed = matches!(done.outcome(), send::Outcome::GsoFailed(_));
        let domain = this.mux.as_ref().get_ref().domain();
        let handler = this.mux.as_mut().handler_mut();
        <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Socket::call(|| {
            socket::Event::new((
                handler,
                net::Event::Settled(net::Settlement::from_driver(domain, &done)),
            ))
        });
        let (_, _, _, data, _) = done.into_parts();
        this.mux.as_mut().output().reclaim(data, gso_failed);
        if failed && !this.io.stopping {
            this.io.stop(this.mux, this.rotation);
        }
    }
}

impl<'d, const ID: u8, H, B, M, Q: buffer::Residency + buffer::raw::Source>
    datagram::Transport<'d, route::KeyTag<ID>> for Engine<'d, H, ID, B, Idle, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
    H: cert::Sink<ID, id::Driver<'d>>,
    <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Packet:
        Mode<H, ID, B, Shared<'d>, id::Driver<'d>, Output = ()>,
{
    fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        mut socket: pin::Pin<&mut datagram::Socket<'d, route::KeyTag<ID>, Payload<Q>>>,
        now: clock::Moment<'d>,
        work: app::Work<'turn, 'd>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let this = self.project();
        let mut mux = this.mux;
        if !this.io.begin(mux.as_mut(), now, work) {
            return;
        }
        let mut quantum = Quantum::new();
        let mut admit = |source| match source {
            Net::Output => this.io.output::<Silent, _, _>(
                mux.as_mut(),
                socket.as_mut(),
                this.rotation,
                now,
                work,
                &mut quantum,
                &mut (),
            ),
            Net::Transport => Self::transport(mux.as_mut(), now, work),
        };
        while let quota::Admission::Item(false) = this.app.work.admit(&mut admit) {}
    }

    fn progress(
        self: pin::Pin<&Self>,
        clock: clock::Clock<'_, 'd>,
        _: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        use std::convert::identity;

        self.progress(clock, identity)
    }
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source> datagram::Install
    for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
{
    fn installed(self: pin::Pin<&mut Self>) {
        let domain = self.as_ref().project_ref().mux.domain();
        let handler = self.project().mux.handler_mut();
        <<H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Endpoint as endpoint::Mode<
            H,
            ID,
            B,
            Shared<'d>,
            id::Driver<'d>,
        >>::call(|| Event::opened(handler, domain));
    }
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source> datagram::Shutdown
    for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
{
    fn shutdown(self: pin::Pin<&mut Self>) {
        let this = self.project();
        this.io.stop(this.mux, this.rotation);
    }
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source> datagram::Finalize
    for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
{
    fn finalized(self: pin::Pin<&mut Self>, state: Option<Residency>) {
        let mut this = self.project();
        let domain = this.mux.as_ref().get_ref().domain();
        let handler = this.mux.as_mut().handler_mut();
        <<H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Endpoint as endpoint::Mode<
            H,
            ID,
            B,
            Shared<'d>,
            id::Driver<'d>,
        >>::call(|| Event::finalized(handler, domain, state));
    }
}

impl<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source>
    datagram::Transfer<'d, route::KeyTag<ID>> for Engine<'d, H, ID, B, A, M, Q>
where
    H: handler::Handler<ID, B, Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, Shared<'d>>,
    M: auth::Mode<ID, Send = Shared<'d>, Identity = id::Driver<'d>>,
{
    fn transferred(self: pin::Pin<&mut Self>, sent: send::Transfer<'d, route::KeyTag<ID>>) {
        let mut this = self.project();
        if let Some(input) = Udp::<'d, H, ID, B>::state::<ID>(&mut this.io.input) {
            input.transferred(sent);
        }
        let domain = this.mux.as_ref().get_ref().domain();
        let handler = this.mux.as_mut().handler_mut();
        <H::Events as Dispatch<H, ID, B, Shared<'d>, id::Driver<'d>>>::Socket::call(|| {
            socket::Event::new((
                handler,
                net::Event::Transferred(net::Transfer::from_driver(domain, sent)),
            ))
        });
    }
}
