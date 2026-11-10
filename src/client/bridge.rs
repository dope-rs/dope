use crate::runtime::brand;

use std::{ops, pin};

use o3::buffer::resident;

use dope::core::{
    driver::{result::SubmitCause, schedule::clock},
    io::datagram::ReceiveError,
};

use crate::client;
use crate::client::life;
use crate::client::protocol;
use crate::conn::{id, scope, stream::Event};
use crate::runtime::mux;
use data::{datagram, stream};
use handler::hook;
use hook::life::{close, closing, create, ready};
use hook::{data, set, wire, writable};
use life::view;
use mux::handler;
use wire::{cert, endpoint, ingress, packet, socket};
use writable::Interest as _;

#[pin_project::pin_project]
pub struct Bridge<'d, const DOMAIN: u8, P: protocol::Protocol<'d>, B> {
    #[pin]
    protocol: P,
    lifecycle: clock::Bound<'d, life::Lifecycle<'d, DOMAIN, B>>,
    writable: <P::Writable as writable::Interest>::State,
}

impl<'d, const DOMAIN: u8, P: protocol::Protocol<'d>, B: client::Backoff> Bridge<'d, DOMAIN, P, B> {
    pub(super) fn new(protocol: P, life: clock::Bound<'d, life::Lifecycle<'d, DOMAIN, B>>) -> Self {
        Self {
            protocol,
            lifecycle: life,
            writable: P::Writable::EMPTY,
        }
    }

    pub(super) fn protocol(&self) -> &P {
        &self.protocol
    }

    pub(super) fn failure(&self) -> Option<protocol::Failure> {
        self.lifecycle.failure()
    }

    pub(super) fn take(self: pin::Pin<&mut Self>) -> writable::Receipt {
        P::Writable::exchange(self.project().writable, writable::Receipt::Idle)
    }

    pub(super) fn protocol_pin(self: pin::Pin<&mut Self>) -> pin::Pin<&mut P> {
        self.project().protocol
    }

    pub(super) fn lifecycle(&self) -> view::View<'_, 'd, DOMAIN, B> {
        self.lifecycle.view()
    }

    pub(super) fn life_mut(self: pin::Pin<&mut Self>) -> &mut life::Lifecycle<'d, DOMAIN, B> {
        self.project().lifecycle
    }

    pub(super) fn bind_input<'scope>(
        &self,
        conn: &mut mux::app::Connection<
            'scope,
            (),
            DOMAIN,
            P::Receive,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
    ) -> Result<Option<client::SlotScope<'scope>>, crate::errors::Access> {
        let handle = conn.handle();
        let scope = conn.transport().brand();
        match self.lifecycle().bind_active(scope, handle) {
            Ok(Some(bound)) => Ok(Some(bound)),
            pending => {
                // The budgeted connect callback resumes this exact native
                // source; its untouched input needs no interim app polling.
                conn.idle();
                pending
            }
        }
    }

    pub(super) fn has_input<'scope>(
        &self,
        conn: &mut scope::Access<'scope, DOMAIN, P::Receive, true, resident::Shared<'d>>,
    ) -> bool {
        (P::DATAGRAMS == protocol::Datagrams::Pull && conn.events().has_datagram())
            || (P::STREAMS == protocol::Streams::Pull && conn.events().has_stream())
    }

    /// Both callback and pull delivery consume the same native event owner.
    pub(super) fn receive_stream<'scope>(
        self: pin::Pin<&mut Self>,
        conn: &mut scope::Access<'scope, DOMAIN, P::Receive, true, resident::Shared<'d>>,
        event: Event,
    ) -> (Option<protocol::Stream<P::Receive>>, bool) {
        let id = match event {
            Event::Writable => {
                P::Writable::exchange(self.project().writable, writable::Receipt::Ready);
                return (Some(protocol::Stream::Writable), true);
            }
            Event::Reset { id, error_code } => {
                return (Some(protocol::Stream::Reset { id, error_code }), true);
            }
            Event::Stopped { id, error_code } => {
                return (Some(protocol::Stream::Stopped { id, error_code }), true);
            }
            Event::Readable { id } => id,
        };
        let mut streams = conn.streams();
        let Ok(stream) = streams.view().resolve_recv(id) else {
            return (None, true);
        };
        let data = streams.receive().recv_buffer(stream);
        let end = streams.view().is_recv_eof(stream);
        if data.is_none() && !end {
            return (None, true);
        }
        (Some(protocol::Stream::Readable { id, data, end }), end)
    }

    /// Completes resident lifecycle transitions and then invokes the matching
    /// pinned protocol callbacks. Both normal driver progress and shutdown use
    /// this one ordering auth.
    pub(super) fn drain_events(
        mut self: pin::Pin<&mut Self>,
        budget: usize,
        mut resume: impl FnMut(id::Handle<id::Driver<'d>, DOMAIN>),
    ) -> usize {
        brand::Token::with(move |scope| {
            let mut left = budget;
            while left != 0 {
                let slot = {
                    let this = self.as_mut().project();
                    this.lifecycle.settle_close(scope.reborrow())
                };
                let Some(slot) = slot else { break };
                left -= 1;
                let this = self.as_mut().project();
                protocol::Protocol::close(this.protocol, slot);
            }
            while left != 0 {
                let slot = {
                    let this = self.as_mut().project();
                    this.lifecycle.settle_ready(scope.reborrow())
                };
                let Some((slot, handle)) = slot else { break };
                left -= 1;
                let this = self.as_mut().project();
                protocol::Protocol::connect(this.protocol, slot);
                resume(handle);
            }
            budget - left
        })
    }
}

impl<'d, const DOMAIN: u8, P, B>
    handler::Handler<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    type Connection = ();
    type Create = create::Unit;
    type Events = set::Set<
        set::On,
        Self,
        Self,
        set::Off,
        set::Off,
        set::On,
        ingress::Off,
        packet::On,
        endpoint::Off,
        closing::On,
        close::On,
        <P::Writable as writable::Interest>::Hook<
            Self,
            DOMAIN,
            P::Receive,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
    >;
}

impl<'d, const DOMAIN: u8, P, B>
    set::Datagram<Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    const PULL: bool = matches!(P::DATAGRAMS, protocol::Datagrams::Pull);

    fn call<'scope, F>(take: impl FnOnce() -> Option<F>) -> ops::ControlFlow<()>
    where
        F: FnOnce() -> datagram::Event<
            'scope,
            Self,
            DOMAIN,
            P::Receive,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
        Self: 'scope,
        P::Receive: 'scope,
        resident::Shared<'d>: 'scope,
    {
        <set::On as set::Datagram<
            Self,
            DOMAIN,
            P::Receive,
            resident::Shared<'d>,
            id::Driver<'d>,
        >>::call(take)
    }
}

impl<'d, const DOMAIN: u8, P, B>
    socket::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn socket(event: socket::Event<'_, Self, DOMAIN, id::Driver<'d>>) {
        use handler::net;

        let (bridge, event) = event.into_parts();
        let failure = match &event {
            net::Event::Rejected { error, .. } => Some(protocol::Failure::Rejected(*error)),
            event => {
                let error = match event {
                    net::Event::Receive {
                        error: ReceiveError::Submit(error),
                        ..
                    } => Some(*error),
                    net::Event::Settled(settlement) => match settlement.outcome() {
                        net::Outcome::Submit(error) => Some(*error),
                        _ => None,
                    },
                    _ => None,
                };
                error
                    .filter(|error| error.cause() == SubmitCause::IdentityExhausted)
                    .map(protocol::Failure::Submit)
            }
        };
        let this = bridge.project();
        if let Some(failure) = failure {
            this.lifecycle.fail(failure);
        }
        this.protocol.socket(&event);
    }
}

impl<'d, const DOMAIN: u8, P, B> cert::Sink<DOMAIN, id::Driver<'d>> for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
{
    fn certificate(event: cert::Event<'_, '_, Self, DOMAIN, id::Driver<'d>>) {
        let (this, handle, from, failure) = event.into_parts();
        this.project().protocol.certificate(handle, from, failure);
    }
}

impl<'d, const DOMAIN: u8, P, B>
    packet::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn packet(event: packet::Event<'_, Self>) {
        let (this, from, error, len) = event.into_parts();
        this.project().protocol.packet(from, error, len);
    }
}

impl<'d, const DOMAIN: u8, P, B>
    ready::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn established(
        mut event: ready::Event<'_, Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>,
    ) {
        event.defer();
        let (this, _, handle, _) = event.into();
        this.project().lifecycle.establish(handle);
    }
}

impl<'d, const DOMAIN: u8, P, B>
    datagram::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn datagram(
        event: datagram::Event<'_, Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>,
    ) {
        let (this, slot, handle, data, _) = event.into();
        let scope = slot.brand();
        let this = this.project();
        if let Some(bound) = this.lifecycle.view().bind(scope, handle) {
            protocol::Protocol::datagram(this.protocol, bound, data);
        }
    }
}

impl<'d, const DOMAIN: u8, P, B>
    set::Stream<Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    const PULL: bool = matches!(P::STREAMS, protocol::Streams::Pull);

    fn call<'scope>(
        event: impl FnOnce() -> stream::Event<
            'scope,
            Self,
            DOMAIN,
            P::Receive,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
    ) -> mux::Disposition
    where
        Self: 'scope,
        P::Receive: 'scope,
        resident::Shared<'d>: 'scope,
    {
        <Self as stream::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>>::stream(
            event(),
        )
    }

    fn early<'scope>(
        event: impl FnOnce() -> stream::Event<
            'scope,
            Self,
            DOMAIN,
            P::Receive,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
    ) -> mux::Disposition
    where
        Self: 'scope,
        P::Receive: 'scope,
        resident::Shared<'d>: 'scope,
    {
        <Self as stream::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>>::early(
            event(),
        )
    }
}

impl<'d, const DOMAIN: u8, P, B>
    stream::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn early(
        _: stream::Event<'_, Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition {
        mux::Disposition::Defer
    }

    fn stream(
        event: stream::Event<'_, Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>,
    ) -> mux::Disposition {
        let (mut this, mut slot, handle, event, _) = event.into();
        let scope = slot.brand();
        let Some(bound) = this.lifecycle().bind(scope, handle) else {
            return mux::Disposition::Accept;
        };
        let (input, accept) = this
            .as_mut()
            .receive_stream(slot.transport(), event.event());
        let this = this.project();
        match input {
            Some(protocol::Stream::Readable { id, data, end }) => {
                protocol::Protocol::stream(this.protocol, bound, id, data, end);
            }
            Some(protocol::Stream::Reset { id, error_code }) => {
                protocol::Protocol::stream_event(
                    this.protocol,
                    bound,
                    Event::Reset { id, error_code },
                );
            }
            Some(protocol::Stream::Stopped { id, error_code }) => {
                protocol::Protocol::stream_event(
                    this.protocol,
                    bound,
                    Event::Stopped { id, error_code },
                );
            }
            Some(protocol::Stream::Writable) | None => {}
        }
        if accept {
            mux::Disposition::Accept
        } else {
            mux::Disposition::Defer
        }
    }
}

impl<'d, const DOMAIN: u8, P, B>
    writable::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn writable(self: pin::Pin<&mut Self>) {
        P::Writable::exchange(self.project().writable, writable::Receipt::Ready);
    }

    fn has_writable(&self) -> bool {
        P::Writable::is_pending(&self.writable)
    }

    fn clear(self: pin::Pin<&mut Self>) {
        P::Writable::exchange(self.project().writable, writable::Receipt::Idle);
    }
}

impl<'d, const DOMAIN: u8, P, B>
    close::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn close(
        event: close::Event<'_, Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>,
    ) {
        let (this, close, (), handle) = event.into_parts();
        this.project()
            .lifecycle
            .close_at(close.brand(), handle, close.at());
    }
}

impl<'d, const DOMAIN: u8, P, B>
    closing::Sink<DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>
    for Bridge<'d, DOMAIN, P, B>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
{
    fn closing(
        event: closing::Event<'_, Self, DOMAIN, P::Receive, resident::Shared<'d>, id::Driver<'d>>,
    ) {
        let (event, again) = event.into_saved();
        let (this, mut slot, handle) = event.into();
        let brand = slot.brand();
        let mut this = this.project();
        if let Some(bound) = this.lifecycle.reconnect(brand.reborrow(), handle, again) {
            this.protocol
                .as_mut()
                .closing(bound, slot.transport().view().lifecycle());
        }
    }
}
