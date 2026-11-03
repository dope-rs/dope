use crate::runtime::mux::handler::hook::wire::packet;
use crate::runtime::mux::handler::hook::writable;
use crate::runtime::mux::handler::net as socket;
use std::net;
use std::pin;

use dope::{
    core::driver::result::SubmitError,
    manifold::{app::dispatch, network::datagram::RejectCause},
};
use o3::buffer::resident;

use crate::conn::{id, stream::Event};
use crate::runtime::receive::Storage;
use crate::wire::stream::Id;
use crate::{client, conn, errors};

/// Permanent failure retained by the exact client endpoint.
/// Further attempts are disabled; existing connections retain their close obligations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Submit(SubmitError),
    Rejected(RejectCause),
    Exhausted {
        attempt: client::AttemptId,
        cause: client::Exhausted,
    },
    Timing {
        attempt: client::AttemptId,
        cause: conn::Timing,
    },
}

/// Selects who consumes the client's native DATAGRAM receive queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Datagrams {
    /// Removes each payload before invoking `Protocol::datagram`.
    Push,
    /// Retains each payload until `Control::at(...).recv_datagram()` consumes it.
    Pull,
}

/// Selects who consumes the client's native stream event and receive queues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Streams {
    Push,
    /// Retains events and their original payloads until an application turn.
    Pull,
}

/// One native stream event, with at most one original receive fragment.
pub enum Stream<B> {
    Readable { id: Id, data: Option<B>, end: bool },
    Reset { id: Id, error_code: errors::Code },
    Stopped { id: Id, error_code: errors::Code },
    Writable,
}

pub trait Protocol<'d> {
    type Receive: Storage<'d, resident::Shared<'d>>;
    type Writable: writable::Interest;

    const DATAGRAMS: Datagrams = Datagrams::Push;
    const STREAMS: Streams = Streams::Push;

    fn connect<'scope>(self: pin::Pin<&'scope mut Self>, slot: client::SlotScope<'scope>);

    fn failed(
        self: pin::Pin<&mut Self>,
        attempt: client::AttemptScope<'_, 'd>,
        error: &errors::Connect,
    );

    fn datagram<'scope>(
        self: pin::Pin<&'scope mut Self>,
        slot: client::SlotScope<'scope>,
        data: Self::Receive,
    );

    /// Delivers bytes and clean receive EOF status for the currently bound attempt.
    /// Process present bytes before `end`; absent data is delivered only at EOF.
    /// Retained receive credit can allow a duplicate FIN to repeat the end status.
    fn stream<'scope>(
        self: pin::Pin<&'scope mut Self>,
        slot: client::SlotScope<'scope>,
        id: Id,
        data: Option<Self::Receive>,
        end: bool,
    );

    /// Delivers peer Reset and Stopped events for the currently bound attempt.
    fn stream_event<'scope>(
        self: pin::Pin<&'scope mut Self>,
        slot: client::SlotScope<'scope>,
        event: Event,
    );

    fn close<'scope>(self: pin::Pin<&'scope mut Self>, slot: client::SlotScope<'scope>);

    /// Observes socket events with their original endpoint and payload owners.
    /// Settled payloads remain borrowed until this call returns, then are recycled.
    /// Native send errors do not become permanent client failures.
    fn socket<const DOMAIN: u8>(
        self: pin::Pin<&mut Self>,
        _: &socket::Event<'_, id::Driver<'d>, DOMAIN>,
    ) {
    }

    fn packet(
        self: pin::Pin<&mut Self>,
        from: net::SocketAddr,
        error: packet::Failure<'_>,
        len: usize,
    );

    fn certificate<const DOMAIN: u8>(
        self: pin::Pin<&mut Self>,
        handle: id::Handle<id::Driver<'d>, DOMAIN>,
        from: net::SocketAddr,
        failure: &rustls::Error,
    );
}

/// Custom control construction from the exact installed protocol loan.
pub trait Control<'d>: Protocol<'d> + Sized {
    type View<'step>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(protocol: dispatch::raw::Loan<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step;
}
