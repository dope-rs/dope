use crate::runtime::mux::handler::hook::Boundary;
use o3::buffer;
use o3::buffer::hold::evidence;
use std::pin;

use crate::conn::{id, scope};
use crate::runtime::mux::handler::hook::set;
use crate::runtime::mux::{app, handler::Handler};
use crate::stream::{self, release, send::admit};

pub trait Mode<H, const DOMAIN: u8, B, D, I: id::Temporal>: Boundary {
    type Evidence: release::Mode;

    /// Returns application send owners while packet protection remains retained.
    fn cancel(
        handler: pin::Pin<&mut H>,
        conn: scope::Access<'_, DOMAIN, B, false, D>,
        handle: id::Handle<I, DOMAIN>,
    ) where
        B: stream::Recv,
        D: buffer::raw::Segment;

    fn release(
        handler: pin::Pin<&mut H>,
        handle: id::Handle<I, DOMAIN>,
        proof: <Self::Evidence as evidence::Mode>::Projection,
        source: admit::Source,
        cause: release::Cause,
    ) -> app::Readiness;
}

impl<H, const DOMAIN: u8, B, D, I: id::Temporal> Mode<H, DOMAIN, B, D, I> for set::Off {
    type Evidence = evidence::Off;

    fn cancel(
        _: pin::Pin<&mut H>,
        mut conn: scope::Access<'_, DOMAIN, B, false, D>,
        _: id::Handle<I, DOMAIN>,
    ) where
        B: stream::Recv,
        D: buffer::raw::Segment,
    {
        conn.release(&mut release::Ignore);
    }

    fn release(
        _: pin::Pin<&mut H>,
        _: id::Handle<I, DOMAIN>,
        proof: evidence::Off,
        _: admit::Source,
        _: release::Cause,
    ) -> app::Readiness {
        let evidence::Off = proof;
        app::Readiness::Preserve
    }
}

impl<H, const DOMAIN: u8, B, D, I> Mode<H, DOMAIN, B, D, I> for set::On
where
    H: Sink<DOMAIN, B, D, I>,
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    type Evidence = evidence::On;

    fn cancel(
        mut handler: pin::Pin<&mut H>,
        mut conn: scope::Access<'_, DOMAIN, B, false, D>,
        handle: id::Handle<I, DOMAIN>,
    ) {
        conn.release(&mut |release| match H::release(Event::new((
            handler.as_mut(),
            handle,
            release,
        ))) {
            app::Readiness::Preserve | app::Readiness::Idle | app::Readiness::Ready => (),
        });
    }

    fn release(
        handler: pin::Pin<&mut H>,
        handle: id::Handle<I, DOMAIN>,
        proof: <evidence::On as evidence::Mode>::Projection,
        source: admit::Source,
        cause: release::Cause,
    ) -> app::Readiness {
        H::release(Event::new((
            handler,
            handle,
            release::Event {
                source,
                charge: proof,
                cause,
            },
        )))
    }
}

pub type Event<'scope, H, const DOMAIN: u8, I> = crate::runtime::mux::handler::hook::Event<
    (
        pin::Pin<&'scope mut H>,
        id::Handle<I, DOMAIN>,
        release::Event,
    ),
    8,
>;

pub trait Sink<const DOMAIN: u8, B, D, I>: Handler<DOMAIN, B, D, I>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn release<'scope>(event: Event<'scope, Self, DOMAIN, I>) -> app::Readiness;
}
