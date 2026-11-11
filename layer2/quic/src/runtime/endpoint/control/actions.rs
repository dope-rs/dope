use std::{pin, time};

use o3::buffer::{self, resident};

use dope::core::driver::schedule::clock;

use crate::conn::{id, scope};
use crate::runtime::endpoint::{self, owner};
use crate::runtime::receive::Storage;
use crate::{errors, proto::datagram, runtime::mux};
use control::Control as _;
use dgram::Dgram as _;
use mux::{handler, protocol};
use protocol::{control, dgram};

/// Splits the callback owner from the native registry that resumes its input.
pub(crate) fn split_ready<'a, 'd, const ID: u8, H, B, A, M, Q>(
    ep: pin::Pin<&'a mut endpoint::Endpoint<'d, ID, H, B, A, M, Q>>,
    now: time::Instant,
) -> (
    pin::Pin<&'a mut H>,
    impl FnMut(id::Handle<id::Driver<'d>, ID>) -> Result<(), errors::Access> + 'a,
)
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
    Q: buffer::Residency + buffer::raw::Source,
{
    let mux = ep.project().udp.handler_mut().mux_mut().project();
    (mux.handler, move |handle| mux.state.resume(handle, now))
}

pub(crate) fn next_app<'d, const ID: u8, H, B, M, Q, T, const PLANE: usize>(
    ep: pin::Pin<&mut endpoint::Endpoint<'d, ID, H, B, endpoint::Idle, M, Q>>,
    now: time::Instant,
    op: impl for<'handler, 'scope> FnOnce(
        pin::Pin<&'handler mut H>,
        mux::app::Connection<'scope, H::Connection, ID, B, resident::Shared<'d>, id::Driver<'d>>,
    ) -> T,
) -> Option<T>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
    Q: buffer::Residency + buffer::raw::Source,
{
    mux::app::next::<_, _, ID, _, _, PLANE>(ep.project().udp.handler_mut().mux_mut(), now, op)
}

pub(crate) fn with_connection<
    'd,
    const ID: u8,
    H,
    B,
    A,
    M,
    R,
    Q: buffer::Residency + buffer::raw::Source,
>(
    ep: pin::Pin<&mut endpoint::Endpoint<'d, ID, H, B, A, M, Q>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    now: clock::Moment<'d>,
    op: impl for<'scope> FnOnce(scope::Temporal<'scope, 'd, ID, B, resident::Shared<'d>>) -> R,
) -> Result<R, errors::Access>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
{
    now.at(ep).apply(|ep, raw| {
        protocol::Io::new(ep.project().udp.handler_mut().mux_mut()).with(handle, raw, |scope| {
            op(scope::Temporal::new(scope.into_tracked(), now))
        })
    })
}

pub(crate) fn try_send_datagram<
    'd,
    const ID: u8,
    H,
    B,
    A,
    M,
    Q: buffer::Residency + buffer::raw::Source,
>(
    ep: pin::Pin<&mut endpoint::Endpoint<'d, ID, H, B, A, M, Q>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    data: Vec<u8>,
    now: time::Instant,
) -> Result<(), errors::SendFailure<Vec<u8>>>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
{
    protocol::Io::new(ep.project().udp.handler_mut().mux_mut()).try_send(handle, data, now)
}

pub(crate) fn try_sender<'a, 'pool, 'd, const ID: u8, H, B, A, M>(
    ep: pin::Pin<&'a mut endpoint::Endpoint<'d, ID, H, B, A, M, datagram::Sealed<'pool>>>,
    now: time::Instant,
) -> Result<super::Sender<'a, 'd, ID, H, B, M, datagram::Sealed<'pool>>, super::SendError>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
{
    ep.project().udp.handler_mut().try_sender::<M>(now)
}

pub(crate) fn close<'d, const ID: u8, H, B, A, M, Q: buffer::Residency + buffer::raw::Source>(
    ep: pin::Pin<&mut endpoint::Endpoint<'d, ID, H, B, A, M, Q>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    now: time::Instant,
) where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
{
    protocol::Io::new(ep.project().udp.handler_mut().mux_mut()).close_at(handle, now)
}
