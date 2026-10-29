use dope_core::driver::{route::table::bounds, schedule::clock};
use dope_net::link::oneshot;
use o3::{base::time::Positive, depot::arena::slab::external::errors};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Stale,
    Identity(errors::Exhausted),
    Route(bounds::Excluded),
    ConnectDeadline(clock::Range<Positive>),
    Delivery(oneshot::Error),
    IntakeClosed,
    RuntimeShutdown,
    ForcedShutdown,
}
