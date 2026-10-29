use std::os::fd;

use o3::mem::Rejected;

use crate::{
    Errno,
    io::reactor::ready::delivery::entry::{
        Accepted, Bytes, Context, Flow, Key, Retired, Slots, Unit,
    },
    table,
    table::token::host,
};

/// Affine host effects used while a native readiness event is consumed.
pub trait Effects<'q, 'd, B>: Context + Sized {
    type Files: table::Files<Slots<Self>, Handle = Self::Handle>;
    type Bound: host::Flight<Flow<Self>, Key = Key<Self>>;
    type Drain: host::Drain<Key = Key<Self>, Flight = Self::Bound, Driver = Self::Driver>;
    type Driver: Copy;
    type Output;
    type Error;
    type Mismatch;
    type Socket: TryFrom<Self::Bound, Error = Self::Mismatch>
        + host::Flight<Flow<Self>, Key = Key<Self>>;
    type Open: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Accept: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Recv: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Send: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Connect: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Write: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Sync: TryFrom<Self::Bound, Error = Self::Mismatch>;
    type Target: Copy + for<'a> From<&'a Self::Socket>;
    type Creation: AsRef<Self::Outbound>;

    const SHUTDOWN: Self::Output;

    fn accepted(bound: Self::Accept, accepted: Accepted<Self>, flow: Flow<Self>) -> Self::Output;
    fn accept_error(bound: Self::Accept, failure: Self::Failure, flow: Flow<Self>) -> Self::Output;
    fn received(
        bound: Self::Bound,
        driver: Self::Driver,
        key: Key<Self>,
        flow: Flow<Self>,
        buffer: B,
    ) -> Result<Self::Output, Rejected<B, Self::Error>>;
    fn control(bound: Self::Recv, result: Unit<Self>, flow: Flow<Self>) -> Self::Output;
    fn sent(bound: Self::Send, result: Bytes<Self>) -> Self::Output;
    fn connected(bound: Self::Connect, result: Unit<Self>) -> Self::Output;
    fn opened(bound: Self::Open, result: Result<fd::OwnedFd, Errno>) -> Self::Output;
    fn written(bound: Self::Write, result: Bytes<Self>) -> Self::Output;
    fn synced(bound: Self::Sync, result: Unit<Self>) -> Self::Output;
    fn mismatched(key: Key<Self>, cause: Self::Mismatch) -> Self::Error;
    fn abandon(driver: Self::Driver, accepted: Accepted<Self>) -> Self::Reclaimed;
    fn report_recv(key: Key<Self>, flow: Flow<Self>, buffer: &B) -> Retired<Self>;
}
