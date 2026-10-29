//! Closing transitions for occupied connection slots.
use std::mem;

use dope_core::driver::{
    receive::Registration,
    retained::{self, flight},
    route::{Identity, KeyTag},
    schedule::ready,
};

use crate::{
    link::{
        egress::{self, data, reserve::demand},
        pool::recv::storage::tail::Slot,
        slot::{transmit::outcome, types},
    },
    wire,
};

#[must_use]
pub enum Decision<'d, ID: Identity> {
    Ready,
    Runnable(Resume<'d>),
    External(outcome::Wait<'d, ID>),
    Stale,
}

#[must_use = "close progress must schedule its exact ready owner"]
pub struct Resume<'d>(ready::Target<'d>);

impl<'d> Resume<'d> {
    pub(in crate::link::pool::change) fn new<'w, ID: Identity + 'w, W: wire::Spec, S>(
        slot: &types::Connection<'w, 'd, ID, W, S>,
    ) -> Self {
        Self(slot.io().wake_target())
    }

    pub(in crate::link::pool::change) fn from_target(target: ready::Target<'d>) -> Self {
        Self(target)
    }

    pub fn reschedule(self) {
        self.0.wake();
    }
}

impl<'d, ID: Identity> Decision<'d, ID> {
    pub fn runnable<'w, W: wire::Spec, S>(slot: &types::Connection<'w, 'd, ID, W, S>) -> Self
    where
        ID: 'w,
    {
        Self::Runnable(Resume::new(slot))
    }
}

pub enum Approval<S, R> {
    Ready(R),
    Pending(S),
    Stale(S),
}

pub enum Kind {
    Opening,
    Connected,
}

pub(in crate::link::pool::change) enum Start<T> {
    Ready(T),
    Terminal,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, Q>
    Start<Slot<'a, types::Connection<'w, 'd, ID, W, S>, ID, Q>>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
{
    pub(in crate::link::pool::change) fn begin(
        mut entry: Slot<'a, types::Connection<'w, 'd, ID, W, S>, ID, Q>,
    ) -> Self {
        entry.get_mut().engine.operations.lifecycle.close();
        if entry.get().terminal.is_pending() {
            return Self::Terminal;
        }
        let ready = entry.get().engine.ready_handle().target();
        Registration::clear(ready);
        entry.get_mut().engine.operations.recv.exhaustion().cancel();
        Self::Ready(entry)
    }
}

#[derive(Clone, Copy)]
pub enum Failure {
    Receive(dope_core::Errno),
}

/// Retains the exact failure with its connection and pool output loans.
pub struct Io<
    'a,
    'p,
    'w,
    'd: 'w,
    ID: Identity + 'w,
    W: wire::Spec,
    S,
    B,
    const IOV: usize,
    D: demand::Mode,
> {
    pub(in crate::link::pool::change) cause: Failure,
    pub(in crate::link::pool::change) connection: &'a mut types::Connection<'w, 'd, ID, W, S>,
    pub(in crate::link::pool::change) egress: &'a mut egress::Storage<'p, 'd, ID, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec, S, B, const IOV: usize, D: demand::Mode>
    Io<'a, 'p, 'w, 'd, ID, W, S, B, IOV, D>
{
    pub fn failure(&self) -> Failure {
        self.cause
    }

    pub fn connection(self) -> &'a mut types::Connection<'w, 'd, ID, W, S> {
        self.connection
    }

    pub fn into_parts(
        self,
    ) -> (
        &'a mut types::Connection<'w, 'd, ID, W, S>,
        egress::Queue<'a, 'p, 'd, IOV, B, D>,
    )
    where
        B: data::raw::Payload<'d>,
    {
        let queue = self.egress.bind_queue(self.connection.key());
        (self.connection, queue.reborrow())
    }
}

#[must_use = "deferred close evidence must reach its pending owner"]
pub enum Deferred<'d, ID: Identity> {
    Terminal,
    Setup,
    Send,
    Recv,
    Ingress,
    Owner(outcome::Wait<'d, ID>),
    Egress,
    Retire(Resume<'d>),
    Scheduled,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Prepare,
    Release,
}

const _: () = {
    assert!(mem::size_of::<Phase>() == 1);
    assert!(mem::size_of::<Failure>() == mem::size_of::<dope_core::Errno>());
    let limit = mem::size_of::<(usize, usize, dope_core::Errno)>();
    assert!(
        mem::size_of::<
            Io<
                'static,
                'static,
                'static,
                'static,
                KeyTag<0>,
                wire::Identity,
                (),
                (),
                1,
                demand::Fallible,
            >,
        >() <= limit
    );
    assert!(
        mem::size_of::<
            Option<
                Io<
                    'static,
                    'static,
                    'static,
                    'static,
                    KeyTag<0>,
                    wire::Identity,
                    (),
                    (),
                    1,
                    demand::Fallible,
                >,
            >,
        >() <= limit
    );
};

/// Owner transition with independently borrowed application, claim, slot and driver.
pub type Owner<'ctx, 'claim, 'flights, 'slot, 'driver, 'runtime, 'region, 'w, 'd, ID, W, S, C, X> = (
    &'ctx mut X,
    &'claim C,
    Phase,
    &'flights flight::Slots<'d, ID>,
    &'slot mut types::Connection<'w, 'd, ID, W, S>,
    &'driver mut retained::Context<'runtime, 'region, 'd>,
);

/// Commit transition retaining its claim and returned connection state.
pub type Commit<'ctx, 'claim, 'driver, 'runtime, 'region, 'd, C, X, S> = (
    &'ctx mut X,
    &'claim C,
    S,
    Kind,
    &'driver mut retained::Context<'runtime, 'region, 'd>,
);

/// Failed transition retaining its exact connection and output loans.
pub type Failed<
    'ctx,
    'claim,
    'io,
    'driver,
    'runtime,
    'region,
    'p,
    'w,
    'd,
    ID,
    W,
    S,
    B,
    const IOV: usize,
    D,
    C,
    X,
> = (
    &'ctx mut X,
    &'claim C,
    Io<'io, 'p, 'w, 'd, ID, W, S, B, IOV, D>,
    &'driver mut retained::Context<'runtime, 'region, 'd>,
);

/// One application's authority and callbacks for an occupied close transition.
pub struct Handler<'a, C, X, O, K, F> {
    pub claim: C,
    pub context: &'a mut X,
    pub owner: O,
    pub commit: K,
    pub failed: F,
}

/// Exact authority returned by terminal close dispatch.
pub enum Outcome<R, C, D> {
    Retired(R),
    Pending(C, D),
    Stale(C),
    Missing(C),
}
