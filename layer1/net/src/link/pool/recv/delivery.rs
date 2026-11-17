use std::marker;

use dope_core::{
    driver::{
        route::{Identity, KeyTag, kind},
        schedule::{app::batch::Bound, ready},
    },
    io::recv::Lease,
};
use o3::mem;
use receive::{batch, mode};
use slot::recv::Prepared;
use storage::queue::Write;

use crate::{
    link::{
        event,
        pool::{
            self,
            recv::{self, input, storage},
        },
        slot,
    },
    wire::{
        self, receive,
        receive::{Chunk, mode::Disabled},
    },
};

/// One admitted receive event and its inseparable connection transaction.
pub struct Data<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w + 'd, M: input::Kind<W>, R = ()>
where
    'd: 'w,
{
    prepared: Prepared<'a, 'w, 'd, ID, W, M::Ingress>,
    rearm: recv::Rearm<'a, 'd, ID>,
    bytes: Lease<'d>,
    receipt: R,
    input: marker::PhantomData<M>,
}

#[must_use]
pub struct Resumed<'d, ID: Identity> {
    key: pool::Key<'d, ID>,
    ready: ready::Key<'d>,
}

impl<'d, ID: Identity> Resumed<'d, ID> {
    pub(in crate::link::pool) const fn new(key: pool::Key<'d, ID>, ready: ready::Key<'d>) -> Self {
        Self { key, ready }
    }

    pub const fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub const fn ready(&self) -> ready::Key<'d> {
        self.ready
    }
}

pub trait BorrowedData {}

impl BorrowedData for input::Borrowed {}
impl BorrowedData for input::Coupled {}

pub trait ScopedData {}

impl ScopedData for input::Scoped {}

pub trait Ownership<W: input::Execution>: mode::Credit {
    fn own<'a, 'w, 'd, ID: Identity + 'w, R>(
        data: Data<'a, 'w, 'd, ID, W, input::Retained, R>,
    ) -> event::DispatchRecv<'d, ID, (Chunk<'w, 'd, W>, R)>
    where
        'd: 'w,
        W: 'w + 'd;
}

impl<W> Ownership<W> for mode::Disabled
where
    W: input::Execution,
    W::Receive: receive::Policy<W, Credit = Self>,
{
    fn own<'a, 'w, 'd, ID: Identity + 'w, R>(
        data: Data<'a, 'w, 'd, ID, W, input::Retained, R>,
    ) -> event::DispatchRecv<'d, ID, (Chunk<'w, 'd, W>, R)>
    where
        'd: 'w,
        W: 'w + 'd,
    {
        Data::<ID, W, input::Retained, R>::scoped_with(
            data.prepared,
            data.rearm,
            data.bytes,
            |chunk| (chunk, data.receipt),
        )
    }
}

impl<W> Ownership<W> for mode::Enabled
where
    W: input::Execution,
    W::Receive: receive::Policy<W, Credit = Self>,
    W::Retained: mode::Stored,
{
    fn own<'a, 'w, 'd, ID: Identity + 'w, R>(
        data: Data<'a, 'w, 'd, ID, W, input::Retained, R>,
    ) -> event::DispatchRecv<'d, ID, (Chunk<'w, 'd, W>, R)>
    where
        'd: 'w,
        W: 'w + 'd,
    {
        let Data {
            mut prepared,
            mut rearm,
            mut bytes,
            receipt,
            input: marker::PhantomData,
        } = data;
        let key = rearm.key();
        let (remaining, finished) = prepared.discard(&mut bytes);
        if remaining.is_zero() {
            let control = slot::Control::Discarded {
                needs_rearm: prepared.needs_rearm,
                finished,
            };
            drop(bytes);
            drop(prepared);
            return event::DispatchRecv::Control(
                Data::<ID, W, input::Retained, R>::control(rearm, control),
            );
        }
        let credit = match <W::Retained as mode::Stored>::arm(
            prepared.credit,
            key.target().dispatch(),
            prepared.engine.ready_handle().target(),
        ) {
            Ok(credit) => credit,
            Err(cause) => {
                let control = slot::Control::Credit {
                    cause,
                    needs_rearm: prepared.needs_rearm,
                };
                drop(bytes);
                drop(prepared);
                return event::DispatchRecv::Control(
                    Data::<ID, W, input::Retained, R>::control(rearm, control),
                );
            }
        };
        let read = <W::Receive as receive::Policy<W>>::read_lease::<ID>(
            prepared.transaction,
            bytes,
            prepared.end,
        )
        .map(|cursor| cursor.map(Chunk::from_raw));
        let (item, captured) = read.into_parts();
        Prepared::<ID, W, W::Retained>::capture(
            prepared.engine,
            &mut prepared.needs_rearm,
            captured,
        );
        match item {
            Some(cursor) => {
                prepared.engine.operations.recv.pause();
                prepared.needs_rearm |= prepared.engine.operations.recv.has_cancel();
                let chunk = cursor.retain(credit.claim().into_return());
                if prepared.needs_rearm {
                    rearm.queue();
                }
                event::DispatchRecv::Chunk(key, (chunk, receipt))
            }
            None => {
                credit.cancel();
                if prepared.needs_rearm {
                    rearm.queue();
                }
                event::DispatchRecv::Control(event::ControlRecv::NoChunk(key))
            }
        }
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, M, R> Data<'a, 'w, 'd, ID, W, M, R>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Kind<W>,
{
    pub(super) fn new(
        prep: Prepared<'a, 'w, 'd, ID, W, M::Ingress>,
        rearm: recv::Rearm<'a, 'd, ID>,
        event: event::Data<'d, R>,
    ) -> Self {
        let (bytes, receipt) = event.into_lease();
        Self {
            prepared: prep,
            rearm,
            bytes,
            receipt,
            input: marker::PhantomData,
        }
    }

    fn scoped_with<C>(
        mut prepared: Prepared<'a, 'w, 'd, ID, W, M::Ingress>,
        mut rearm: recv::Rearm<'_, 'd, ID>,
        mut bytes: Lease<'d>,
        pack: impl FnOnce(Chunk<'w, 'd, W, Disabled>) -> C,
    ) -> event::DispatchRecv<'d, ID, C> {
        let key = rearm.key();
        let (remaining, finished) = prepared.discard(&mut bytes);
        if remaining.is_zero() {
            let control = slot::Control::Discarded {
                needs_rearm: prepared.needs_rearm,
                finished,
            };
            drop(bytes);
            drop(prepared);
            return event::DispatchRecv::Control(Self::control(rearm, control));
        }
        let read = <W::Receive as receive::Policy<W>>::read_lease::<ID>(
            prepared.transaction,
            bytes,
            prepared.end,
        )
        .map(|cursor| cursor.map(Chunk::from_raw));
        let (item, captured) = read.into_parts();
        Prepared::<ID, W, M::Ingress>::capture(
            prepared.engine,
            &mut prepared.needs_rearm,
            captured,
        );
        if prepared.needs_rearm {
            rearm.queue();
        }
        match item {
            Some(chunk) => event::DispatchRecv::Chunk(key, pack(chunk)),
            None => event::DispatchRecv::Control(event::ControlRecv::NoChunk(key)),
        }
    }

    pub(in crate::link::pool::recv) fn control(
        mut rearm: recv::Rearm<'_, 'd, ID>,
        ctl: slot::Control,
    ) -> event::ControlRecv<'d, ID> {
        let key = rearm.key();
        match ctl {
            slot::Control::Eof => event::ControlRecv::Eof(key),
            slot::Control::Cancelled => event::ControlRecv::Cancelled(key),
            slot::Control::Failed(error) => event::ControlRecv::Failed { key, error },
            slot::Control::Credit {
                cause,
                needs_rearm: arm,
            } => {
                if arm {
                    rearm.queue();
                }
                event::ControlRecv::Credit { key, cause }
            }
            slot::Control::Discarded {
                needs_rearm: arm,
                finished,
            } => {
                if arm {
                    rearm.queue();
                }
                event::ControlRecv::Discarded { key, finished }
            }
            slot::Control::NoChunk { needs_rearm: arm } => {
                if arm {
                    rearm.queue();
                }
                event::ControlRecv::NoChunk(key)
            }
        }
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, M, R> Data<'a, 'w, 'd, ID, W, M, R>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Kind<W> + BorrowedData,
{
    /// Moves the admitted event into caller-owned backing before borrowing its
    /// bytes. The returned batch retains that backing, not the pool loan.
    pub fn dispatch<'bytes, 'id>(
        self,
        backing: &'bytes mut Option<event::Data<'d, R>>,
        cap: Bound<'id, batch::Capacity<'d, W>>,
    ) -> (
        event::DispatchRecv<'d, ID, batch::Items<'bytes, W>>,
        mem::raw::Unused<'id>,
    )
    where
        'w: 'bytes,
    {
        let Self {
            mut prepared,
            mut rearm,
            bytes,
            receipt,
            input: marker::PhantomData,
        } = self;
        let key = rearm.key();
        let token = key.target().operation::<kind::Recv>().erase_route();
        let event = backing.insert(event::Data::new(token, bytes).with_receipt(receipt));
        let (remaining, finished) = prepared.discard(event.lease_mut());
        if remaining.is_zero() {
            let unused = cap.empty();
            let control = slot::Control::Discarded {
                needs_rearm: prepared.needs_rearm,
                finished,
            };
            drop(prepared);
            return (
                event::DispatchRecv::Control(Self::control(rearm, control)),
                unused,
            );
        }
        let read = <W::Receive as receive::Policy<W>>::process::<ID>(
            prepared.transaction,
            event.lease_mut().as_mut_slice(),
            batch::Grant::new(cap),
            prepared.end,
        );
        let (produced, captured) = read.into_parts();
        let (chunk, unused) = produced.into_parts();
        Prepared::<ID, W, M::Ingress>::capture(
            prepared.engine,
            &mut prepared.needs_rearm,
            captured,
        );
        let next = if receive::raw::Batch::is_empty(&chunk) {
            drop(chunk);
            event::DispatchRecv::Control(Self::control(
                rearm,
                slot::Control::NoChunk {
                    needs_rearm: prepared.needs_rearm,
                },
            ))
        } else {
            if prepared.needs_rearm {
                rearm.queue();
            }
            event::DispatchRecv::Chunk(key, batch::Items::from_raw(chunk))
        };
        (next, unused)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, M, R> Data<'a, 'w, 'd, ID, W, M, R>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Kind<W> + ScopedData,
{
    pub fn scoped(self) -> event::DispatchRecv<'d, ID, Chunk<'w, 'd, W, Disabled>> {
        drop(self.receipt);
        Self::scoped_with(self.prepared, self.rearm, self.bytes, std::convert::identity)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, R> Data<'a, 'w, 'd, ID, W, input::Retained, R>
where
    'd: 'w,
    W: input::Execution + 'w + 'd,
    <W::Receive as receive::Policy<W>>::Credit: Ownership<W>,
{
    pub fn retained(self) -> event::DispatchRecv<'d, ID, (Chunk<'w, 'd, W>, R)> {
        <<W::Receive as receive::Policy<W>>::Credit as Ownership<W>>::own(self)
    }
}

pub(super) fn defer_completion<'a, 'd: 'a, ID: Identity, P: storage::raw::Mode, R: 'a>(
    mut store: P::Bound<'a, 'd, R>,
    mut rearm: recv::Rearm<'_, 'd, ID>,
    key: &pool::Key<'d, ID>,
    cancel: bool,
    done: event::Data<'d, R>,
) -> event::ParkRecv<'d, ID>
where
    P::Bound<'a, 'd, R>: Write<'d, R>,
{
    let key = *key;
    match store.prepare_push() {
        Some(vacant) => {
            vacant.append(done.complete());
            if cancel {
                rearm.queue();
            }
            event::ParkRecv::Deferred
        }
        None => {
            drop(done);
            event::ParkRecv::Failed {
                key,
                error: event::ParkError::Full,
            }
        }
    }
}

const _: () = assert!(
    size_of::<Resumed<'static, KeyTag<0>>>()
        == size_of::<(pool::Key<'static, KeyTag<0>>, ready::Key<'static>)>()
);
