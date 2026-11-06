use dope_core::driver::{receive::credit, route::Identity};
use slot::recv::{Prepared, Reserve};
use storage::queue::{Ops, Write};

use crate::{
    link::{
        egress::reserve::demand,
        event,
        pool::{
            self,
            recv::{self, arrival, delivery, input, storage},
        },
        slot::{self, types},
    },
    wire::{
        self,
        receive::{self, mode, publication::Policy as _, reserve},
    },
};

pub(super) enum Queued<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w + 'd, S, M: input::Mode<W>>
where
    'd: 'w,
    input::Receipt<W, M>: 'a,
{
    Parked(event::ParkRecv<'d, ID>),
    Ready(Plan<'a, 'w, 'd, ID, W, S, M>),
}

struct Status {
    rearm: bool,
    cancel: bool,
    origin: arrival::Origin,
    paused: bool,
}

pub(super) struct Plan<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w + 'd, S, M: input::Mode<W>>
where
    'd: 'w,
    input::Receipt<W, M>: 'a,
{
    rt: &'a mut W::Runtime<'w, 'd, ID>,
    store: <M::Ingress as storage::raw::Mode>::Bound<'a, 'd, input::Receipt<W, M>>,
    rearm: recv::Rearm<'a, 'd, ID>,
    connection: &'a mut types::Connection<'w, 'd, ID, W, S>,
    event: event::Data<'d, input::Receipt<W, M>>,
    status: Status,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M> Plan<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    input::Receipt<W, M>: 'a,
{
    pub(super) fn observe<T, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        item: A,
        capture: impl FnOnce(pool::Key<'d, ID>, &S) -> M::Receipt,
    ) -> Result<Self, super::Reservation<'a, 'w, 'd, ID, W, M>>
    where
        T: crate::Transport,
        D: demand::Mode,
        A: arrival::Arrival<'d, input::Receipt<W, M>, Observed = event::Data<'d>>,
    {
        let (event, timing) = item.observe().into_parts();
        let target = event.token();
        let key = match pool.keys.parse(target) {
            Ok(key) => key,
            Err(_) => return Err(event::Reservation::Drop),
        };
        let slab = &mut pool.prepared.slab;
        let rt = &mut pool.prepared.runtime;
        let (slot, recv) = match slab.get_mut(key.parts()) {
            Some(bound) => bound,
            None => return Err(event::Reservation::Drop),
        };
        let close = slot.is_recv_done();
        let (arm, origin) = match &timing {
            arrival::Timing::Fresh(more) => {
                let seen = match slot.engine.operations.observe_recv(target, *more, close) {
                    Some(seen) => seen,
                    None => return Err(event::Reservation::Drop),
                };
                let arm = match seen.is_probe() {
                    true => {
                        if let Err(stale) = slot.engine.settle_probe() {
                            return Err(event::Reservation::Stale(stale));
                        }
                        slot.engine.operations.recv.is_armable(close)
                    }
                    false => seen.rearm().is_needed(),
                };
                (arm, arrival::Origin::Fresh)
            }
            arrival::Timing::Replay(_) => (
                slot.engine.operations.recv.is_armable(close),
                arrival::Origin::Replay,
            ),
        };
        if close {
            return Err(
                if slot.is_closing() && !slot.engine.operations.recv.is_armed() {
                    event::Reservation::Parked(event::ParkRecv::Closing(key.connection()))
                } else {
                    event::Reservation::Drop
                },
            );
        }
        let status = Status {
            rearm: arm,
            cancel: slot.engine.operations.recv.has_cancel(),
            origin,
            paused: slot.engine.operations.recv.is_paused(),
        };
        let receipt = match timing {
            arrival::Timing::Fresh(_) => {
                <W::Receive as receive::Policy<W>>::Publication::observe(|| {
                    capture(key.connection(), &slot.state)
                })
            }
            arrival::Timing::Replay(receipt) => receipt,
        };
        Ok(Self {
            rt,
            store: recv.storage,
            rearm: recv.rearm,
            connection: slot,
            event: event.with_receipt(receipt),
            status,
        })
    }

    pub(super) fn immediate(
        self,
        credit: <M::Ingress as mode::Flag>::Binding<'a, 'd, ID>,
    ) -> super::Reservation<'a, 'w, 'd, ID, W, M>
    where
        <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
    {
        match Prepared::<ID, W, M::Ingress>::new(
            &mut self.connection.control,
            self.rt,
            credit,
            self.status.rearm,
        ) {
            Ok(prep) => {
                event::Reservation::Ready(delivery::Data::new(prep, self.rearm, self.event))
            }
            Err(control) => {
                drop(self.event);
                event::Reservation::Control(delivery::Data::<ID, W, M::Kind>::control(
                    self.rearm, control,
                ))
            }
        }
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M> Plan<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    input::Receipt<W, M>: 'a,
    <M::Ingress as storage::raw::Mode>::Bound<'a, 'd, input::Receipt<W, M>>:
        Write<'d, input::Receipt<W, M>>,
{
    pub(super) fn queue(self) -> Queued<'a, 'w, 'd, ID, W, S, M> {
        let key = &self.connection.key;
        let trails = match self.status.origin {
            arrival::Origin::Fresh => self.store.has_queued(),
            arrival::Origin::Replay => false,
        };
        if !self.status.paused && !trails {
            return Queued::Ready(self);
        }
        let parked = delivery::defer_completion::<ID, M::Ingress, _>(
            self.store,
            self.rearm,
            key,
            self.status.cancel,
            self.event,
        );
        Queued::Parked(parked)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M> Plan<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W, Ingress = mode::Enabled>,
    input::Receipt<W, M>: 'a,
{
    pub(super) fn credit(&self) -> credit::Slot<'a, 'd> {
        self.store.get()
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M, R> Plan<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W, Ingress = mode::Waiting<R>>,
    input::Receipt<W, M>: 'a,
    R: reserve::Try<W>,
    W::Receive: receive::Policy<W, Reserve = reserve::Waiting<R>>,
{
    pub(super) fn try_reserve(self) -> super::Reservation<'a, 'w, 'd, ID, W, M> {
        let key = self.connection.key;
        match Reserve::<ID, W, mode::Waiting<R>, R>::try_new(
            &mut self.connection.control,
            self.rt,
            self.store.get(),
            self.status.rearm,
        ) {
            Reserve::Ready(prep) => {
                event::Reservation::Ready(delivery::Data::new(prep, self.rearm, self.event))
            }
            Reserve::Control(control) => {
                drop(self.event);
                event::Reservation::Control(delivery::Data::<ID, W, M::Kind>::control(
                    self.rearm, control,
                ))
            }
            Reserve::Blocked(wait) => {
                let parked = delivery::Park::<ID, W, mode::Waiting<R>, R, _>::new(
                    self.store, self.rearm, wait, self.event,
                )
                .park();
                event::Reservation::Parked(parked)
            }
            Reserve::Credit(cause) => event::Reservation::Parked(event::ParkRecv::Failed {
                key,
                error: event::ParkError::Credit(cause),
            }),
        }
    }
}
