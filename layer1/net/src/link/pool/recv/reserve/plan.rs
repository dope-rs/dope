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
        receive::{self, mode, reserve},
    },
};

pub(super) enum Queued<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w + 'd, S, M: input::Mode<W>>
where
    'd: 'w,
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
{
    rt: &'a mut W::Runtime<'w, 'd, ID>,
    store: <M::Ingress as storage::raw::Mode>::Bound<'a, 'd>,
    rearm: recv::Rearm<'a, 'd, ID>,
    connection: &'a mut types::Connection<'w, 'd, ID, W, S>,
    event: event::Data<'d>,
    status: Status,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M> Plan<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
{
    pub(super) fn observe<T, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        item: A,
    ) -> Result<Self, super::Reservation<'a, 'w, 'd, ID, W, M>>
    where
        T: crate::Transport,
        D: demand::Mode,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        let (event, timing) = item.observe().into_parts();
        let target = event.token();
        let key = match pool.keys.parse(target) {
            Ok(key) => key,
            Err(_) => return Err(event::Reservation::Drop),
        };
        let slab = &mut pool.prepared.slab;
        let rt = &mut pool.prepared.runtime;
        let (slot, recv) = match slab.get(key.parts()) {
            Some(bound) => bound,
            None => return Err(event::Reservation::Drop),
        };
        let close = slot.is_recv_done();
        let (arm, origin) = match timing {
            arrival::Timing::Fresh(more) => {
                let seen = match slot.engine.operations.observe_recv(more, close) {
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
            arrival::Timing::Replay => (
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
        Ok(Self {
            rt,
            store: recv.storage,
            rearm: recv.rearm,
            connection: slot,
            event,
            status,
        })
    }

    pub(super) fn immediate(
        self,
        credit: <M::Ingress as mode::Flag>::Binding<'d, ID>,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>>
    where
        <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
    {
        match Prepared::<ID, W, M::Ingress>::new(
            self.connection,
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
    <M::Ingress as storage::raw::Mode>::Bound<'a, 'd>: Write<'d>,
{
    pub(super) fn queue(self) -> Queued<'a, 'w, 'd, ID, W, S, M> {
        let resident = self.connection.resident();
        let trails = match self.status.origin {
            arrival::Origin::Fresh => self.store.has_queued(),
            arrival::Origin::Replay => false,
        };
        if !self.status.paused && !trails {
            return Queued::Ready(self);
        }
        let parked = delivery::defer_completion::<ID, M::Ingress>(
            self.store,
            self.rearm,
            resident,
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
{
    pub(super) fn credit(&self) -> credit::Binding<'d, ID> {
        let target = self.connection.resident().key().target().dispatch();
        let control = self.store.get();
        control.control().bind(target)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M, R> Plan<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W, Ingress = mode::Waiting<R>>,
    R: reserve::Try<W>,
    W::Receive: receive::Policy<W, Reserve = reserve::Waiting<R>>,
{
    pub(super) fn try_reserve(
        self,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>> {
        let resident = self.connection.resident();
        let key = resident.key();
        let ctl = self.store.get();
        match Reserve::<ID, W, mode::Waiting<R>, R>::try_new(
            self.connection,
            self.rt,
            ctl.control(),
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
                let parked = delivery::Park::<ID, W, mode::Waiting<R>, R>::new(
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
