use std::{marker, ops::ControlFlow, task::Poll};

use dope_core::{
    driver::{
        receive::{Absent, Registration, Stale},
        route::{Identity, kind},
    },
    io::event::net::recv::{ControlKind, Status},
};
use storage::queue::{Ops, Push, Write};

use crate::{
    link::{
        egress::reserve::demand,
        event,
        pool::{
            self,
            recv::{self, arrival, delivery, input, storage},
        },
        slot::{self, recv::Capture, types},
    },
    wire::{self, receive, receive::mode},
};

struct Flags {
    arm: bool,
    end: bool,
    close: bool,
    paused: bool,
    origin: arrival::Origin,
}

enum Rejected {
    Retired,
    Stale(Stale),
    Observed(slot::Control),
}

pub struct Observed<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w + 'd, S, M: input::Mode<W>>
where
    'd: 'w,
{
    slot: &'a mut types::Connection<'w, 'd, ID, W, S>,
    rearm: recv::Rearm<'a, 'd, ID>,
    key: pool::Key<'d, ID, kind::Recv>,
    item: event::Control<'d>,
    status: Flags,
    input: marker::PhantomData<M>,
}

pub(in crate::link::pool::recv) enum Observation<
    'a,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
> where
    'd: 'w,
{
    Complete(event::Dispatch<'d, ID>),
    Ready(
        Observed<'a, 'w, 'd, ID, W, S, M>,
        <M::Ingress as storage::raw::Mode>::Bound<'a, 'd>,
    ),
}

/// Settles observations using the storage capability selected by the ingress mode.
pub trait Delivery<'a, 'd: 'a, W: wire::Spec, P: storage::raw::Mode> {
    fn ready<'w, ID: Identity + 'w, S, M>(
        next: Observed<'a, 'w, 'd, ID, W, S, M>,
        storage: P::Bound<'a, 'd>,
    ) -> event::Dispatch<'d, ID>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = P>;
}

pub struct Direct;

pub struct Buffered;

impl<'a, 'd: 'a, W> Delivery<'a, 'd, W, mode::Disabled> for Direct
where
    W: wire::Spec,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
{
    fn ready<'w, ID: Identity + 'w, S, M>(
        next: Observed<'a, 'w, 'd, ID, W, S, M>,
        _: &'a mut (),
    ) -> event::Dispatch<'d, ID>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Disabled>,
    {
        if next.status.paused {
            return event::Dispatch::Rejected {
                completion: next.item.retire(),
                key: next.key.connection(),
                error: event::ParkError::Unavailable,
            };
        }
        next.ready()
    }
}

impl<'a, 'd: 'a, W, P> Delivery<'a, 'd, W, P> for Buffered
where
    W: wire::Spec,
    P: storage::raw::Mode,
    P::Bound<'a, 'd>: Write<'d>,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
{
    fn ready<'w, ID: Identity + 'w, S, M>(
        mut next: Observed<'a, 'w, 'd, ID, W, S, M>,
        mut store: P::Bound<'a, 'd>,
    ) -> event::Dispatch<'d, ID>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = P>,
    {
        if next.status.paused
            || (!next.status.close
                && matches!(next.status.origin, arrival::Origin::Fresh)
                && store.has_queued())
        {
            return match store.prepare_push(next.item) {
                Push::Ready { vacant, done } => {
                    vacant.append(done.complete());
                    if next.slot.control.engine.operations.recv.has_cancel() {
                        next.rearm.queue();
                    }
                    event::Dispatch::Parked(event::ParkRecv::Deferred)
                }
                Push::Full(done) => event::Dispatch::Rejected {
                    completion: done.retire(),
                    key: next.key.connection(),
                    error: event::ParkError::Full,
                },
            };
        }
        next.ready()
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, S, M> Observed<'a, 'w, 'd, ID, W, S, M>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
{
    pub(in crate::link::pool::recv) fn observe<'p, T, B, const IOV: usize, D>(
        pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        item: event::Control<'d>,
        timing: arrival::Timing,
    ) -> Observation<'a, 'w, 'd, ID, W, S, M>
    where
        T: crate::Transport,
        D: demand::Mode,
    {
        let key = match pool.keys.parse(item.token()) {
            Ok(key) => key,
            Err(_) => {
                return Observation::Complete(event::Dispatch::Retired(item.retire()));
            }
        };
        let Some((slot, recv)) = pool.prepared.slab.get(key.parts()) else {
            return Observation::Complete(event::Dispatch::Retired(item.retire()));
        };
        let status = item.status();
        let close = slot.is_closing();
        let stop = slot.is_recv_done();
        let (mut arm, probe, origin) = match timing {
            arrival::Timing::Fresh(more) => {
                let seen = match slot.control.engine.operations.observe_recv(more, stop) {
                    Some(seen) => seen,
                    None => {
                        return Observation::Complete(event::Dispatch::Retired(item.retire()));
                    }
                };
                (
                    seen.rearm().is_needed(),
                    seen.is_probe(),
                    arrival::Origin::Fresh,
                )
            }
            arrival::Timing::Replay => (
                slot.control.engine.operations.recv.is_armable(stop),
                false,
                arrival::Origin::Replay,
            ),
        };
        let end = !slot.control.engine.operations.recv.is_armed();
        if probe && !matches!(status, Status::Exhausted) {
            if let Err(stale) = slot.control.engine.settle_probe() {
                return Observation::Complete(event::Dispatch::Stale {
                    completion: item.retire(),
                    cause: stale,
                });
            }
            arm = slot.control.engine.operations.recv.is_armable(stop);
        } else if probe {
            arm = !stop;
        }
        if close
            && end
            && let Some(handle) = pool.prepared.pending.handle(key.connection())
        {
            handle.wake();
        }
        if stop && !matches!(status, Status::Failed(_)) {
            let done = item.dispatch(|kind| match kind {
                ControlKind::Eof | ControlKind::Cancelled => ControlFlow::Continue(()),
                kind => ControlFlow::Break((kind, ())),
            });
            return Observation::Complete(match done {
                ControlFlow::Continue(()) => event::Dispatch::Settled,
                ControlFlow::Break((item, ())) => event::Dispatch::Retired(item.retire()),
            });
        }
        let close = close && !slot.terminal.has_eof();
        let paused = slot.terminal.has_eof()
            || (slot.control.engine.operations.recv.is_paused()
                && !close
                && !slot.terminal.is_read_done());
        Observation::Ready(
            Self {
                slot,
                rearm: recv.rearm,
                key,
                item,
                status: Flags {
                    arm,
                    end,
                    close,
                    paused,
                    origin,
                },
                input: marker::PhantomData,
            },
            recv.storage,
        )
    }

    fn ready(mut self) -> event::Dispatch<'d, ID>
    where
        <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    {
        let next = self.item.dispatch(|kind| {
            let next = match kind {
                ControlKind::Eof => self.slot.recv_eof(),
                ControlKind::Cancelled => self.slot.cancelled(self.status.end, self.status.arm),
                ControlKind::Exhausted(full) => {
                    if self
                        .slot
                        .control
                        .engine
                        .operations
                        .recv
                        .exhaustion()
                        .has_waiter()
                        || (matches!(self.status.origin, arrival::Origin::Replay)
                            && !self.status.end)
                    {
                        if self.status.arm {
                            self.rearm.queue();
                        }
                        return ControlFlow::Continue(None);
                    }
                    if !self.status.arm {
                        return ControlFlow::Break((
                            ControlKind::Exhausted(full),
                            Rejected::Retired,
                        ));
                    }
                    let target = self.slot.control.engine.ready_handle().target();
                    let reg = match Registration::try_acquire(target) {
                        Ok(reg) => reg,
                        Err(Absent) => {
                            return ControlFlow::Break((
                                ControlKind::Exhausted(full),
                                Rejected::Retired,
                            ));
                        }
                    };
                    match reg.exhausted(full) {
                        Ok(Poll::Ready(trial)) => {
                            if self
                                .slot
                                .control
                                .engine
                                .operations
                                .recv
                                .exhaustion()
                                .try_probe()
                                .is_started()
                            {
                                trial.commit();
                                self.rearm.queue();
                            }
                        }
                        Ok(Poll::Pending) => self
                            .slot
                            .control
                            .engine
                            .operations
                            .recv
                            .exhaustion()
                            .pending(),
                        Err(stopped) => {
                            return ControlFlow::Break((
                                ControlKind::Exhausted(stopped.0),
                                Rejected::Stale(stopped.1),
                            ));
                        }
                    }
                    slot::Control::NoChunk { needs_rearm: false }
                }
                ControlKind::Starved(error) => {
                    return ControlFlow::Break((
                        ControlKind::Starved(error),
                        Rejected::Observed(slot::Control::NoChunk {
                            needs_rearm: self.status.arm,
                        }),
                    ));
                }
                ControlKind::Failed(error) => self.slot.recv_failed(error),
            };
            ControlFlow::Continue(Some(next))
        });
        match next {
            ControlFlow::Continue(None) => event::Dispatch::Settled,
            ControlFlow::Continue(Some(next)) => {
                event::Dispatch::Ready(delivery::Data::<ID, W, M::Kind>::control(self.rearm, next))
            }
            ControlFlow::Break((state, Rejected::Retired)) => {
                event::Dispatch::Retired(state.retire())
            }
            ControlFlow::Break((state, Rejected::Observed(next))) => event::Dispatch::Observed {
                completion: state.retire(),
                next: delivery::Data::<ID, W, M::Kind>::control(self.rearm, next),
            },
            ControlFlow::Break((state, Rejected::Stale(cause))) => event::Dispatch::Stale {
                completion: state.retire(),
                cause,
            },
        }
    }
}
