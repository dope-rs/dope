use dope_core::driver::{route, route::Identity};

use crate::{
    link::{
        egress::{data, reserve::demand},
        pool::{
            self,
            change::drain,
            recv::{
                Readiness, Ready, Resumption, delivery, input,
                receiver::{observed, resume::Resume},
                storage::{self, queue::Ops as _},
            },
        },
        slot::recv::Capture,
    },
    wire::{
        self,
        receive::{self, mode, reserve},
        send::Availability,
    },
};

pub trait Dispatch<W: wire::Spec>: Sized + storage::raw::Mode + drain::Policy<W> {
    /// Selects direct or buffered delivery under this exact storage loan.
    type Delivery<'a, 'd: 'a, R: 'a>: observed::Delivery<'a, 'd, W, Self, R>;

    fn release<'w, 'd: 'w, ID: Identity + 'w>(_: Availability, _: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w + 'd,
    {
    }

    fn resume<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        target: route::Target<'d, ID>,
    ) -> Resumption<'d, ID>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        match Resume::try_new(pool, target) {
            Ok(resume) => Resumption::Ready(resume.direct()),
            Err(missing) => Resumption::Unavailable(missing),
        }
    }

    fn pop<'a, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        item: &'a delivery::Resumed<'d, ID>,
    ) -> Ready<'d, input::Receipt<W, M>>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        let key = item.key();
        let Some((slot, mut events)) = pool.prepared.slab.get_mut(key.parts()) else {
            return Ready::Stale;
        };
        if slot.engine.operations.recv.is_paused() && !slot.terminal.is_read_done() {
            return Ready::Paused;
        }
        match events.storage.prepare_pop() {
            Some(front) => Ready::Completion(front.pop()),
            None => Ready::Empty,
        }
    }

    fn wake<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        key: pool::Key<'d, ID>,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        if let Ok(mut entry) = pool.entry(key) {
            entry.resume();
        }
    }

    fn inspect<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        key: pool::Key<'d, ID>,
    ) -> Readiness
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        let Some((slot, queued)) = pool.prepared.slab.read(key.parts()) else {
            return Readiness::Stale;
        };
        if slot.engine.operations.recv.is_paused() && !slot.terminal.is_read_done() {
            Readiness::Paused
        } else if queued {
            Readiness::Ready
        } else {
            Readiness::Empty
        }
    }
}

impl<W> Dispatch<W> for mode::Disabled
where
    W: wire::Spec,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
{
    type Delivery<'a, 'd: 'a, R: 'a> = observed::Direct;
}

impl<W> Dispatch<W> for mode::Parking
where
    W: wire::Spec,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
{
    type Delivery<'a, 'd: 'a, R: 'a> = observed::Buffered;
}

impl<W> Dispatch<W> for mode::Enabled
where
    W: wire::Spec,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
{
    type Delivery<'a, 'd: 'a, R: 'a> = observed::Buffered;

    fn resume<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        target: route::Target<'d, ID>,
    ) -> Resumption<'d, ID>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        match Resume::try_new(pool, target) {
            Ok(resume) => Resumption::Ready(resume.credit(|_| {})),
            Err(missing) => Resumption::Unavailable(missing),
        }
    }
}

impl<W, R> Dispatch<W> for mode::Waiting<R>
where
    W: wire::Spec,
    R: reserve::Try<W>,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    W::Receive: receive::Policy<W, Reserve = reserve::Waiting<R>>,
{
    type Delivery<'a, 'd: 'a, C: 'a> = observed::Buffered;

    fn release<'w, 'd: 'w, ID: Identity + 'w>(space: Availability, run: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w + 'd,
    {
        if space == Availability::Released {
            R::release_send::<ID>(run);
        }
    }

    fn resume<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        target: route::Target<'d, ID>,
    ) -> Resumption<'d, ID>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
    {
        match Resume::try_new(pool, target) {
            Ok(resume) => Resumption::Ready(resume.credit(R::release_recv)),
            Err(missing) => Resumption::Unavailable(missing),
        }
    }
}
