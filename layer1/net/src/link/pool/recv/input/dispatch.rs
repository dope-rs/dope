use dope_core::driver::{route, route::Identity};

use crate::{
    link::{
        duplex,
        egress::reserve::demand,
        event,
        pool::{
            self,
            change::drain,
            recv::{
                Readiness, Ready, Resumption, arrival, delivery, input,
                receiver::{
                    observed::{self, Delivery as _, Observation, Observed},
                    resume::Resume,
                },
                storage::{self, queue::Ops as _},
            },
            send,
        },
        slot::recv::Capture,
    },
    wire::{
        self,
        receive::{self, mode, reserve},
    },
};

pub trait Dispatch<W: wire::Spec>: storage::raw::Mode + drain::Policy<W> + send::Finish<W> {
    /// Selects direct or buffered delivery under this exact storage loan.
    type Delivery<'a, 'd: 'a>: observed::Delivery<'a, 'd, W, Self>;

    fn dispatch<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        item: arrival::Input<event::Control<'d>>,
    ) -> event::Dispatch<'d, ID>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        let (item, timing) = item.into_parts();
        match Observed::<ID, W, S, M>::observe(pool, item, timing) {
            Observation::Complete(done) => done,
            Observation::Ready(next, storage) => Self::Delivery::ready(next, storage),
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
    ) -> Ready<'d>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        let key = item.key();
        let Some((slot, mut events)) = pool.prepared.slab.get(key.parts()) else {
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
        D: demand::Mode,
    {
        let Some((slot, mut recv)) = pool.prepared.slab.get(key.parts()) else {
            return;
        };
        let queued = recv.storage.has_queued();
        let close = slot.engine.operations.lifecycle.is_closing();
        let ended = slot.terminal.is_read_done();
        let next = slot.engine.operations.recv.resume_app(close || ended);
        let wake = !close
            && ((queued && (ended || next.is_some() && !slot.engine.operations.recv.is_paused()))
                || (slot.terminal.has_eof() && next.is_some()));
        let ready = wake.then(|| slot.io().ready_handle());
        if next.is_some_and(duplex::Rearm::is_needed) && !queued {
            recv.rearm.queue();
        }
        if let Some(ready) = ready {
            ready.activate();
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
    type Delivery<'a, 'd: 'a> = observed::Direct;
}

impl<W> Dispatch<W> for mode::Parking
where
    W: wire::Spec,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
{
    type Delivery<'a, 'd: 'a> = observed::Buffered;
}

impl<W> Dispatch<W> for mode::Enabled
where
    W: wire::Spec,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
{
    type Delivery<'a, 'd: 'a> = observed::Buffered;

    fn resume<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        target: route::Target<'d, ID>,
    ) -> Resumption<'d, ID>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        match Resume::try_new(pool, target) {
            Ok(resume) => Resumption::Ready(resume.credit()),
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
    type Delivery<'a, 'd: 'a> = observed::Buffered;

    fn resume<'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        target: route::Target<'d, ID>,
    ) -> Resumption<'d, ID>
    where
        'd: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        match Resume::try_new(pool, target) {
            Ok(resume) => Resumption::Ready(resume.waiting::<R>()),
            Err(missing) => Resumption::Unavailable(missing),
        }
    }
}
