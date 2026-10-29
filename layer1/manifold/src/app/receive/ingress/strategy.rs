use std::{ops::ControlFlow, pin};

use dope_core::driver::{
    retained, route,
    route::Identity,
    schedule::{self, ready},
};
use dope_net::{
    link::{
        event,
        pool::recv::{self, Readiness, Ready, arrival, delivery, reserve::Choice},
    },
    wire,
};

use crate::app::receive::{Mode, ingress};

#[derive(Clone, Copy)]
pub(crate) enum Replay {
    First,
    Waiting,
}

pub(crate) trait Admission<'a, 'c: 'a, 'owner: 'a, 'd: 'a + 'c + 'owner>:
    Sized
    + Into<(
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    )>
{
    fn fresh(
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> schedule::Capacity<Self>;

    fn replay(
        replay: Replay,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> schedule::Capacity<Self>;
}

pub(crate) trait Strategy<W: wire::Spec, M: Mode<W>> {
    type Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;

    type Pending<'w, 'd>
    where
        'd: 'w,
        W: 'w + 'd;

    type Admission<'a, 'c, 'owner, 'd>: Admission<'a, 'c, 'owner, 'd>
    where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'owner, 'w, 'd, ID: Identity + 'd, P, A>(
        policy: pin::Pin<&mut P>,
        data: A,
        work: Self::Admission<'a, 'c, 'owner, 'd>,
    ) where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner + 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>;

    fn resume_tail<'p, 'w, 'd, ID: Identity + 'd, P>(
        _: pin::Pin<&mut P>,
        _: &delivery::Resumed<'d, ID>,
        _: bool,
        _: schedule::Turn<'_, 'd>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> ControlFlow<(), bool>
    where
        'd: 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
    {
        ControlFlow::Continue(false)
    }

    fn resume<'p, 'w, 'd, ID: Identity + 'd, P>(
        mut policy: pin::Pin<&mut P>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        'd: 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
    {
        let item = match P::storage(policy.as_mut()).recv().resume(target) {
            recv::Resumption::Ready(item) => item,
            recv::Resumption::Unavailable(_) => return,
        };
        let ready = item.ready();
        let mut first = true;
        loop {
            match Self::resume_tail(policy.as_mut(), &item, first, turn.reborrow(), driver) {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(true) => {
                    first = false;
                    continue;
                }
                ControlFlow::Continue(false) => {}
            }
            let key = item.key();
            let (end, done) = {
                let inspect = P::storage(policy.as_mut()).inspect();
                let done = matches!(inspect.recv(key), Readiness::Ready);
                let end = inspect.get(key).is_some_and(|slot| {
                    slot.terminal.has_eof() || (!done && slot.terminal.kind().is_some())
                });
                (end, done)
            };
            if end {
                ingress::prepare_terminal(policy.as_mut(), key);
                match P::terminal(policy.as_mut(), key, turn.reborrow(), driver) {
                    ingress::Terminal::Ended(end) => {
                        ingress::finish_eof(policy.as_mut(), key, end, turn.reborrow(), driver);
                        first = false;
                        continue;
                    }
                    ingress::Terminal::Pending | ingress::Terminal::Closed => break,
                    ingress::Terminal::Idle => {}
                }
            }
            if !done {
                break;
            }
            let replay = if first {
                Replay::First
            } else {
                Replay::Waiting
            };
            let work = match Self::Admission::replay(replay, ready, turn.reborrow(), driver) {
                schedule::Capacity::Available(work) => work,
                schedule::Capacity::Exhausted => break,
            };
            let done = match P::storage(policy.as_mut()).recv().pop_ready(&item) {
                Ready::Completion(done) => done,
                Ready::Empty | Ready::Paused | Ready::Stale => break,
            };
            first = false;
            match done.classify() {
                event::Kind::Data(data) => Self::drive_data(policy.as_mut(), data, work),
                event::Kind::Control(ctrl) => {
                    let (driver, step) = work.into();
                    ingress::drive_control(policy.as_mut(), ctrl, step, driver);
                }
            }
        }
        P::flush(policy, turn, driver);
    }
}
