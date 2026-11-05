use dope_fiber::context::PollResult;

pub mod set;

use std::{array, cell::Cell, pin::Pin, rc::Rc, task::Poll};

use dope::core::driver::{
    retained,
    route::{Epoch, Erased, KeyTag, indices::SlotIndex, kind},
    schedule::{
        self,
        ready::{Capacity, wake::Waker},
        state::scope,
    },
};
use dope_fiber::{
    abi::{self, Fiber, Ready, Task},
    context,
    context::{Context, PollCall},
};

pub enum Domain {}

pub fn nested<'d, F: Fiber<'d>>(fiber: F) -> impl Fiber<'d, Output = F::Output> {
    dope_fiber::fiber!('d => async move { fiber.await })
}

pub fn lexical_scope<'a, 'd>(
    failed: bool,
    entered: &'a Cell<usize>,
    retained: Rc<()>,
) -> impl Fiber<'d, Output = Result<(), usize>> + use<'a, 'd> {
    dope_fiber::fiber!('d => async move {
        Ready::new(()).await;
        let outcome: Result<(), usize> = async {
            let _retained = retained;
            entered.set(1);
            if !failed {
                return Ok(());
            }
            Err::<bool, u8>(7)?;
            Ok(())
        }.await;
        entered.set(2);
        outcome
    })
}

pub trait Rounds<'d> {
    fn ready_round(&self, activate: impl FnMut(Erased<'d, kind::Any>)) -> usize;
}

impl<'d> Rounds<'d> for scope::Guard<'_, 'd> {
    fn ready_round(&self, activate: impl FnMut(Erased<'d, kind::Any>)) -> usize {
        self.ready().drain(activate).map_or(0, |count| count.get())
    }
}

pub trait Polled<T> {
    fn polled(self) -> Option<Poll<T>>;
}

impl<T> Polled<T> for context::PollResult<T> {
    fn polled(self) -> Option<Poll<T>> {
        match self {
            PollResult::Polled(poll) => Some(poll),
            PollResult::Exhausted => None,
        }
    }
}

pub struct PendingCount(pub(crate) Rc<Cell<usize>>);

impl<'d> Fiber<'d> for PendingCount {
    type Output = ();

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let self_ = call.into_fiber();
        self_.0.set(self_.0.get() + 1);
        Poll::Pending
    }
}

pub struct Probe {
    pub(crate) polls: Rc<Cell<usize>>,
    pub(crate) ready: bool,
}

pub struct StateProbe {
    pub(crate) polls: Rc<Cell<usize>>,
    pub(crate) ready: Rc<Cell<bool>>,
    pub(crate) output: usize,
}

impl<'d> Fiber<'d> for StateProbe {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<usize> {
        let self_ = call.into_fiber();
        self_.polls.set(self_.polls.get() + 1);
        if self_.ready.get() {
            Poll::Ready(self_.output)
        } else {
            Poll::Pending
        }
    }
}

pub struct WakeProbe<'d> {
    pub(crate) polls: Rc<Cell<usize>>,
    pub(crate) wake: Rc<Cell<Option<Waker<'d>>>>,
}

pub struct SelfWake(pub(crate) Rc<Cell<usize>>);

impl<'d> Fiber<'d> for SelfWake {
    type Output = ();

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (self_, context) = call.into_parts();
        self_.0.set(self_.0.get() + 1);
        context.as_ref().completion().wake();
        Poll::Pending
    }
}

impl<'d> Fiber<'d> for WakeProbe<'d> {
    type Output = ();

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (self_, context) = call.into_parts();
        self_.polls.set(self_.polls.get() + 1);
        self_.wake.replace(Some(context.as_ref().completion()));
        Poll::Pending
    }
}

pub struct CaptureReady<'d> {
    pub(crate) wake: Rc<Cell<Option<Waker<'d>>>>,
    pub(crate) drops: Rc<Cell<usize>>,
    pub(crate) polls: Rc<Cell<usize>>,
    pub(crate) ready: Rc<Cell<bool>>,
    pub(crate) output: u8,
}

impl Drop for CaptureReady<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl<'d> Fiber<'d> for CaptureReady<'d> {
    type Output = u8;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<u8> {
        let (self_, context) = call.into_parts();
        self_.polls.set(self_.polls.get() + 1);
        self_.wake.replace(Some(context.as_ref().completion()));
        if self_.ready.get() {
            Poll::Ready(self_.output)
        } else {
            Poll::Pending
        }
    }
}

impl<'d> Fiber<'d> for Probe {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<usize> {
        let self_ = call.into_fiber();
        self_.polls.set(self_.polls.get() + 1);
        if self_.ready {
            Poll::Ready(1)
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct WideTarget(pub(crate) u128);

pub fn poll_twice<'d, F>(
    mut fiber: Pin<&mut Task<F>>,
    turn: schedule::Turn<'_, 'd>,
    mut driver: retained::Context<'_, 'd, 'd>,
) -> Result<(usize, [Option<bool>; 2], bool), Capacity>
where
    F: Fiber<'d>,
{
    let admitted = (0..schedule::LIMIT - 3)
        .filter(|_| turn.app().permit().is_some())
        .count();
    let core = driver.driver_ref();
    let targets = core.targets::<KeyTag<0>>();
    let root = core
        .ready()
        .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())?;
    let polls = Context::with_target(
        root.target(),
        turn.app(),
        driver.reborrow(),
        |mut context| {
            array::from_fn(|_| {
                Polled::polled(context.as_mut().try_poll(fiber.as_mut()))
                    .map(|poll| poll.is_pending())
            })
        },
    );
    Ok((admitted, polls, turn.app().permit().is_none()))
}

pub struct OwnedOnce<'a> {
    pub(crate) polls: &'a Cell<usize>,
    pub(crate) drops: &'a Cell<usize>,
    pub(crate) pending: bool,
    pub(crate) value: usize,
}

impl Drop for OwnedOnce<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl<'d> abi::Resume<'d> for OwnedOnce<'_> {
    type Output = usize;
    type Ready = ();

    fn poll<'slot, 'id>(
        live: &mut abi::Live<'slot, 'id, Self>,
        _: &mut context::Admission<'_, '_, 'd>,
    ) -> Poll<Self::Ready> {
        let state = live.get_mut();
        state.polls.set(state.polls.get() + 1);
        if state.pending {
            state.pending = false;
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn finish<'slot, 'id>(
        live: abi::Live<'slot, 'id, Self>,
        _: Self::Ready,
        _: context::Admission<'_, '_, 'd>,
    ) -> abi::Transition<'id, usize> {
        let (vacant, state) = live.take();
        vacant.complete(state.value)
    }
}
