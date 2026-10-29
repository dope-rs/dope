use std::{cell::Cell, rc::Rc, task::Poll, time};

use dope_core::driver::route::KeyTag;
use dope_manifold::timing::interval::{Interval, Tick};
use dope_runtime::{control, executor::root};
use dope_test::harness::signals::Gate;

type ID = KeyTag<41>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
#[coordinate]
pub struct Host<'d> {
    #[pin]
    #[manifold(control)]
    interval: Interval<'d, ID>,
    #[dispatcher(state)]
    gate: Gate,
    #[dispatcher(state)]
    observed: Option<Rc<Cell<Option<time::Instant>>>>,
}

impl<'d> Host<'d> {
    pub fn new(interval: Interval<'d, ID>, gate: Gate) -> Self {
        Self {
            interval,
            gate,
            observed: None,
        }
    }

    pub fn recording(
        interval: Interval<'d, ID>,
        gate: Gate,
        observed: Rc<Cell<Option<time::Instant>>>,
    ) -> Self {
        Self {
            interval,
            gate,
            observed: Some(observed),
        }
    }

    fn coordinate(mut this: host_coord::Fields<'_, 'd>) -> control::Flow {
        if let Some(observed) = this.observed.as_ref() {
            this.step
                .now()
                .at(observed)
                .apply(|observed, now| observed.set(Some(now)));
        }
        if this.interval.take_tick() == Tick::Ready {
            this.gate.hit();
        }
        control::Flow::Idle
    }
}

pub struct Root<'a>(pub &'a Cell<usize>);

impl<'d> root::Root<'d> for Root<'_> {
    type Output = ();

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, _, _, _) = context.into_parts();
        let polls = root.get_mut().0;
        polls.set(polls.get().saturating_add(1));
        Poll::Ready(())
    }
}
