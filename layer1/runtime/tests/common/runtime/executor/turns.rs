use std::{cell::Cell, rc::Rc, thread, time::Instant};

use dope_core::{
    driver::{
        retained::{self},
        route::{Erased, KeyTag, kind},
        schedule::ready::Slot,
    },
    io::{self},
};
use dope_runtime::shutdown;

use crate::common::scenario::dispatch;

pub struct Cascade {
    pub polls: Rc<Cell<Option<usize>>>,
    pub first: Rc<Cell<Option<usize>>>,
    pub stop: usize,
}

impl<'d> dispatch::Hooks<'d, Slot<'d, KeyTag<254>>> for Cascade {
    type Park = dispatch::On;

    const WAKE: bool = true;

    fn activate(
        &mut self,
        ready: &mut Slot<'d, KeyTag<254>>,
        _: Erased<'d, kind::Any>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        let polls = self.polls.get().and_then(|polls| polls.checked_add(1));
        self.polls.set(polls);
        if polls.is_some_and(|polls| polls < self.stop) {
            ready.activate();
        }
    }
}

impl<'d> dispatch::Park<'d, Slot<'d, KeyTag<254>>> for Cascade {
    fn pre_park(&mut self, _: &mut Slot<'d, KeyTag<254>>, _: &mut retained::Context<'_, '_, 'd>) {
        if self.first.get().is_none() {
            self.first.set(self.polls.get());
        }
    }
}

pub struct Clock {
    pub stable: Rc<Cell<Option<bool>>>,
}

impl<'d> dispatch::Hooks<'d, Slot<'d, KeyTag<254>>> for Clock {
    type Park = dispatch::Off;

    const WAKE: bool = true;

    fn activate(
        &mut self,
        _: &mut Slot<'d, KeyTag<254>>,
        _: Erased<'d, kind::Any>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let start = driver.driver_ref().clock().now();
        let tick = Instant::now();
        let advanced = (0..1_000).any(|_| {
            thread::yield_now();
            Instant::now() != tick
        });
        self.stable
            .set(Some(advanced && start == driver.driver_ref().clock().now()));
    }
}

pub struct Storm {
    pub polls: Rc<Cell<Option<usize>>>,
    pub first: Rc<Cell<Option<usize>>>,
}

pub struct Deferred<'a> {
    pub calls: &'a mut Option<usize>,
    pub shutdown: Option<shutdown::Trigger>,
    pub fired: &'a mut Option<Result<(), shutdown::FireError>>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct Missing<T> {
    #[pin]
    #[app(optional)]
    pub app: Option<T>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct Fallback<T> {
    #[pin]
    #[app]
    pub first: Missing<T>,
    #[pin]
    #[app]
    pub second: T,
}

impl<'d> dispatch::Hooks<'d, ()> for Deferred<'_> {
    type Park = dispatch::Off;

    fn dispatch(&mut self, _: &mut (), _: &io::Event<'d>) -> dispatch::Decision {
        *self.calls = self.calls.and_then(|calls| calls.checked_add(1));
        if *self.calls == Some(1) {
            dispatch::Decision::Defer
        } else {
            if let Some(trigger) = self.shutdown.take() {
                *self.fired = Some(trigger.fire());
            }
            dispatch::Decision::Consume
        }
    }
}

impl<'d> dispatch::Hooks<'d, Box<[Slot<'d, KeyTag<254>>]>> for Storm {
    type Park = dispatch::On;

    const WAKE: bool = true;

    fn activate(
        &mut self,
        _: &mut Box<[Slot<'d, KeyTag<254>>]>,
        _: Erased<'d, kind::Any>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.polls
            .set(self.polls.get().and_then(|polls| polls.checked_add(1)));
    }
}

impl<'d> dispatch::Park<'d, Box<[Slot<'d, KeyTag<254>>]>> for Storm {
    fn pre_park(
        &mut self,
        _: &mut Box<[Slot<'d, KeyTag<254>>]>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        if self.first.get().is_none() {
            self.first.set(self.polls.get());
        }
    }
}
