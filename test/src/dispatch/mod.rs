//! Statically dispatched runtime probes for integration tests.

use std::{marker, ops};

use dope::{
    core::{
        driver::{
            retained, route,
            schedule::{self, ready},
        },
        io,
    },
    runtime::executor,
};
use o3::cell::region;

mod sealed;
pub(in crate::dispatch) use sealed::{Proof, ReadyState};

/// Whether the exact event supplied to a probe is consumed or deferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventDecision {
    Consume,
    Defer,
}

/// Safe, statically dispatched callbacks for a test-only runtime probe.
/// Static hook state cannot hide a branded owner; `R` is sealed to audited
/// ready-slot storage forms.
pub trait Hooks<'d, R>: 'static + Unpin {
    fn dispatch(&mut self, _ready: &mut R, _event: &io::Event<'d>) -> EventDecision {
        EventDecision::Consume
    }

    fn activate(
        &mut self,
        _ready: &mut R,
        _target: route::Token,
        _driver: &mut retained::Context<'_, '_, 'd>,
    ) {
    }

    fn pre_park(&mut self, _ready: &mut R, _driver: &mut retained::Context<'_, '_, 'd>) {}

    fn progress(&self, _ready: &R, _region: &region::Token<'d>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn shutdown(&mut self, _ready: &mut R) {}

    fn finish(&mut self, _ready: &mut R) {}
}
/// One statically routed dispatcher probe with no dynamic dispatch or allocation.
pub struct Probe<'d, R, H, const ID: u8, const FILTER: bool = true> {
    ready: R,
    hooks: H,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

pub struct Builder<H> {
    hooks: H,
}

impl<H> Builder<H> {
    pub const fn new(hooks: H) -> Self {
        Self { hooks }
    }

    pub fn probe<'d, const ID: u8>(self) -> Probe<'d, (), H, ID> {
        Probe::new((), self.hooks)
    }

    pub fn ready<'d, const ID: u8>(
        self,
        ready: ready::Slot<'d, route::KeyTag<ID>>,
    ) -> Probe<'d, ready::Slot<'d, route::KeyTag<ID>>, H, ID> {
        Probe::new(ready, self.hooks)
    }

    pub fn ready_set<'d, const ID: u8>(
        self,
        ready: Box<[ready::Slot<'d, route::KeyTag<ID>>]>,
    ) -> Probe<'d, Box<[ready::Slot<'d, route::KeyTag<ID>>]>, H, ID> {
        Probe::new(ready, self.hooks)
    }
}

impl<'d, R, H, const ID: u8, const FILTER: bool> Probe<'d, R, H, ID, FILTER> {
    fn new(ready: R, hooks: H) -> Self {
        Self {
            ready,
            hooks,
            driver: marker::PhantomData,
        }
    }
}

impl<'d, R, H, const ID: u8, const FILTER: bool> executor::Application<'d>
    for Probe<'d, R, H, ID, FILTER>
where
    R: ReadyState<'d>,
    H: Hooks<'d, R>,
{
    fn dispatch<'driver, 'turn, 'app>(
        call: executor::raw::Dispatch<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'app,
    {
        let (self_, event) = <Self as Proof<'d>>::dispatch(call);
        if FILTER && event.route() != ID {
            return ops::ControlFlow::Continue(());
        }
        let this = self_.get_mut();
        match this.hooks.dispatch(&mut this.ready, &event) {
            EventDecision::Consume => ops::ControlFlow::Continue(()),
            EventDecision::Defer => ops::ControlFlow::Break(event),
        }
    }

    fn activate<'driver, 'turn, 'app>(
        call: executor::raw::Activate<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) where
        'd: 'app,
    {
        let (self_, target, mut driver) = <Self as Proof<'d>>::activate(call);
        let exact =
            route::Space::<route::KeyTag<ID>>::for_driver(driver.driver_ref()).parse(target);
        if !FILTER || exact.is_some() {
            let this = self_.get_mut();
            this.hooks.activate(&mut this.ready, target, &mut driver);
        }
    }

    fn pre_park<'driver, 'turn, 'app>(
        call: executor::raw::PrePark<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) where
        'd: 'app,
    {
        let (self_, mut driver) = <Self as Proof<'d>>::pre_park(call);
        let this = self_.get_mut();
        this.hooks.pre_park(&mut this.ready, &mut driver);
    }

    fn progress(call: executor::raw::Progress<'_, '_, 'd, Self>) -> schedule::Progress<'d> {
        let (self_, region) = <Self as Proof<'d>>::progress(call);
        let this = self_.get_ref();
        this.hooks.progress(&this.ready, region)
    }

    fn shutdown<'driver, 'turn, 'app>(
        call: executor::raw::Shutdown<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) -> executor::raw::Pending<'app, 'd, Self>
    where
        'd: 'app,
    {
        let (self_, mut shutdown) = <Self as Proof<'d>>::shutdown(call);
        let this = self_.get_mut();
        this.hooks.shutdown(&mut this.ready);
        shutdown.pending()
    }

    fn finish<'finalization, 'app>(call: executor::raw::Finish<'_, 'finalization, 'app, 'd, Self>)
    where
        'd: 'app,
    {
        let self_ = <Self as Proof<'d>>::finish(call);
        let this = self_.get_mut();
        this.hooks.finish(&mut this.ready);
    }
}

pub(crate) struct ReadyCollector;

impl<'a, 'd> Hooks<'d, &'a mut Vec<route::Token>> for ReadyCollector {
    fn activate(
        &mut self,
        values: &mut &'a mut Vec<route::Token>,
        target: route::Token,
        _driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        values.push(target);
    }
}

impl ReadyCollector {
    pub(crate) fn probe<'d, const ID: u8>(
        values: &mut Vec<route::Token>,
    ) -> Probe<'d, &mut Vec<route::Token>, Self, ID, false> {
        Probe::new(values, Self)
    }
}
