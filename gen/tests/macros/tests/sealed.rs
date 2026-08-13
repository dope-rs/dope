use std::{ops, pin::Pin};

use dope::{
    core::driver::schedule::{self, Progress},
    manifold::dispatch::raw::Manifold,
};

use crate::tests::{BudgetCounter, Counter};

// SAFETY: Counter retains no driver-visible pointer and is pinned by every
// generated dispatcher that owns it.
unsafe impl<'d, const ID: u8, M> Manifold<'d> for Counter<ID, M> {
    const ID: u8 = ID;
    type Dispatch = dope::manifold::dispatch::raw::Plain;
    type Activate = dope::manifold::dispatch::raw::Plain;
    type PrePark = dope::manifold::dispatch::raw::Plain;
    type Shutdown = dope::manifold::dispatch::raw::Plain;

    fn install(
        self: Pin<&mut Self>,
        _install: &mut dope::core::driver::lifecycle::Install<'_, 'd>,
    ) {
        let calls = &self.as_ref().get_ref().install_calls;
        calls.set(calls.get() + 1);
    }

    unsafe fn dispatch<'turn>(
        self: Pin<&mut Self>,
        _ev: dope::core::io::Event<'d>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<dope::core::io::Event<'d>> {
        let this = self.as_ref().get_ref();
        this.dispatch_calls.set(this.dispatch_calls.get() + 1);
        ops::ControlFlow::Continue(())
    }

    unsafe fn activate<'turn>(
        self: Pin<&mut Self>,
        _target: dope::manifold::dispatch::typed::Token<'d, Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let this = self.as_ref().get_ref();
        this.activate_calls.set(this.activate_calls.get() + 1);
    }

    unsafe fn pre_park<'turn>(
        self: Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let this = self.as_ref().get_ref();
        this.tick_calls.set(this.tick_calls.get() + 1);
    }

    fn progress(self: Pin<&Self>, _region: &o3::cell::region::Token<'d>) -> Progress<'d> {
        let this = self.as_ref().get_ref();
        this.idle_calls.set(this.idle_calls.get() + 1);
        if this.pending {
            Progress::Runnable
        } else {
            Progress::Quiescent
        }
    }

    fn shutdown<'turn>(
        self: Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        let calls = &self.as_ref().get_ref().shutdown_calls;
        calls.set(calls.get() + 1);
    }

    fn finish(
        self: Pin<&mut Self>,
        _context: &mut dope::core::driver::lifecycle::Finalize<'_, 'd>,
    ) {
        let calls = &self.as_ref().get_ref().finish_calls;
        calls.set(calls.get() + 1);
    }
}

// SAFETY: BudgetCounter retains no driver-visible pointer and is pinned by its
// generated dispatcher for the complete run.
unsafe impl<'d, const ID: u8> Manifold<'d> for BudgetCounter<ID> {
    const ID: u8 = ID;
    type Dispatch = dope::manifold::dispatch::raw::Plain;
    type Activate = dope::manifold::dispatch::raw::Plain;
    type PrePark = dope::manifold::dispatch::raw::Plain;
    type Shutdown = dope::manifold::dispatch::raw::Plain;

    unsafe fn dispatch<'turn>(
        self: Pin<&mut Self>,
        _ev: dope::core::io::Event<'d>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<dope::core::io::Event<'d>> {
        ops::ControlFlow::Continue(())
    }

    unsafe fn pre_park<'turn>(
        self: Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let this = self.as_ref().get_ref();
        if this.first.get().is_some() {
            return;
        }
        let mut consumed = 0;
        while turn.maintenance().take() {
            consumed += 1;
        }
        this.first.set(Some(consumed));
    }

    fn progress(self: Pin<&Self>, _region: &o3::cell::region::Token<'d>) -> Progress<'d> {
        Progress::Quiescent
    }

    fn shutdown<'turn>(
        self: Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
    }
}
