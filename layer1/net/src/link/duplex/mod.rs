use std::mem;

use dope_core::driver::route::{Identity, KeyTag};

mod ops;
mod receive;
pub(in crate::link) mod sending;
pub(in crate::link) mod state;

use dope_core::{
    driver::{
        self,
        receive::{Absent, Registration, Stale},
        retained::flight,
        route,
        schedule::ready,
    },
    io::{
        event::net::{connect, tuning},
        fd::handles,
        socket::{self, option},
    },
};
pub(in crate::link) use ops::{Cancel, QueueError, Shutdown};
pub(in crate::link) use receive::Rearm;
use send::reclaim;

use crate::{
    link::{self, setup},
    wire::{
        receive::{discard, mode},
        send, terminal,
    },
};

pub(in crate::link) struct Duplex<
    'd,
    ID: Identity,
    D: discard::Policy = mode::Disabled,
    P: reclaim::Policy = reclaim::OnComplete,
> {
    pub(in crate::link) operations: ops::State<'d, P>,
    pub(in crate::link) discard: D::State,
    pub(in crate::link) establish: link::Setup<'d, ID>,
}

impl<'d, ID: Identity, D: discard::Policy, P: reclaim::Policy> Duplex<'d, ID, D, P> {
    pub(in crate::link) fn tuned(
        &mut self,
        done: tuning::Completion<'d>,
    ) -> setup::Completion<tuning::Completion<'d>> {
        let (next, step) = self
            .establish
            .take()
            .tuned(done, &self.operations.lifecycle);
        self.establish = next;
        step
    }

    pub(in crate::link) fn created<A: socket::Prepared>(
        &mut self,
        made: handles::CreatedSlot<'d>,
        opts: option::Set,
        addr: &A,
        target: route::Target<'d, ID>,
        flights: &flight::Slots<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> setup::Submission {
        let (next, step) = self
            .establish
            .take()
            .created(made, opts, addr, target, flights, driver);
        self.establish = next;
        step
    }

    pub(in crate::link) fn complete(
        &mut self,
        done: connect::Completion<'d>,
    ) -> setup::Completion<connect::Completion<'d>, connect::Error> {
        let (next, step) = self.establish.take().complete(done);
        self.establish = next;
        step
    }

    pub(in crate::link) fn cancel(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> setup::Cancellation {
        let (next, step) = self.establish.take().cancel(driver);
        self.establish = next;
        step
    }

    pub(in crate::link) fn send<'a, E>(
        &self,
        state: &'a mut terminal::State<E>,
    ) -> terminal::Admission<terminal::Send<'a, E>> {
        if self.operations.lifecycle.close.is_discarded() {
            return terminal::Admission::Closed;
        }
        state.send()
    }

    pub(in crate::link) fn both<'a, E>(
        &self,
        state: &'a mut terminal::State<E>,
    ) -> terminal::Admission<terminal::Both<'a, E>> {
        if self.operations.lifecycle.close.is_discarded() {
            return terminal::Admission::Closed;
        }
        state.both()
    }

    pub(in crate::link) fn settle_probe(&mut self) -> Result<(), Stale> {
        match Registration::try_acquire(self.ready_handle().target()) {
            Ok(reg) => {
                let result = reg.cancel();
                self.operations.recv.exhaustion().settle_probe();
                result
            }
            Err(Absent) => {
                self.operations.recv.exhaustion().cancel();
                Ok(())
            }
        }
    }

    pub(in crate::link) fn captured(&mut self, captured: terminal::Captured) {
        if matches!(captured, terminal::Captured::None) {
            return;
        }
        let handle = self.ready_handle();
        if matches!(captured, terminal::Captured::Eof) {
            Registration::clear(handle.target());
            self.operations.recv.exhaustion().cancel();
        }
        self.operations.capture(handle);
    }

    pub(in crate::link) fn discard<E>(&mut self, end: &mut terminal::State<E>) {
        self.operations.lifecycle.discard();
        let resumed = end.is_pending() && self.operations.recv.resume_app(true).is_some();
        if matches!(end.finish_send(), terminal::Wake::Ready) || resumed {
            self.ready_handle().activate();
        }
    }

    pub(in crate::link) fn new(setup: link::Setup<'d, ID>) -> Self {
        Self {
            operations: ops::State::new(),
            discard: D::STATE,
            establish: setup,
        }
    }

    pub(in crate::link) fn fd(&self) -> Option<&handles::Descriptor<'d>> {
        self.establish.fd()
    }

    pub(in crate::link) fn ready_handle(&self) -> ready::Handle<'d> {
        self.establish.ready_handle()
    }
}

const _: () = {
    let setup = mem::size_of::<link::Setup<'_, KeyTag<0>>>();
    let disabled = mem::size_of::<ops::State<'_>>()
        + mem::size_of::<<mode::Disabled as discard::Policy>::State>()
        + setup;
    let align = mem::align_of::<Duplex<'_, KeyTag<0>>>();
    assert!(mem::size_of::<Duplex<'_, KeyTag<0>>>() == disabled.div_ceil(align) * align);

    let enabled = mem::size_of::<ops::State<'_>>()
        + mem::size_of::<<mode::Enabled as discard::Policy>::State>()
        + setup;
    let align = mem::align_of::<Duplex<'_, KeyTag<0>, mode::Enabled>>();
    assert!(
        mem::size_of::<Duplex<'_, KeyTag<0>, mode::Enabled>>() == enabled.div_ceil(align) * align
    );

    let submit = mem::size_of::<ops::State<'_, reclaim::OnSubmit>>()
        + mem::size_of::<<mode::Disabled as discard::Policy>::State>()
        + setup;
    let align = mem::align_of::<Duplex<'_, KeyTag<0>, mode::Disabled, reclaim::OnSubmit>>();
    assert!(
        mem::size_of::<Duplex<'_, KeyTag<0>, mode::Disabled, reclaim::OnSubmit>>()
            == submit.div_ceil(align) * align
    );

    let combined = mem::size_of::<ops::State<'_, reclaim::OnSubmit>>()
        + mem::size_of::<<mode::Enabled as discard::Policy>::State>()
        + setup;
    let align = mem::align_of::<Duplex<'_, KeyTag<0>, mode::Enabled, reclaim::OnSubmit>>();
    assert!(
        mem::size_of::<Duplex<'_, KeyTag<0>, mode::Enabled, reclaim::OnSubmit>>()
            == combined.div_ceil(align) * align
    );
};
