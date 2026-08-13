use std::{io, mem, process};

use dope_core::driver::{flight, route, schedule::ready::completion};

use crate::file::sealed::operation;

#[derive(Clone, Copy)]
enum Phase {
    Submitted,
    Waiting,
    CancelActive,
    CancelSubmitted,
    CancelDone,
    Settled,
}

union Payload<'d, R> {
    empty: (),
    wake: mem::ManuallyDrop<completion::Waker<'d>>,
    output: mem::ManuallyDrop<R>,
}

struct State<'d, R> {
    phase: Phase,
    payload: Payload<'d, R>,
}

pub(super) struct Operation<'d, O: operation::Contract> {
    pub(super) prepared: O::Prepared,
    state: State<'d, O::Output>,
    pub(super) flight: Option<flight::Flight<'d>>,
}

pub(super) struct SettledWitness;
pub(super) struct CancelWitness;
pub(super) struct QueueWitness;

pub(super) enum CancelStep<'d, Tag: route::Tag> {
    Submit(route::Operation<'d, Tag>),
    Retire(CancelWitness),
}

pub(super) enum Completion<W> {
    Stored,
    Wake(W),
    Retire(CancelWitness),
    Submit,
}

impl<'d, R> State<'d, R> {
    fn submitted() -> Self {
        Self {
            phase: Phase::Submitted,
            payload: Payload { empty: () },
        }
    }

    fn wait(&mut self, wake: completion::Waker<'d>) {
        match self.phase {
            Phase::Submitted => {
                self.payload.wake = mem::ManuallyDrop::new(wake);
                self.phase = Phase::Waiting;
            }
            Phase::Waiting => {
                let _ = unsafe { mem::replace(&mut *self.payload.wake, wake) };
            }
            _ => {}
        }
    }

    fn settled_witness(&self) -> Option<SettledWitness> {
        matches!(self.phase, Phase::Settled).then_some(SettledWitness)
    }

    fn begin_cancel<'target, Tag: route::Tag>(
        &mut self,
        target: route::Operation<'target, Tag>,
    ) -> Option<CancelStep<'target, Tag>> {
        match self.phase {
            Phase::CancelActive => {
                self.phase = Phase::CancelSubmitted;
                Some(CancelStep::Submit(target))
            }
            Phase::CancelDone => Some(CancelStep::Retire(CancelWitness)),
            _ => None,
        }
    }

    fn retry_cancel(&mut self) {
        if matches!(self.phase, Phase::CancelSubmitted) {
            self.phase = Phase::CancelActive;
        }
    }

    fn into_output(mut self, _witness: SettledWitness) -> R {
        debug_assert!(matches!(self.phase, Phase::Settled));
        self.phase = Phase::CancelDone;
        unsafe { mem::ManuallyDrop::take(&mut self.payload.output) }
    }

    fn queue_cancel(&mut self) -> Option<QueueWitness> {
        match self.phase {
            Phase::Submitted => {
                self.phase = Phase::CancelActive;
                Some(QueueWitness)
            }
            Phase::Waiting => {
                self.phase = Phase::CancelActive;
                unsafe { mem::ManuallyDrop::drop(&mut self.payload.wake) };
                Some(QueueWitness)
            }
            Phase::CancelActive | Phase::CancelSubmitted | Phase::CancelDone | Phase::Settled => {
                None
            }
        }
    }
}

impl<R> Drop for State<'_, R> {
    fn drop(&mut self) {
        unsafe {
            match self.phase {
                Phase::Waiting => mem::ManuallyDrop::drop(&mut self.payload.wake),
                Phase::Settled => mem::ManuallyDrop::drop(&mut self.payload.output),
                Phase::Submitted
                | Phase::CancelActive
                | Phase::CancelSubmitted
                | Phase::CancelDone => {}
            }
        }
    }
}

impl<'d, O: operation::Contract> Operation<'d, O> {
    pub(super) fn submitted(prepared: O::Prepared) -> Self {
        Self {
            prepared,
            state: State::submitted(),
            flight: None,
        }
    }

    pub(super) fn wait(&mut self, wake: completion::Waker<'d>) {
        self.state.wait(wake);
    }

    pub(super) fn settled_witness(&self) -> Option<SettledWitness> {
        self.state.settled_witness()
    }

    pub(super) fn begin_cancel<Tag: route::Tag>(
        &mut self,
        target: route::Target<'d, Tag>,
    ) -> Option<CancelStep<'d, Tag>> {
        let target = O::target(&self.prepared, target);
        self.state.begin_cancel(target)
    }

    pub(super) fn retry_cancel(&mut self) {
        self.state.retry_cancel();
    }

    pub(super) fn into_settled(self, witness: SettledWitness) -> (O, O::Output) {
        let Self {
            prepared,
            state,
            flight,
        } = self;
        debug_assert!(flight.is_none());
        let output = state.into_output(witness);
        (O::into_hold(prepared), output)
    }

    pub(super) fn queue_cancel(&mut self) -> Option<QueueWitness> {
        self.state.queue_cancel()
    }

    pub(super) fn complete(&mut self, event: O::Event) -> Completion<completion::Waker<'d>> {
        let Some(flight) = self.flight.take() else {
            process::abort();
        };
        let _ = flight.complete();
        match self.state.phase {
            Phase::Submitted => match O::complete(&mut self.prepared, event) {
                operation::Step::Submit => Completion::Submit,
                operation::Step::Done(output) => {
                    self.state.payload.output = mem::ManuallyDrop::new(output);
                    self.state.phase = Phase::Settled;
                    Completion::Stored
                }
            },
            Phase::Waiting => match O::complete(&mut self.prepared, event) {
                operation::Step::Submit => Completion::Submit,
                operation::Step::Done(output) => {
                    let wake = unsafe { mem::ManuallyDrop::take(&mut self.state.payload.wake) };
                    self.state.payload.output = mem::ManuallyDrop::new(output);
                    self.state.phase = Phase::Settled;
                    Completion::Wake(wake)
                }
            },
            Phase::CancelActive | Phase::CancelSubmitted => {
                let _ = O::complete(&mut self.prepared, event);
                self.state.phase = Phase::CancelDone;
                Completion::Retire(CancelWitness)
            }
            Phase::CancelDone | Phase::Settled => process::abort(),
        }
    }

    pub(super) fn reject(&mut self, error: io::Error) -> Completion<completion::Waker<'d>> {
        let output = O::rejected(&mut self.prepared, error);
        match self.state.phase {
            Phase::Submitted => {
                self.state.payload.output = mem::ManuallyDrop::new(output);
                self.state.phase = Phase::Settled;
                Completion::Stored
            }
            Phase::Waiting => {
                let wake = unsafe { mem::ManuallyDrop::take(&mut self.state.payload.wake) };
                self.state.payload.output = mem::ManuallyDrop::new(output);
                self.state.phase = Phase::Settled;
                Completion::Wake(wake)
            }
            Phase::CancelActive | Phase::CancelSubmitted | Phase::CancelDone | Phase::Settled => {
                process::abort()
            }
        }
    }
}
