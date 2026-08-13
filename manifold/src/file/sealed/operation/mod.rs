use std::{cell, io, process};

use dope_core::{
    driver::{
        self, flight, ops,
        route::{self, table},
        schedule,
        schedule::ready::completion,
    },
    io::fs,
};
use driver::retained;
use o3::{
    cell::region,
    collections::{self, slab},
    queue,
};

use crate::file::cancellation;

mod slot;
mod state;

/// # Safety
/// Submitted resources stay fixed and inaccessible until terminal completion.
pub(super) unsafe trait Contract: Sized {
    type Mode: fs::Mode;
    type Event;
    type Output;
    type Prepared;

    fn prepare(self) -> Result<Self::Prepared, (Self, io::Error)>;

    fn submission<'a, 'd, Tag: route::Tag>(
        prepared: &'a mut Self::Prepared,
        target: route::Target<'d, Tag>,
    ) -> io::Result<fs::Submission<'a, 'd, Self::Mode, Tag>>;

    fn into_hold(prepared: Self::Prepared) -> Self;

    fn target<'d, Tag: route::Tag>(
        prepared: &Self::Prepared,
        target: route::Target<'d, Tag>,
    ) -> route::Operation<'d, Tag>;

    fn complete(prepared: &mut Self::Prepared, event: Self::Event) -> Step<Self::Output>;

    fn rejected(prepared: &mut Self::Prepared, error: io::Error) -> Self::Output;
}

pub(super) enum Step<R> {
    Submit,
    Done(R),
}

struct CancellationQueue<Tag: route::Tag> {
    entries: queue::Fifo<table::Parts<Tag>>,
}

impl<Tag: route::Tag> CancellationQueue<Tag> {
    fn try_with_capacity(capacity: table::Capacity) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            entries: queue::Fifo::try_with_capacity(capacity.get())?,
        })
    }

    fn push(&self, parts: table::Parts<Tag>, _witness: state::QueueWitness) {
        // SAFETY: a QueueWitness is emitted once per live operation generation;
        // the operation slab and this queue have the same fixed capacity.
        unsafe { queue::raw::Fifo::push_back_unchecked(&self.entries, parts) };
    }

    fn pop(&self) -> Option<table::Parts<Tag>> {
        self.entries.pop_front()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub(super) struct OperationTable<'d, O: Contract, Tag: route::Tag> {
    flights: flight::Slots<'d, Tag>,
    entries: table::CellSlab<state::Operation<'d, O>, Tag>,
    cancelled: CancellationQueue<Tag>,
    retry: slot::Slot<Tag>,
    shutdown_cursor: cell::Cell<Option<usize>>,
}

impl<'d, O: Contract, const ID: u8, const KIND: u8> OperationTable<'d, O, route::KeyTag<ID, KIND>> {
    pub(super) fn try_with_capacity(
        capacity: table::Capacity,
        flights: flight::Slots<'d, route::KeyTag<ID, KIND>>,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            flights,
            entries: table::CellSlab::try_with_capacity(capacity)?,
            cancelled: CancellationQueue::try_with_capacity(capacity)?,
            retry: slot::Slot::empty(),
            shutdown_cursor: cell::Cell::new(None),
        })
    }

    pub(super) fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        if self.retry.is_deferred() {
            return schedule::Progress::waiting(region);
        }
        if self.shutdown_cursor.get().is_some()
            || !self.retry.is_empty()
            || !self.cancelled.is_empty()
        {
            return schedule::Progress::Runnable;
        }
        if !self.entries.is_empty() {
            schedule::Progress::waiting(region)
        } else {
            schedule::Progress::Quiescent
        }
    }

    pub(super) fn begin_shutdown(&self, signal: &cancellation::Cancellation) {
        if self.shutdown_cursor.get().is_none() {
            self.shutdown_cursor.set(Some(0));
            signal.mark();
        }
    }

    pub(super) fn begin(
        &self,
        hold: O,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<route::Token, (O, io::Error)> {
        let prepared = O::prepare(hold)?;
        let inserted = self.entries.try_insert_build(
            prepared,
            state::Operation::submitted,
            |key, operation| {
                let target = route::Space::for_driver(driver.driver().driver_ref()).bind_key(key);
                let submission = O::submission(&mut operation.prepared, target)?;
                // SAFETY: the fixed slab retains `prepared` at this address through terminal
                // completion. Rejected submission rolls the entry back before returning it.
                let flight =
                    unsafe { retained::raw::Owner::submit_file(driver, &self.flights, submission) }
                        .map_err(io::Error::from)?;
                operation.flight = Some(flight);
                Ok(())
            },
        );
        match inserted {
            Ok((key, ())) => Ok(route::Token::from_key(key)),
            Err(slab::BuildError::Full(prepared)) => Err((
                O::into_hold(prepared),
                io::Error::from(io::ErrorKind::WouldBlock),
            )),
            Err(slab::BuildError::Rejected(operation, error)) => {
                Err((O::into_hold(operation.prepared), error))
            }
        }
    }

    pub(super) fn poll(
        &self,
        token: route::Token,
        wake: completion::Waker<'d>,
    ) -> Option<(O, O::Output)> {
        let parts = token.parts::<route::KeyTag<ID, KIND>>()?;
        let (operation, witness) = self.entries.remove_parts_with(parts, |operation| {
            let settled = operation.settled_witness();
            if settled.is_none() {
                operation.wait(wake);
            }
            settled
        })?;
        Some(operation.into_settled(witness))
    }

    pub(super) fn request_cancel(
        &self,
        token: route::Token,
        signal: &cancellation::Cancellation,
    ) -> Option<O> {
        enum Action {
            Queue(state::QueueWitness),
            Ignore,
            Remove(state::SettledWitness),
        }

        let parts = token.parts::<route::KeyTag<ID, KIND>>()?;
        let action = self.entries.update_parts(parts, |operation| {
            if let Some(witness) = operation.queue_cancel() {
                Action::Queue(witness)
            } else if let Some(witness) = operation.settled_witness() {
                Action::Remove(witness)
            } else {
                Action::Ignore
            }
        });
        match action {
            Some(Action::Queue(witness)) => {
                self.cancelled.push(parts, witness);
                signal.mark();
                None
            }
            Some(Action::Remove(witness)) => Some(self.take_settled(parts, witness).0),
            Some(Action::Ignore) | None => None,
        }
    }

    pub(super) fn flush_cancellations(
        &self,
        signal: &cancellation::Cancellation,
        work: schedule::Maintenance<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        if !self.enqueue_shutdown(signal, work) {
            return false;
        }
        let mut next = match self.retry.take() {
            Some((parts, _deferred)) => Some(parts),
            None => self.cancelled.pop(),
        };
        while let Some(parts) = next {
            if !work.take() {
                self.retry.store(parts, false);
                return false;
            }
            match self
                .entries
                .update_parts(parts, |operation| {
                    let target = route::Space::for_driver(driver.driver_ref()).bind_parts(parts);
                    operation.begin_cancel(target)
                })
                .flatten()
            {
                Some(state::CancelStep::Submit(target)) => {
                    let submitted = self
                        .entries
                        .update_parts(parts, |operation| {
                            operation.flight.as_mut().is_some_and(|flight| {
                                ops::Submit::cancel(driver, flight, target).is_ok()
                            })
                        })
                        .unwrap_or(false);
                    if !submitted {
                        if self
                            .entries
                            .update_parts(parts, state::Operation::retry_cancel)
                            .is_none()
                        {
                            process::abort();
                        }
                        self.retry.store(parts, true);
                        return false;
                    }
                }
                Some(state::CancelStep::Retire(witness)) => {
                    self.retire(parts, witness);
                }
                None => {}
            }
            next = self.cancelled.pop();
        }
        true
    }

    fn enqueue_shutdown(
        &self,
        signal: &cancellation::Cancellation,
        work: schedule::Maintenance<'_, 'd>,
    ) -> bool {
        let Some(mut cursor) = self.shutdown_cursor.get() else {
            return true;
        };
        let capacity = self.entries.capacity().get();
        while cursor < capacity {
            if !work.take() {
                self.shutdown_cursor.set(Some(cursor));
                return false;
            }
            if let Some(key) = self.entries.key_at(cursor) {
                let _ = self.request_cancel(route::Token::from_key(key), signal);
            }
            cursor += 1;
        }
        self.shutdown_cursor.set(None);
        true
    }

    pub(super) fn complete(
        &self,
        token: route::Token,
        event: O::Event,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let Some(parts) = token.parts::<route::KeyTag<ID, KIND>>() else {
            process::abort();
        };
        let Some(completion) = self.entries.update_parts(parts, |operation| {
            let base = route::Space::for_driver(driver.driver().driver_ref()).bind_parts(parts);
            if !O::target(&operation.prepared, base).matches(token) {
                process::abort();
            }
            operation.complete(event)
        }) else {
            process::abort();
        };
        match completion {
            state::Completion::Submit => {
                let Some(result) = self.entries.update_parts(parts, |operation| {
                    let base =
                        route::Space::for_driver(driver.driver().driver_ref()).bind_parts(parts);
                    let submission = O::submission(&mut operation.prepared, base)?;
                    // SAFETY: this fixed entry belongs to the installed Files owner and
                    // remains pinned until this next terminal completion or quiescence.
                    let flight = unsafe {
                        retained::raw::Owner::submit_file(driver, &self.flights, submission)
                    }
                    .map_err(io::Error::from)?;
                    operation.flight = Some(flight);
                    Ok(())
                }) else {
                    process::abort();
                };
                if let Err(error) = result {
                    let Some(completion) = self
                        .entries
                        .update_parts(parts, |operation| operation.reject(error))
                    else {
                        process::abort();
                    };
                    self.finish_completion(parts, completion);
                }
            }
            completion => self.finish_completion(parts, completion),
        }
    }

    fn finish_completion(
        &self,
        parts: table::Parts<route::KeyTag<ID, KIND>>,
        completion: state::Completion<completion::Waker<'d>>,
    ) {
        match completion {
            state::Completion::Stored => {}
            state::Completion::Wake(wake) => wake.wake(),
            state::Completion::Retire(witness) => self.retire(parts, witness),
            state::Completion::Submit => process::abort(),
        }
    }

    fn take_settled(
        &self,
        parts: table::Parts<route::KeyTag<ID, KIND>>,
        witness: state::SettledWitness,
    ) -> (O, O::Output) {
        let Some((operation, witness)) = self.entries.remove_parts_with(parts, |_| Some(witness))
        else {
            process::abort();
        };
        operation.into_settled(witness)
    }

    fn retire(&self, parts: table::Parts<route::KeyTag<ID, KIND>>, witness: state::CancelWitness) {
        let Some((operation, _witness)) = self.entries.remove_parts_with(parts, |_| Some(witness))
        else {
            process::abort();
        };
        drop(operation);
    }
}
