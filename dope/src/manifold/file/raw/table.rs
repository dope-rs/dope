use std::cell::Cell;
use std::io;

use crate::DriverContext;
use dope_core::backend::{RawSqe, RetainedSqe, StableSqeSource};
use dope_core::driver::control::Quiesce;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{Key, KeyParts, KeyTag, Token, TokenCapacity, TokenCellSlab};
use o3::collections::CellQueue;
use std::io::Error;
use std::io::ErrorKind;
use std::mem::replace;
use std::process::abort;

struct TableSubmission(RawSqe);

// SAFETY: the sole construction sites run after the operation and its hold
// enter the table, which retains every captured resource until quiescence.
unsafe impl StableSqeSource for TableSubmission {
    fn into_raw(self) -> RawSqe {
        self.0
    }
}

enum State<'d, R> {
    Submitted,
    Waiting(CompletionWaker<'d>),
    CancelPending,
    Settled(R),
}

struct Operation<'d, H, R> {
    hold: H,
    state: State<'d, R>,
}

pub(in crate::manifold::file) struct CancellationSignal(Cell<bool>);

impl CancellationSignal {
    pub(in crate::manifold::file) const fn new() -> Self {
        Self(Cell::new(false))
    }

    fn mark(&self) {
        self.0.set(true);
    }

    pub(in crate::manifold::file) fn is_pending(&self) -> bool {
        self.0.get()
    }

    pub(in crate::manifold::file) fn clear(&self) {
        self.0.set(false);
    }
}

struct BeginEntry<'a, 'd, H, R, Tag> {
    entries: &'a TokenCellSlab<Operation<'d, H, R>, Tag>,
    key: Key<Tag>,
    active: bool,
}

impl<'a, 'd, H, R, Tag> BeginEntry<'a, 'd, H, R, Tag> {
    fn new(entries: &'a TokenCellSlab<Operation<'d, H, R>, Tag>, key: Key<Tag>) -> Self {
        Self {
            entries,
            key,
            active: true,
        }
    }

    fn commit(mut self) {
        self.active = false;
    }

    fn rollback(mut self) -> H {
        self.active = false;
        match self.entries.remove(self.key) {
            Some(operation) => operation.hold,
            None => unreachable!("file operation entry vanished during begin"),
        }
    }
}

impl<H, R, Tag> Drop for BeginEntry<'_, '_, H, R, Tag> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.entries.remove(self.key);
        }
    }
}

pub(in crate::manifold::file) struct OperationTable<'d, H, R, Tag> {
    entries: TokenCellSlab<Operation<'d, H, R>, Tag>,
    cancelled: CellQueue<KeyParts<Tag>>,
}

impl<'d, H, R, const ID: u8, const KIND: u8> OperationTable<'d, H, R, KeyTag<ID, KIND>> {
    pub(in crate::manifold::file) fn with_capacity(capacity: TokenCapacity) -> Self {
        Self {
            entries: TokenCellSlab::with_capacity(capacity),
            cancelled: CellQueue::with_capacity(capacity.get()),
        }
    }

    pub(in crate::manifold::file) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(in crate::manifold::file) fn for_each_target(&self, mut visit: impl FnMut(Token)) {
        for key in self.entries.keys() {
            let token = Token::from_key(key);
            if self
                .entries
                .update(key, |operation| match &operation.state {
                    State::Submitted | State::Waiting(_) | State::CancelPending => true,
                    State::Settled(_) => false,
                })
                .unwrap_or(false)
            {
                visit(token);
            }
        }
    }

    pub(in crate::manifold::file) fn begin<T>(
        &self,
        hold: H,
        driver: &mut DriverContext<'_, 'd>,
        make_sqe: impl FnOnce(Token, &mut H) -> Option<(T, RawSqe)>,
    ) -> Result<T, H> {
        let key = self
            .entries
            .insert(Operation {
                hold,
                state: State::Submitted,
            })
            .map_err(|operation| operation.hold)?;
        let entry = BeginEntry::new(&self.entries, key);
        let prepared = self.entries.update(key, |operation| {
            make_sqe(Token::from_key(key), &mut operation.hold)
        });
        let Some(Some((result, sqe))) = prepared else {
            return Err(entry.rollback());
        };
        if driver
            .push_retained(RetainedSqe::from_stable(TableSubmission(sqe)))
            .is_err()
        {
            return Err(entry.rollback());
        }
        entry.commit();
        Ok(result)
    }

    pub(in crate::manifold::file) fn begin_prepared<T>(
        &self,
        hold: H,
        driver: &mut DriverContext<'_, 'd>,
        prepare: impl FnOnce(Token, &mut H) -> io::Result<(T, RawSqe)>,
        accepted: impl FnOnce(&mut H),
        aborted: impl FnOnce(&mut H),
    ) -> Result<T, (H, Error)> {
        let key = self
            .entries
            .insert(Operation {
                hold,
                state: State::Submitted,
            })
            .map_err(|operation| (operation.hold, Error::from(ErrorKind::WouldBlock)))?;
        let entry = BeginEntry::new(&self.entries, key);
        let prepared = self.entries.update(key, |operation| {
            prepare(Token::from_key(key), &mut operation.hold)
        });
        let (result, sqe) = match prepared {
            Some(Ok(prepared)) => prepared,
            Some(Err(error)) => return Err((entry.rollback(), error)),
            None => abort(),
        };
        if let Err(error) = driver.push_retained(RetainedSqe::from_stable(TableSubmission(sqe))) {
            let hold = entry.rollback();
            let mut hold = hold;
            aborted(&mut hold);
            return Err((hold, error.into()));
        }
        let accepted = self
            .entries
            .update(key, |operation| accepted(&mut operation.hold));
        if accepted.is_none() {
            abort();
        }
        entry.commit();
        Ok(result)
    }

    pub(in crate::manifold::file) fn poll(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> Option<(H, R)> {
        let parts = token.parts::<KeyTag<ID, KIND>>()?;
        let (operation, ()) =
            self.entries
                .remove_parts_with(parts, |operation| match &operation.state {
                    State::Settled(_) => Some(()),
                    State::Submitted | State::Waiting(_) => {
                        operation.state = State::Waiting(wake);
                        None
                    }
                    State::CancelPending => None,
                })?;
        match operation.state {
            State::Settled(result) => Some((operation.hold, result)),
            _ => unreachable!(),
        }
    }

    pub(in crate::manifold::file) fn request_cancel(
        &self,
        token: Token,
        signal: &CancellationSignal,
    ) -> Option<H> {
        enum Action {
            Queue,
            Ignore,
            Remove,
        }

        let parts = token.parts::<KeyTag<ID, KIND>>()?;
        let action = self
            .entries
            .update_parts(parts, |operation| match &operation.state {
                State::Submitted | State::Waiting(_) => {
                    operation.state = State::CancelPending;
                    Action::Queue
                }
                State::CancelPending => Action::Ignore,
                State::Settled(_) => Action::Remove,
            })
            .unwrap_or(Action::Ignore);
        match action {
            Action::Queue => {
                if self.cancelled.push_back(parts).is_err() {
                    abort();
                }
                signal.mark();
                None
            }
            Action::Ignore => None,
            Action::Remove => self
                .entries
                .remove_parts(parts)
                .map(|operation| operation.hold),
        }
    }

    pub(in crate::manifold::file) fn flush_cancellations(&self, quiesce: &mut Quiesce<'_>) {
        while let Some(parts) = self.cancelled.pop_front() {
            quiesce.cancel(Token::from_parts(parts));
            let Some(operation) = self.entries.remove_parts(parts) else {
                abort();
            };
            if !matches!(operation.state, State::CancelPending) {
                abort();
            }
            drop(operation);
        }
    }

    pub(in crate::manifold::file) fn complete<E>(
        &self,
        token: Token,
        event: E,
        transition: impl FnOnce(&mut H, E) -> R,
    ) {
        let Some(parts) = token.parts::<KeyTag<ID, KIND>>() else {
            return;
        };
        let mut wake = None;
        self.entries.update_parts(parts, |operation| {
            match replace(&mut operation.state, State::CancelPending) {
                State::CancelPending => {
                    let _ = transition(&mut operation.hold, event);
                }
                State::Settled(result) => {
                    operation.state = State::Settled(result);
                }
                State::Submitted => {
                    operation.state = State::Settled(transition(&mut operation.hold, event));
                }
                State::Waiting(registered) => {
                    operation.state = State::Settled(transition(&mut operation.hold, event));
                    wake = Some(registered);
                }
            }
        });
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
