use std::io;

use crate::DriverContext;
use dope_core::backend::RawSqe;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::submission::raw::Submission as _;
use dope_core::driver::token::{Key, KeyTag, SLOT_MASK, Token, TokenCellSlab};
use std::io::Error;
use std::io::ErrorKind;
use std::process::abort;
use std::slice::from_ref;

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
}

impl<'d, H, R, const ID: u8, const KIND: u8> OperationTable<'d, H, R, KeyTag<ID, KIND>> {
    pub(in crate::manifold::file) fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity <= SLOT_MASK as usize + 1,
            "dope: file table overflow"
        );
        Self {
            entries: TokenCellSlab::with_capacity(capacity),
        }
    }

    pub(in crate::manifold::file) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(in crate::manifold::file) fn append_targets(&self, targets: &mut Vec<Token>) {
        for key in self.entries.keys() {
            let token = Token::from_key(key);
            if self
                .entries
                .update(key, |operation| match operation.state {
                    State::Submitted | State::Waiting(_) | State::CancelPending => true,
                    State::Settled(_) => false,
                })
                .unwrap_or(false)
            {
                targets.push(token);
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
        // SAFETY: the operation and its `hold` were inserted before SQE
        // construction and remain in the table until completion or quiesce.
        if unsafe { driver.push_raw(sqe) }.is_err() {
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
        // SAFETY: `hold` owns every descriptor backing the raw SQE and stays
        // table-resident until completion. Rejection rolls it back below.
        if let Err(error) = unsafe { driver.push_raw(sqe) } {
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
                .remove_parts_with(parts.slab(), |operation| match &operation.state {
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

    pub(in crate::manifold::file) fn request_cancel(&self, token: Token) -> Option<H> {
        let parts = token.parts::<KeyTag<ID, KIND>>()?;
        let settled = self
            .entries
            .update_parts(parts.slab(), |operation| match operation.state {
                State::Submitted | State::Waiting(_) => {
                    operation.state = State::CancelPending;
                    false
                }
                State::CancelPending => false,
                State::Settled(_) => true,
            })
            .unwrap_or(false);
        settled
            .then(|| self.entries.remove_parts(parts.slab()))
            .flatten()
            .map(|operation| operation.hold)
    }

    pub(in crate::manifold::file) fn flush_cancellations(
        &self,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let keys: Vec<_> = self.entries.keys().collect();
        let mut quiesced = false;
        for key in keys {
            let token = Token::from_key(key);
            let pending = self
                .entries
                .update(key, |operation| {
                    matches!(operation.state, State::CancelPending)
                })
                .unwrap_or(false);
            if !pending {
                continue;
            }
            driver.quiesce(from_ref(&token));
            let _ = self.entries.remove(key);
            quiesced = true;
        }
        quiesced
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
        self.entries
            .update_parts(parts.slab(), |operation| match &operation.state {
                State::CancelPending => {
                    let _ = transition(&mut operation.hold, event);
                }
                State::Settled(_) => {}
                State::Submitted | State::Waiting(_) => {
                    let registered = match &operation.state {
                        State::Waiting(registered) => Some(*registered),
                        _ => None,
                    };
                    operation.state = State::Settled(transition(&mut operation.hold, event));
                    wake = registered;
                }
            });
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
