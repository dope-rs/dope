use crate::DriverContext;
use dope_core::backend::Sqe;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{Key, KeyTag, SLOT_MASK, Token, TokenCellSlab};
use dope_core::io::fd::FdSlot;

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

pub(super) enum CompletionAction<R> {
    Settle(R),
    Resubmit { sqe: Sqe, failed: R },
}

impl<R> From<R> for CompletionAction<R> {
    fn from(result: R) -> Self {
        Self::Settle(result)
    }
}

pub(super) struct Targets {
    first: Token,
    second: Option<Token>,
    release: Option<FdSlot>,
}

impl Targets {
    pub(super) fn one(first: Token) -> Self {
        Self {
            first,
            second: None,
            release: None,
        }
    }

    pub(super) fn two_releasing(first: Token, second: Token, slot: FdSlot) -> Self {
        Self {
            first,
            second: Some(second),
            release: Some(slot),
        }
    }

    fn append_to(self, targets: &mut Vec<Token>) {
        targets.push(self.first);
        if let Some(second) = self.second {
            targets.push(second);
        }
    }

    fn quiesce(self, driver: &mut DriverContext<'_, '_>) {
        match self.second {
            Some(second) => driver.quiesce(&[self.first, second]),
            None => driver.quiesce(std::slice::from_ref(&self.first)),
        };
        if let Some(slot) = self.release {
            // SAFETY: cancellation removed the operation that uniquely owned this
            // reserved slot, and quiescing stopped every create touching it.
            drop(unsafe { driver.guard_raw(slot) });
        }
    }
}

pub(super) struct OperationTable<'d, H, R, Tag> {
    entries: TokenCellSlab<Operation<'d, H, R>, Tag>,
}

impl<'d, H, R, const ID: u8, const KIND: u8> OperationTable<'d, H, R, KeyTag<ID, KIND>> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity <= SLOT_MASK as usize + 1,
            "dope: file table overflow"
        );
        Self {
            entries: TokenCellSlab::with_capacity(capacity),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn append_targets(&self, targets: &mut Vec<Token>) {
        self.append_targets_with(targets, |token, _| Targets::one(token));
    }

    pub(super) fn append_targets_with(
        &self,
        targets: &mut Vec<Token>,
        mut map: impl FnMut(Token, &H) -> Targets,
    ) {
        for key in self.entries.keys() {
            let token = Token::from_key(key);
            if let Some(target) = self
                .entries
                .update(key, |operation| match operation.state {
                    State::Submitted | State::Waiting(_) | State::CancelPending => {
                        Some(map(token, &operation.hold))
                    }
                    State::Settled(_) => None,
                })
                .flatten()
            {
                target.append_to(targets);
            }
        }
    }

    pub(super) fn begin<T>(
        &self,
        hold: H,
        driver: &mut DriverContext<'_, 'd>,
        make_sqe: impl FnOnce(Token, &mut H) -> Option<(T, Sqe)>,
    ) -> Result<T, H> {
        let (key, entry) = self
            .entries
            .insert_with(
                Operation {
                    hold,
                    state: State::Submitted,
                },
                |key, operation| {
                    let token = Token::from_key(key);
                    (key, make_sqe(token, &mut operation.hold))
                },
            )
            .map_err(|operation| operation.hold)?;
        let Some((result, sqe)) = entry else {
            return Err(self.remove(key).unwrap());
        };
        if driver.push(sqe).is_err() {
            return Err(self.remove(key).unwrap());
        }
        Ok(result)
    }

    fn remove(&self, key: Key<KeyTag<ID, KIND>>) -> Option<H> {
        self.entries.remove(key).map(|operation| operation.hold)
    }

    pub(super) fn poll(&self, token: Token, wake: CompletionWaker<'d>) -> Option<(H, R)> {
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

    pub(super) fn request_cancel(&self, token: Token) -> Option<H> {
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

    pub(super) fn flush_cancellations(
        &self,
        driver: &mut DriverContext<'_, 'd>,
        mut target: impl FnMut(Token, &H) -> Targets,
    ) -> bool {
        let keys: Vec<_> = self.entries.keys().collect();
        let mut quiesced = false;
        for key in keys {
            let token = Token::from_key(key);
            let pending = self
                .entries
                .update(key, |operation| {
                    matches!(operation.state, State::CancelPending)
                        .then(|| target(token, &operation.hold))
                })
                .flatten();
            let Some(targets) = pending else {
                continue;
            };
            targets.quiesce(driver);
            let _ = self.entries.remove(key);
            quiesced = true;
        }
        quiesced
    }

    pub(super) fn complete<E, C>(
        &self,
        token: Token,
        event: E,
        driver: &mut DriverContext<'_, 'd>,
        transition: impl FnOnce(&mut H, E) -> C,
    ) where
        C: Into<CompletionAction<R>>,
    {
        let Some(parts) = token.parts::<KeyTag<ID, KIND>>() else {
            return;
        };
        let mut wake = None;
        self.entries
            .update_parts(parts.slab(), |operation| match &operation.state {
                State::CancelPending => {
                    let _: CompletionAction<R> = transition(&mut operation.hold, event).into();
                }
                State::Settled(_) => {}
                State::Submitted | State::Waiting(_) => {
                    let registered = match &operation.state {
                        State::Waiting(registered) => Some(*registered),
                        _ => None,
                    };
                    match transition(&mut operation.hold, event).into() {
                        CompletionAction::Settle(result) => {
                            operation.state = State::Settled(result);
                            wake = registered;
                        }
                        CompletionAction::Resubmit { sqe, failed } => {
                            if driver.push(sqe).is_err() {
                                operation.state = State::Settled(failed);
                                wake = registered;
                            }
                        }
                    }
                }
            });
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
