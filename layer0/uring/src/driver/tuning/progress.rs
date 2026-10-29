use std::num;

use dope_sys::{
    io::{
        reactor::{complete, complete::Outcome as _, submit::Queue as _},
        socket::option::tuning,
    },
    table::token::{
        self, Codec, Tuning as _,
        frame::{self, Decode as _},
    },
};
use o3::base::num::bounded;

use crate::{
    driver::{
        Key,
        tuning::{Phase, Table},
    },
    reactor::{cq, sq},
};

type Parts<T> = complete::Parts<T, cq::Outcome, cq::Flags>;

/// Native option completions retain their host flight until every submitted
/// scalar has completed and, when requested, the separate connect has finished.
pub(crate) trait Progress<T: Codec> {
    fn observe(&mut self, parts: Parts<T>) -> Option<Parts<T>>;
    fn next(&mut self, queue: sq::raw::Queue<'_, T>) -> Option<Option<Parts<T>>>;
    fn has_pending(&self) -> bool;
    fn cancel_all(&mut self);
    fn cancel_at(&mut self, slot: sq::Fixed, target: Option<Key<T>>) -> Option<&mut bool>;
}

impl<T: Codec, const N: usize> Progress<T> for Table<Key<T>, N> {
    fn observe(&mut self, mut parts: Parts<T>) -> Option<Parts<T>> {
        let token::Decoded::Framework(frame) = parts.token.decode() else {
            return Some(parts);
        };
        let frame::Control::Tuning(tune) = frame.decode() else {
            return Some(parts);
        };
        if parts.flags != cq::Flags::new(0) || parts.outcome.unit().is_err() {
            return Some(parts);
        }
        let index: num::NonZero<u32> = tune.slot().into();
        let index = index.get() - 1;
        let Some(state) = tuning::Table::get(self, index) else {
            return Some(parts);
        };
        // SAFETY: only scalar state and the unsubmitted SQE move; the pinned
        // option values stay at their original addresses throughout the phase.
        let state = unsafe { state.get_unchecked_mut() };
        if !matches!(state.status.phase, Phase::Options | Phase::OptionsConnect) {
            return Some(parts);
        }
        if !parts.outcome.is_zero()
            && (state.status.error.is_none()
                || state.status.error == Some(cq::Outcome::from_error(dope_sys::Errno::CANCELED)))
        {
            state.status.error = Some(parts.outcome);
        }
        if let Some(count) = num::NonZero::new(state.status.count.get() - 1) {
            state.status.count = count;
            return None;
        }
        if let Some(failure) = state.status.error {
            parts.outcome = failure;
        } else if state.status.stopped {
            parts.outcome = cq::Outcome::from_error(dope_sys::Errno::CANCELED);
        } else if matches!(state.status.phase, Phase::OptionsConnect) {
            state.status.phase = Phase::Ready;
            let slot = fixed(index);
            if let Some(tail) = self.tail {
                // SAFETY: `tail` was admitted by `get` before it was linked.
                let state = unsafe { self.admitted(tail) };
                // SAFETY: linking a ready slot does not move its pinned values.
                unsafe { state.get_unchecked_mut() }.next = Some(slot);
            } else {
                self.ready = Some(slot);
            }
            self.tail = Some(slot);
            return None;
        }
        state.status.phase = Phase::Complete;
        Some(parts)
    }

    fn next(&mut self, queue: sq::raw::Queue<'_, T>) -> Option<Option<Parts<T>>> {
        let slot = self.ready?;
        // SAFETY: `ready` holds a slot admitted by `get`; only scalar state
        // and the deferred request move, retaining every pinned option value.
        let state = unsafe { self.admitted(slot).get_unchecked_mut() };
        let stopped = state.status.stopped;
        if stopped {
            state.status.phase = Phase::Complete;
        } else {
            let vacancy = queue.try_acquire().ok()?;
            // SAFETY: `observe` links only an initialized, unread CONNECT.
            // This consumes it once; its host retains every encoded address
            // until the terminal CQE, including after this link is released.
            let entry = unsafe { state.connect.assume_init_read() };
            entry.submit(vacancy, token::<T>(slot));
            state.status.phase = Phase::Connecting;
        }
        self.ready = state.next.take();
        if self.ready.is_none() {
            self.tail = None;
        }
        Some(if stopped {
            Some(Parts {
                token: token::<T>(slot),
                outcome: cq::Outcome::from_error(dope_sys::Errno::CANCELED),
                flags: cq::Flags::new(0),
            })
        } else {
            None
        })
    }

    fn has_pending(&self) -> bool {
        self.ready.is_some()
    }

    fn cancel_all(&mut self) {
        // SAFETY: changing cancellation intent moves no pinned scalar.
        for state in unsafe { self.states.as_mut().get_unchecked_mut() } {
            if state.target.is_some() {
                state.status.stopped = true;
            }
        }
    }

    fn cancel_at(&mut self, slot: sq::Fixed, target: Option<Key<T>>) -> Option<&mut bool> {
        let state = tuning::Table::get(self, slot.index())?;
        // SAFETY: returning the cancellation field cannot move pinned values.
        let state = unsafe { state.get_unchecked_mut() };
        let current = state.target?;
        if target.is_some() && target != Some(current) {
            return None;
        }
        Some(&mut state.status.stopped)
    }
}

fn fixed(index: u32) -> sq::Fixed {
    let bytes = index.to_le_bytes();
    sq::Fixed::new(sq::Index::from_le([bytes[0], bytes[1], bytes[2]]))
}

fn token<T: Codec>(slot: sq::Fixed) -> T {
    let index: <T::Context as token::Context>::Slot =
        bounded::U32::clamp(slot.index() as usize).into();
    T::tuning(T::Tuning::new(index))
}
