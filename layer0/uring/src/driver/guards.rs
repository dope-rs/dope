use std::convert::Infallible;

use dope_sys::{
    engine::{
        admit::{self, Context},
        ring,
    },
    io::{
        reactor::submit::{self, Control, Queue as _, Ready as _},
        request,
    },
    ring::ready::Ready as _,
    table::token::{self, Codec, Tuning as _},
};
use o3::base::num::bounded;

use crate::{
    Uring,
    driver::{
        Engine, Key,
        tuning::{self, Progress as _},
    },
    reactor::sq,
};

/// An SQ reservation for one key-bound cancellation.
pub enum Cancellation<'a, T: Codec> {
    Pending {
        vacant: sq::raw::Vacancy<'a, T>,
        key: Key<T>,
    },
    Tuning {
        vacant: sq::raw::Vacancy<'a, T>,
        target: T::Tuning,
        stopped: &'a mut bool,
    },
    Completed,
}

impl<T: Codec> admit::Submission for Cancellation<'_, T> {
    fn submit(self) -> admit::Cancelled {
        match self {
            Self::Pending { vacant, key } => {
                let entry = Uring::cancel(T::flight(key), T::cancel_key(key));
                submit::raw::Vacancy::prepare(vacant, &entry).push();
                admit::Cancelled::Submitted
            }
            Self::Tuning {
                vacant,
                target,
                stopped,
            } => {
                let entry = tuning::cancel::<T>(target);
                *stopped = true;
                submit::raw::Vacancy::prepare(vacant, &entry).push();
                admit::Cancelled::Submitted
            }
            Self::Completed => admit::Cancelled::Completed,
        }
    }
}

impl<'a, C> admit::Reserve<'a, C, Engine<C>> for Cancellation<'a, C::Token>
where
    C: ring::Context + Context<Native = Uring, Key = Key<C::Token>>,
    C::Tuning: tuning::Progress<C::Token>,
    Engine<C>: 'a,
{
    fn try_new(
        backend: &'a mut Engine<C>,
        key: C::Key,
        pending: bool,
    ) -> Result<Self, admit::Error<Infallible>> {
        if !pending {
            return Ok(Cancellation::Completed);
        }
        let vacant = backend
            .ring
            .submission()
            .try_one()
            .map_err(admit::Error::Full)?;
        Ok(Cancellation::Pending { vacant, key })
    }

    fn try_at(
        backend: &'a mut Engine<C>,
        key: C::Key,
        pending: bool,
        slot: &<C::Inputs as request::Context>::Target,
    ) -> Result<Self, admit::Error<Infallible>> {
        if !pending {
            return Ok(Cancellation::Completed);
        }
        let vacant = backend
            .ring
            .submission()
            .try_one()
            .map_err(admit::Error::Full)?;
        let slot = <Uring as request::Source<C::Inputs>>::target(*slot);
        let Some(stopped) = backend.tuning.cancel_at(slot, Some(key)) else {
            return Ok(Cancellation::Pending { vacant, key });
        };
        let index: <<C::Token as Codec>::Context as token::Context>::Slot =
            bounded::U32::clamp(slot.index() as usize).into();
        Ok(Cancellation::Tuning {
            vacant,
            target: <C::Token as Codec>::Tuning::new(index),
            stopped,
        })
    }
}
