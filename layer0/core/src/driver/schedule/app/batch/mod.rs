use std::num::NonZero;

use o3::{
    base::cell::region,
    mem::{self, lease},
};

use crate::driver::{
    schedule,
    schedule::{app, app::batch, state::lanes},
};

pub mod raw;

/// Quota reserved for one batch before its actual size is known.
#[must_use = "dropping a batch reservation returns all of its quota"]
pub struct Reserved<'turn, 'd, T> {
    quota: lanes::Quota<'turn, 'd, lease::Open>,
    limit: T,
}

/// The batch bound carrying the identity of its reservation.
#[must_use = "a batch bound must be converted into an unused proof"]
pub struct Bound<'id, T> {
    limit: T,
    token: region::Token<region::Identity<'id>>,
}

impl<'turn, 'd, T: batch::raw::Limit> Reserved<'turn, 'd, T> {
    pub fn reserve(work: app::Work<'turn, 'd>, limit: T, prior: bool) -> schedule::Capacity<Self> {
        let reserved = limit.get().get() - 1 + usize::from(prior);
        match lanes::Quota::reserve_app(work, reserved) {
            schedule::Capacity::Available(quota) => {
                schedule::Capacity::Available(Self { quota, limit })
            }
            schedule::Capacity::Exhausted => schedule::Capacity::Exhausted,
        }
    }

    /// Opens a fresh identity and requires its exact unused proof back.
    ///
    /// The proof cannot escape the operation or cross reservations:
    ///
    pub fn with<C, R>(
        self,
        context: C,
        apply: impl for<'id> FnOnce(C, Bound<'id, T>) -> (R, mem::raw::Unused<'id>),
    ) -> R {
        let Self {
            quota: reserved,
            limit,
        } = self;
        reserved
            .inner
            .with((context, limit), |(context, limit), token| {
                apply(context, Bound { limit, token })
            })
    }
}

impl<'id, T: batch::raw::Limit> Bound<'id, T> {
    pub fn get(&self) -> &T {
        &self.limit
    }

    pub fn empty(self) -> mem::raw::Unused<'id> {
        // SAFETY: every unit beyond the implicit first item remains unused
        // under this bound's original reservation identity.
        unsafe { mem::raw::Unused::new(self.limit.get().get() - 1, self.token) }
    }

    fn into_parts(self) -> (T, region::Token<region::Identity<'id>>) {
        (self.limit, self.token)
    }
}

const _: () = {
    let word = size_of::<usize>();
    assert!(size_of::<Reserved<'static, 'static, NonZero<usize>>>() == 3 * word);
    assert!(size_of::<Bound<'static, NonZero<usize>>>() == word);
};
