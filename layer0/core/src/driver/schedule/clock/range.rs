use o3::base::time::DurationExt as _;
use std::{borrow, time};

use crate::driver::schedule::clock::Origin;

/// Exact future deadline that exceeds the platform monotonic range.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Range<D = time::Duration> {
    origin: Origin,
    duration: D,
}

impl<D: Copy> Range<D> {
    pub(super) const fn new(origin: Origin, duration: D) -> Self {
        Self { origin, duration }
    }

    pub const fn duration(self) -> D {
        self.duration
    }

    #[cold]
    pub(super) fn furthest(self) -> time::Instant
    where
        D: borrow::Borrow<time::Duration>,
    {
        let mut lower = time::Duration::ZERO;
        let mut upper = *self.duration.borrow();
        let mut at = self.origin.0;
        while lower < upper {
            let middle = lower.average::<2>(upper);
            let next = if middle == lower { upper } else { middle };
            match self.origin.0.checked_add(next) {
                Some(value) => {
                    lower = next;
                    at = value;
                }
                None => {
                    let nanos = next.subsec_nanos();
                    upper = if nanos == 0 {
                        time::Duration::new(next.as_secs() - 1, 999_999_999)
                    } else {
                        time::Duration::new(next.as_secs(), nanos - 1)
                    };
                }
            }
        }
        at
    }
}

const _: () = {
    use std::mem;

    use o3::base::time::Positive;

    assert!(mem::size_of::<Range<Positive>>() == mem::size_of::<Range>());
    assert!(mem::align_of::<Range<Positive>>() == mem::align_of::<Range>());
    assert!(!mem::needs_drop::<Range<Positive>>());
};
