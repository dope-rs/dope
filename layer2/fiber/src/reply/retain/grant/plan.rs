use std::{array, num::NonZero};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reply) struct Plan<const N: usize> {
    pub(in crate::reply::retain) shared: [usize; N],
    pub(in crate::reply::retain) reserve: [usize; N],
}

impl Plan<1> {
    /// Orders retire before their entries and need no separate credit.
    pub(in crate::reply) fn slots(entries: NonZero<u32>, lanes: NonZero<u32>) -> Self {
        let shared = Self::entry_limit(entries.get() as usize, lanes) - 1;
        Self {
            shared: [shared],
            reserve: [1],
        }
    }
    pub(in crate::reply) fn entry_limit(entries: usize, lanes: NonZero<u32>) -> usize {
        entries - lanes.get() as usize + 1
    }

    /// Retains the one reserved order and the shared amount below u32::MAX.
    pub(in crate::reply) const fn order_limit(self) -> NonZero<u32> {
        NonZero::<u32>::MIN.saturating_add(self.shared[0] as u32)
    }
}

impl Plan<3> {
    pub(in crate::reply) fn capacity(self, lanes: NonZero<u32>, axis: usize) -> usize {
        self.shared[axis] + self.reserve[axis] * lanes.get() as usize
    }

    pub(in crate::reply) fn payload(capacity: [usize; 3], lanes: NonZero<usize>) -> Self {
        let reserve = capacity.map(|amount| {
            if lanes.get() == 1 {
                amount
            } else {
                amount / lanes.get() / 2
            }
        });
        Self {
            shared: array::from_fn(|axis| capacity[axis] - reserve[axis] * lanes.get()),
            reserve,
        }
    }

    pub(in crate::reply) fn ceiling(self) -> [usize; 3] {
        array::from_fn(|axis| self.shared[axis] + self.reserve[axis])
    }
}

const _: () = {
    assert!(size_of::<Plan<1>>() == 2 * size_of::<usize>());
    assert!(size_of::<Plan<3>>() == 6 * size_of::<usize>());
};
