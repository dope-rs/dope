use crate::driver::route;

/// Words in one route set; its bit count equals the complete route namespace.
const WORDS: usize = 4;

/// The non-framework route identities owned by one component or topology.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Routes([u64; WORDS]);

impl Routes {
    pub const EMPTY: Self = Self([0; WORDS]);

    /// Builds the route set holding exactly one application identity.
    pub const fn singleton<T: route::App>() -> Self {
        Self::singleton_id(T::ID)
    }

    /// Builds a route set from a value-level ID for generated aggregates.
    pub const fn singleton_id(id: route::Id) -> Self {
        let mut words = [0; WORDS];
        let word = id.raw() as usize / u64::BITS as usize;
        let bit = id.raw() as usize % u64::BITS as usize;
        words[word] |= 1u64 << bit;
        Self(words)
    }

    #[must_use]
    pub const fn has_route(self, route: route::Route) -> bool {
        let Some(id) = route.app() else {
            return false;
        };
        let word = id.raw() as usize / u64::BITS as usize;
        let bit = id.raw() as usize % u64::BITS as usize;
        self.0[word] & (1u64 << bit) != 0
    }

    /// Finds an overlapping identity inside the complete, u8-bounded route bitset.
    pub(in crate::driver::settings) const fn overlap(self, other: Self) -> Option<route::Id> {
        const { assert!(WORDS * (u64::BITS as usize) == route::CAPACITY) };
        let mut word = 0;
        while word < WORDS {
            let overlap = self.0[word] & other.0[word];
            if overlap != 0 {
                let base = (word as u8) * (u64::BITS as u8);
                return route::Route::from_bits(base + overlap.trailing_zeros() as u8).app();
            }
            word += 1;
        }
        None
    }

    /// Joins application route ownership as an idempotent set union.
    pub const fn join(self, other: Self) -> Self {
        Self([
            self.0[0] | other.0[0],
            self.0[1] | other.0[1],
            self.0[2] | other.0[2],
            self.0[3] | other.0[3],
        ])
    }
}
