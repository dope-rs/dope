use o3::{base::num::bounded, depot::capacity};

use crate::io::{
    reactor::{
        Full, complete,
        submit::{self, Ready},
    },
    socket::option::tuning,
};

/// An exclusive view of locally staged native submissions.
pub trait Queue<'q, T: complete::raw::Token>: Sized {
    type Owned;
    type Vacancy: submit::raw::Vacancy<'q, T, Owned = Self::Owned>;
    type Reserved<const N: usize>: tuning::raw::Chain<T, N>;
    type Prepared<'e, E: 'e, const N: usize>: Ready;

    fn refresh(&mut self);
    fn len(&self) -> bounded::U32<0, { u32::MAX }>;
    fn is_empty(&self) -> bool;
    fn try_one(self) -> Result<Self::Vacancy, Full>;
    /// Acquires the current kernel head without publishing local submissions.
    /// A full queue leaves both cached and shared queue state unchanged.
    fn try_acquire(self) -> Result<Self::Vacancy, Full>;
    fn try_reserve<const N: usize>(
        self,
        count: capacity::Limit<N>,
    ) -> Result<Self::Reserved<N>, Full>;
    fn prepare<'e, const N: usize>(
        self,
        entries: &'e [Self::Owned; N],
    ) -> Result<Self::Prepared<'e, Self::Owned, N>, Full>;
}
