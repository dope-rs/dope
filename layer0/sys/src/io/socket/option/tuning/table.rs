use std::pin;

use crate::io::socket::option::tuning;

/// Address-stable option states indexed by physical descriptor slots.
pub trait Table<E: Copy, const N: usize>: tuning::Create + tuning::Quiesce {
    type State: tuning::raw::State<E, N>;
    type Occupied<'a>: tuning::Occupied<E>
    where
        Self: 'a;

    fn bind(&mut self, index: u32) -> Result<Self::Occupied<'_>, tuning::Missing>;
    fn get(&mut self, index: u32) -> Option<pin::Pin<&mut Self::State>>;

    /// Makes a failed cancellation retryable without releasing its chain.
    /// A late reply may only permit another attempt; it cannot clear a target.
    fn retry_cancel(&mut self, index: u32);
}
