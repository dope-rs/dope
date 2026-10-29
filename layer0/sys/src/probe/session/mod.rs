use crate::{memory, probe::Stopped};
mod checked;
mod closed;
mod create;
mod direct;
mod empty;
mod occupied;
mod pending;
mod primary;
pub use checked::Checked;
pub use closed::Closed;
pub use create::Create;
pub use direct::Direct;
pub use empty::Empty;
pub use occupied::Occupied;
pub use pending::Pending;
pub use primary::Primary;

use crate::{io::Close, probe, ring::map::Unmap};

type Failure<S> = probe::Cause<
    <S as Session>::Probe,
    <<S as Session>::Outcome as probe::Outcome>::Observation,
    <S as Session>::Error,
>;
type Stage<S> = probe::State<S, <S as Session>::Pending>;

/// An idle capability session owns its canary mappings and fixed-file state.
pub trait Session: Close<State: Unmap> + Sized {
    type Ring;
    type Layout: memory::raw::Canary;
    type Owned;
    type Probe: Copy;
    type Outcome: probe::Outcome<Probe = Self::Probe>;
    type Error;
    type Pending: Pending<Session = Self>;

    fn submit(
        self,
        entry: &Self::Owned,
        probe: Self::Probe,
    ) -> Result<Self::Pending, Stopped<Self, Failure<Self>>>;
}
