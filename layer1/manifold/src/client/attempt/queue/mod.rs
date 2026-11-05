pub mod storage;

use dope_core::driver::route::{Identity, KeyTag};
use o3::depot::{self, arena::slab::key, index::Initialized};

use crate::client::attempt;

type Held<'a, 'd, T, ID> = depot::raw::Proof<
    (&'a table::Table<'d, T, ID>, attempt::Id<'d, ID>),
    Initialized<entry::Pending>,
>;

mod control;
mod entry;
mod lease;
pub mod mailbox;
mod reserve;
mod source;
pub(super) mod table;

pub use control::Control;
pub use lease::Lease;
pub use reserve::Reserve;
pub use source::Source;

/// The exact failed access to an attempt generation.
pub enum Lookup<'d, ID: Identity = KeyTag<0>> {
    Key(storage::Error<attempt::Tag<'d, ID>>),
    Finished(attempt::Id<'d, ID>),
}

impl<'d, ID: Identity> Lookup<'d, ID> {
    pub(in crate::client) fn is_stale(&self) -> bool {
        match self {
            Self::Key(storage::Error {
                observed:
                    key::Observation::Free { .. }
                    | key::Observation::Generation { .. }
                    | key::Observation::Retired { .. },
                ..
            }) => true,
            Self::Key(storage::Error {
                key,
                observed: key::Observation::Reserved { actual },
            }) => *actual != key.generation(),
            _ => false,
        }
    }

    pub(in crate::client) fn into_event(self) -> (attempt::Id<'d, ID>, attempt::AttemptError) {
        match self {
            Self::Key(cause) => (
                attempt::Id { handle: cause.key },
                attempt::AttemptError::Lookup(cause.observed),
            ),
            Self::Finished(key) => (key, attempt::AttemptError::Finished),
        }
    }
}
