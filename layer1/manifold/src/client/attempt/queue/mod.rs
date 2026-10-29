pub mod storage;

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::event;
use o3::depot::{self, index::Initialized};

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
    pub(in crate::client) fn into_event(self) -> (attempt::Id<'d, ID>, event::AttemptError) {
        match self {
            Self::Key(cause) => (
                attempt::Id { handle: cause.key },
                event::AttemptError::Lookup(cause.observed),
            ),
            Self::Finished(key) => (key, event::AttemptError::Finished),
        }
    }
}
