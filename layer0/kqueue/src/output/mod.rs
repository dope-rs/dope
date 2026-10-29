pub(crate) use data::Data;
use dope_sys::{io::reactor::retry::Context, table::change};
use kind::Kind;
pub(crate) use pending::Pending;
pub(crate) use record::Record;
pub(crate) use retry::Retry;
pub(crate) use state::{State, removal::Removal, vacancy::Vacancy};

use crate::store::change::Id;

mod kind;
mod pending;
mod record;
mod state;

type File<C> = Id<<<C as Context>::Slots as change::raw::Slots>::Physical>;

mod data;
mod retry;

mod raw;
