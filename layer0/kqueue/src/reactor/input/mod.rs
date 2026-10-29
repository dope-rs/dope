pub(crate) use arm::Arm;
use dope_sys::{io::reactor::read, table::change};
pub(crate) use entry::{Call, Slot};
pub(crate) use poll::Poll;

use crate::store::change::Id;

mod arm;
pub(crate) mod entry;

type File<C> = Id<<<C as read::Context>::Slots as change::raw::Slots>::Physical>;

mod poll;

mod raw;
