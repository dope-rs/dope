mod context;
mod create;
mod effects;
mod event;
mod resolved;
mod retired;
mod socket;
mod source;

pub use context::Context;
pub use create::Create;
pub use effects::Effects;
pub use event::Event;
pub use resolved::Resolved;
pub use retired::Retired;
pub use socket::Socket;
pub use source::Source;

use crate::{
    io::reactor::{read, retry},
    table,
    table::change,
};

type Key<C> = <<C as Context>::Reader as read::Context>::Key;
type Flow<C> = <<C as Context>::Reader as read::Context>::Flow;
type Slots<C> = <<C as Context>::Reader as read::Context>::Slots;
type Physical<C> = <Slots<C> as change::raw::Slots>::Physical;
type Accepted<C> = <Slots<C> as table::raw::Slots>::Accepted;
type Bytes<C> = <<C as Context>::Writer as retry::Context>::Outcome;
type Unit<C> = <<C as Context>::Writer as retry::Context>::Unit;

type Owned<C> = <Slots<C> as table::raw::Slots>::Owned;
