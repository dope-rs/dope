use dope_sys::{
    io::reactor::ready::{self, store},
    table,
    table::change,
};
pub(crate) use engine::Engine;
pub(crate) use entry::event::Event;
pub(crate) use guards::Guard;
pub(crate) use state::State;
pub(crate) use trace::Trace;
pub(crate) use turn::{Dispatch, Work};
pub(crate) use watch::Poll;
use watch::{Events, Kernel};

mod engine;
mod entry;
mod guards;
pub(crate) mod input;
mod state;
mod trace;
mod turn;
mod watch;

const CAPACITY: usize = 64;

type Key<C> = <<C as ready::Driver>::Context as store::Exec>::Key;
type Slot<C> =
    <<<C as ready::Driver>::Context as store::Context>::Slots as change::raw::Slots>::Physical;
type Slots<C> = <<C as ready::Driver>::Context as store::Context>::Slots;
type Owned<C> = <Slots<C> as table::raw::Slots>::Owned;
