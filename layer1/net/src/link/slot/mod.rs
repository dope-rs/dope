use dope_core::driver::receive::credit::error::Arm;

use crate::{
    link::duplex,
    wire::{self, receive, send},
};

mod io;
pub(in crate::link) mod recv;
pub mod transmit;
pub mod types;

pub use io::Io;

type Engine<'d, ID, W> = duplex::Duplex<
    'd,
    ID,
    <<W as wire::Spec>::Receive as receive::Policy<W>>::Discard,
    <<W as wire::Spec>::Send as send::Policy<W>>::Reclaim,
>;

pub(in crate::link) enum Decision<C> {
    Control(Control),
    Chunk { chunk: C, needs_rearm: bool },
}

pub(in crate::link) enum Control {
    Eof,
    Cancelled,
    Failed(dope_core::Errno),
    Credit { cause: Arm, needs_rearm: bool },
    NoChunk { needs_rearm: bool },
    Discarded { needs_rearm: bool, finished: bool },
}
