use crate::conn::stream::state::receive::recv::map;
use o3::depot::arena::linked::fifo;

pub(crate) mod raw;

pub(crate) type Segments = fifo::Fifo<map::Tag>;

const _: () = assert!(usize::BITS == u64::BITS);
