use std::mem;

mod entry;
mod flags;
pub(crate) use flags::{Buffer, Flags, Meta};
mod message;
pub(crate) use message::{Capacity, Length, Output};
mod outcome;
pub(crate) mod packet;
pub(crate) use outcome::{Invalid, Outcome};
mod queue;
pub(crate) mod raw;

pub(crate) use entry::Entry;
use o3::base::num::bounded;
pub(crate) use queue::Queue;

pub(crate) type Value = bounded::U32<0, { i32::MAX as u32 }>;

const _: () = {
    assert!(mem::size_of::<bounded::U32<0, { i32::MAX as u32 }>>() == mem::size_of::<u32>());
    assert!(mem::align_of::<bounded::U32<0, { i32::MAX as u32 }>>() == mem::align_of::<u32>());
};
