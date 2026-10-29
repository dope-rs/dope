use std::mem;

use dope_sys::io::reactor::complete;

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

impl From<Value> for bounded::U32<0, { i32::MAX as u32 }> {
    fn from(value: Value) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Value(bounded::U32<0, { i32::MAX as u32 }>);

impl Value {
    pub(crate) const fn into_outcome(self) -> Outcome {
        Outcome::new(self.0.get() as i32)
    }
}

impl complete::Value for Value {
    fn is_zero(self) -> bool {
        self.0.get() == 0
    }
}

const _: () = {
    assert!(mem::size_of::<bounded::U32<0, { i32::MAX as u32 }>>() == mem::size_of::<u32>());
    assert!(mem::align_of::<bounded::U32<0, { i32::MAX as u32 }>>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Value>() == mem::size_of::<u32>());
};
