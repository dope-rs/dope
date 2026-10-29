use std::{marker, mem};

use cq::{Flags, Outcome};
use dope_sys::io::reactor::complete::{self, Parts};

use crate::reactor::{cq, sq};

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct Entry<T> {
    user_data: u64,
    result: i32,
    flags: u32,
    marker: marker::PhantomData<fn(T) -> T>,
}

impl<T> complete::Entry for Entry<T> {
    type Token = T;
    type Outcome = Outcome;
    type Flags = Flags;

    fn into_parts(self) -> Parts<T, Outcome, Flags>
    where
        T: complete::raw::Token,
    {
        use sq::raw::Word;

        Parts {
            token: Word::new(self.user_data).decode(),
            outcome: Outcome::new(self.result),
            flags: Flags::new(self.flags),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Parts<u64, Outcome, Flags>>() == 2 * mem::size_of::<u64>());
    assert!(mem::align_of::<Parts<u64, Outcome, Flags>>() == mem::align_of::<u64>());
    assert!(mem::size_of::<Entry<()>>() == 16);
    assert!(mem::align_of::<Entry<()>>() == 8);
    assert!(mem::offset_of!(Entry<()>, user_data) == 0);
    assert!(mem::offset_of!(Entry<()>, result) == 8);
    assert!(mem::offset_of!(Entry<()>, flags) == 12);
};
