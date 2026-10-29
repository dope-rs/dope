use std::{cell::Cell, mem};

use dope_sys::{
    io::reactor::complete,
    ring::Source,
    table::token::{self, host},
};
use dope_uring::Uring;

use crate::common::cleanup;

type Entry = <Uring as Source>::Entry<()>;
type Outcome = <Entry as complete::Entry>::Outcome;
type Returned = <Outcome as complete::Outcome>::Returned;
type Parts = complete::Parts<u64, Outcome, <Entry as complete::Entry>::Flags>;
type Token = <Uring as token::Source>::Token<cleanup::Host>;

pub struct Lease<'a>(pub &'a Cell<usize>);

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

pub struct Echo<'a>(pub &'a Cell<usize>, pub &'a Cell<usize>);

impl host::Echo for Echo<'_> {
    fn complete(self) {
        self.0.set(self.0.get() + 1);
    }

    fn retry(self) {
        self.1.set(self.1.get() + 1);
    }
}

type Flags = <Entry as complete::Entry>::Flags;

pub fn parts(flags: u32) -> Parts {
    complete::Parts {
        token: 7,
        outcome: outcome(-libc::EIO),
        flags: self::flags(flags),
    }
}

pub fn outcome(raw: i32) -> Outcome {
    // SAFETY: the native result is transparent over i32 and admits every kernel word.
    unsafe { mem::transmute(raw) }
}

pub fn rejected(value: Returned) -> i32 {
    // SAFETY: Returned preserves the transparent native result without changing its bits.
    unsafe { mem::transmute(value) }
}

pub fn flags(bits: u32) -> Flags {
    // SAFETY: Flags is transparent over u32 and admits every kernel bit pattern.
    unsafe { mem::transmute(bits) }
}

pub fn token(raw: u64) -> Token {
    // SAFETY: the native token is transparent over u64 and admits every kernel word.
    unsafe { mem::transmute(raw) }
}
