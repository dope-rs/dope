pub mod key;
pub mod prefix;
pub mod seccomp;

use dope_sys::{
    io::reactor::complete::{self, Decode, Flags as _, Outcome as _},
    ring::{
        Source,
        create::{self, Create},
    },
};
use dope_uring::Uring;
use o3::base::num::bounded;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Token(pub u64);

type Owner = <Uring as Source>::Ring<Token>;
type Setup =
    create::Error<<Owner as Create>::Setup, <Owner as Create>::Map, <Owner as Create>::Cleanup>;
type Entry = <Uring as Source>::Entry<Token>;
type Outcome = <Entry as complete::Entry>::Outcome;
type Value = <Outcome as complete::Outcome>::Value;
type Invalid = <Outcome as complete::Outcome>::Invalid;
type Flags = <Entry as complete::Entry>::Flags;
type Meta = <Flags as complete::Flags>::Meta;

pub trait Inspect {
    fn try_inspect(
        self,
        check: impl FnOnce(Token, Decode<Value, Invalid>, Meta),
    ) -> Result<(), <Flags as complete::Flags>::Invalid>;
}

impl Inspect for complete::Parts<Token, Outcome, Flags> {
    fn try_inspect(
        self,
        check: impl FnOnce(Token, Decode<Value, Invalid>, Meta),
    ) -> Result<(), <Flags as complete::Flags>::Invalid> {
        let meta = self.flags.decode()?;
        check(self.token, self.outcome.decode(), meta);
        Ok(())
    }
}

pub trait Ring {
    fn try_ring<const SQ: u32, const CQ: u32>(defer: bool) -> Result<Owner, Setup>;
}

impl Ring for Uring {
    fn try_ring<const SQ: u32, const CQ: u32>(defer: bool) -> Result<Owner, Setup> {
        try_create::<Token, SQ, CQ>(defer)
    }
}

pub fn try_create<T: complete::raw::Token, const SQ: u32, const CQ: u32>(
    defer: bool,
) -> Result<<Uring as Source>::Ring<T>, Setup> {
    <<Uring as Source>::Ring<T> as Create>::new(
        bounded::U32::<1, { u32::MAX }>::exact::<SQ>(),
        bounded::U32::<1, { u32::MAX }>::exact::<CQ>(),
        defer,
    )
}

// SAFETY: Token is a transparent u64 admitting every kernel-returned bit pattern.
unsafe impl complete::raw::Token for Token {}

pub const CAPACITY: u32 = 8;
pub const BATCH_WIDTH: u32 = 5;
pub const BATCHES: u64 = 3;
