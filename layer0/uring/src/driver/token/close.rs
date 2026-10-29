type Invariant<'d, C> = marker::PhantomData<fn(&'d (), C) -> (&'d (), C)>;

use std::marker;

use dope_sys::table::token::{self, host};

use crate::driver::token::{MAX, MIN, PAYLOAD, PROOF, SHIFT};

#[derive(Debug)]
#[repr(transparent)]
pub struct Close<'d, C>(u64, Invariant<'d, C>);

impl<'d, C: host::Context> token::Close<'d> for Close<'d, C> {
    type Context = C;

    fn new(close: &C::Close<'d>) -> Self {
        Self(*close.as_ref(), marker::PhantomData)
    }

    fn retire(retire: &C::Retire<'d>) -> Self {
        use host::Retire as _;
        Self(retire.echo(), marker::PhantomData)
    }

    fn decode(raw: u64) -> Option<C::Returned> {
        unpack(raw).map(C::Returned::from)
    }

    fn raw(self) -> u64 {
        pack(self.0)
    }
}

const fn pack(raw: u64) -> u64 {
    let proof = ((raw & PROOF) >> SHIFT) as u8;
    (((MAX - proof) as u64) << SHIFT) | (raw & PAYLOAD)
}

const fn unpack(raw: u64) -> Option<u64> {
    let tag = (raw >> SHIFT) as u8;
    if tag < MIN || tag > MAX {
        return None;
    }
    let payload = raw & PAYLOAD;
    let proof = ((MAX - tag) as u64) << SHIFT;
    Some(payload | proof)
}

const _: () = {
    assert!(MAX < u8::MAX);
    assert!(MAX - MIN == (PROOF >> SHIFT) as u8);
    assert!(pack(0) == (MAX as u64) << SHIFT);
    assert!(pack(PROOF) == (MIN as u64) << SHIFT);
    assert!(unpack(((MIN as u64) << SHIFT) | PAYLOAD).is_some());
    assert!(unpack(((MAX as u64) << SHIFT) | PAYLOAD).is_some());
    assert!(unpack(PAYLOAD).is_none());
    assert!(unpack((u8::MAX as u64) << SHIFT).is_none());
    assert!(unpack(250_u64 << SHIFT).is_none());
    assert!(unpack(249_u64 << SHIFT).is_none());
    assert!(unpack(248_u64 << SHIFT).is_none());
    assert!(std::mem::size_of::<Close<'static, ()>>() == std::mem::size_of::<u64>());
};
