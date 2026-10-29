use std::{marker, mem};

use dope_core::io::transfer;
use o3::buffer::{bytes::bounded::Overflow, storage};

use crate::link::egress::reserve::demand::{Body, Prefix};

/// Exact non-empty final-link demand produced before an owner moves.
pub struct Plan<B, const PREFIX: usize> {
    wire: transfer::NonZero,
    owner: marker::PhantomData<fn(B) -> B>,
}

impl<B, const PREFIX: usize> Plan<B, PREFIX> {
    pub fn prefix(prefix: Prefix<PREFIX>) -> Result<Self, storage::Zero> {
        let wire = transfer::NonZero::try_from(prefix.len())?;
        Ok(Self {
            wire,
            owner: marker::PhantomData,
        })
    }

    pub fn with_body(prefix: Prefix<PREFIX>, body: Body<B>) -> Result<Self, Overflow> {
        let wire = body.len.checked_add(prefix.len())?;
        Ok(Self {
            wire,
            owner: marker::PhantomData,
        })
    }

    pub const fn wire_len(&self) -> transfer::NonZero {
        self.wire
    }
}

const _: () = {
    assert!(mem::size_of::<Plan<(), 1>>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Plan<(), 1>>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Option<Plan<(), 1>>>() == mem::size_of::<Plan<(), 1>>());
};
