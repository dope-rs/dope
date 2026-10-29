use std::mem;

use crate::{lookup::error, query::answer::addr, wire::packet};

pub(crate) const MAX_ALIAS_HOPS: usize = 16;
const _: () = assert!(MAX_ALIAS_HOPS <= u8::MAX as usize);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Hops(u8);

impl Hops {
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) const fn get(self) -> usize {
        self.0 as usize
    }

    pub(crate) fn advance(self) -> Result<Self, usize> {
        self.add(Self(1))
    }

    pub(crate) fn add(self, added: Self) -> Result<Self, usize> {
        let actual = self.get() + added.get();
        if actual > MAX_ALIAS_HOPS {
            return Err(actual);
        }
        Ok(Self(actual as u8))
    }
}

pub(crate) enum Reply<'packet, const N: usize> {
    Addresses {
        values: addr::Addresses<N>,
        ttl: u32,
        alias_hops: Hops,
    },
    Alias {
        name: packet::Expanded<'packet>,
        ttl: u32,
        hops: Hops,
    },
    NoData,
    Truncated,
    NameError,
    Refused,
    Server {
        code: u16,
    },
    Capacity {
        actual: usize,
    },
    Malformed(error::Malformed),
}

const _: () = assert!(!mem::needs_drop::<Reply<'static, 1>>());
