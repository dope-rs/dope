pub(crate) mod query;
pub(crate) mod response;

pub(crate) const A: u16 = 1;
pub(crate) const AAAA: u16 = 28;
const CNAME: u16 = 5;
const OPT: u16 = 41;
const IN: u16 = 1;
const HEADER_LEN: usize = 12;
const MAX_POINTER_HOPS: usize = 32;
pub(crate) const MAX_CNAME_DEPTH: usize = 16;
const MAX_CNAME_RECORDS: usize = 32;
const EDNS_PAYLOAD: u16 = 1232;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct TransactionId(pub(crate) u16);

impl TransactionId {
    pub(crate) const fn from_wire(id: u16) -> Self {
        Self(id)
    }

    pub(crate) const fn wire(self) -> u16 {
        self.0
    }

    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }
}

impl set::DenseIndex for TransactionId {
    fn into_usize(self) -> usize {
        self.0 as usize
    }

    fn from_usize(raw: usize) -> Self {
        Self::from_wire(raw as u16)
    }
}

const _: () = assert!(std::mem::size_of::<TransactionId>() == std::mem::size_of::<u16>());
const _: () = {
    assert!(TransactionId::from_wire(u16::MIN).wire() == u16::MIN);
    assert!(TransactionId::from_wire(u16::MAX).wire() == u16::MAX);
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rcode {
    Format,
    Server,
    Name,
    NotImplemented,
    Refused,
    Other(u8),
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "decoded aliases move their bounded name directly into query state without allocation"
)]
pub(crate) enum Outcome<const N: usize> {
    Addresses {
        values: crate::Addresses<N>,
        ttl: u32,
        alias_hops: u8,
    },
    Alias {
        name: crate::Name,
        ttl: u32,
        hops: u8,
    },
    NoData,
    Truncated,
    Failed(Rcode),
}
use o3::collections::batch::set;
