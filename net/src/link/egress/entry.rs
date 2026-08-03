use dope_core::io::socket::msg::IoVec;
use o3::cell::RegionToken;

use super::EGRESS_QUANTUM;
use super::StableBytes;
use super::metadata::pool::{Pool, ReservedIndex};
use super::wire::Span;

#[allow(clippy::large_enum_variant)]
pub(in crate::link::egress) enum Entry<B> {
    Retained(B),
    Wire(Span),
    Inline {
        data: [u8; EGRESS_QUANTUM],
        len: u16,
    },
    Static(&'static [u8]),
}

pub(in crate::link::egress) enum PreparedEntry {
    Empty,
    Node {
        index: ReservedIndex,
        bytes: usize,
        resident: usize,
    },
}

impl<B> Entry<B> {
    pub(in crate::link::egress) fn retained_ref(&self) -> Option<&B> {
        match self {
            Self::Retained(value) => Some(value),
            _ => None,
        }
    }

    pub(in crate::link::egress) fn wire_span(&self) -> Option<Span> {
        match self {
            Self::Wire(span) => Some(*span),
            _ => None,
        }
    }

    pub(in crate::link::egress) fn iov(
        bytes: &[u8],
        offset: usize,
        cap: usize,
    ) -> Option<(IoVec, usize)> {
        let bytes = bytes.get(offset..)?;
        let available = bytes.len();
        let take = available.min(cap);
        Some((IoVec::from_slice(&bytes[..take]), available))
    }
}

impl<B: StableBytes> Entry<B> {
    pub(in crate::link::egress) fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Retained(value) => Some(value.as_ref()),
            Self::Wire(_) => None,
            Self::Inline { data, len } => Some(&data[..*len as usize]),
            Self::Static(bytes) => Some(bytes),
        }
    }

    pub(in crate::link::egress) fn prepare_buffer<'d>(
        pool: &Pool<'d, Self>,
        token: &mut RegionToken<'d>,
        value: B,
    ) -> Result<PreparedEntry, B> {
        let bytes = value.as_ref().len();
        if bytes == 0 {
            return Ok(PreparedEntry::Empty);
        }
        let index = match pool.reserve(token, Self::Retained(value), bytes, bytes) {
            Ok(index) => index,
            Err(Self::Retained(value)) => return Err(value),
            Err(_) => unreachable!(),
        };
        Ok(PreparedEntry::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    pub(in crate::link::egress) fn prepare_wire<'d>(
        pool: &Pool<'d, Self>,
        token: &mut RegionToken<'d>,
        span: Span,
    ) -> Option<PreparedEntry> {
        let bytes = span.len();
        if bytes == 0 {
            return Some(PreparedEntry::Empty);
        }
        let index = pool.reserve(token, Self::Wire(span), bytes, bytes).ok()?;
        Some(PreparedEntry::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    pub(in crate::link::egress) fn prepare_copy<'d>(
        pool: &Pool<'d, Self>,
        token: &mut RegionToken<'d>,
        src: &[u8],
    ) -> Option<PreparedEntry> {
        debug_assert!(!src.is_empty() && src.len() <= EGRESS_QUANTUM);
        let bytes = src.len();
        let index = pool.reserve(token, Self::inline(src), bytes, bytes).ok()?;
        Some(PreparedEntry::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    pub(in crate::link::egress) fn prepare_static<'d>(
        pool: &Pool<'d, Self>,
        token: &mut RegionToken<'d>,
        src: &'static [u8],
    ) -> Option<PreparedEntry> {
        if src.is_empty() {
            return Some(PreparedEntry::Empty);
        }
        let bytes = src.len();
        let index = pool.reserve(token, Self::Static(src), bytes, 0).ok()?;
        Some(PreparedEntry::Node {
            index,
            bytes,
            resident: 0,
        })
    }

    fn inline(src: &[u8]) -> Self {
        let mut data = [0; EGRESS_QUANTUM];
        data[..src.len()].copy_from_slice(src);
        Self::Inline {
            data,
            len: src.len() as u16,
        }
    }
}
