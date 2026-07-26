use dope_core::io::socket::msg::IoVec;

use super::super::EGRESS_QUANTUM;
use super::super::metadata::raw::pool::MetadataPool;
use std::ptr::null;
use std::slice::from_raw_parts;

#[allow(clippy::large_enum_variant)]
pub(crate) enum Entry<B> {
    Retained {
        value: B,
        data: *const u8,
        len: usize,
    },
    Wire {
        data: *const u8,
        len: usize,
    },
    Inline {
        data: [u8; EGRESS_QUANTUM],
        len: u16,
    },
    Static {
        data: *const u8,
        len: usize,
    },
}

pub(crate) enum PreparedEntry {
    Empty,
    Node {
        index: u32,
        bytes: usize,
        resident: usize,
    },
}

impl<B> Entry<B> {
    pub(crate) fn retained(value: B) -> Self {
        Self::Retained {
            value,
            data: null(),
            len: 0,
        }
    }

    pub(crate) fn wire(data: *const u8, len: usize) -> Self {
        Self::Wire { data, len }
    }

    pub(crate) fn inline(src: &[u8]) -> Self {
        let mut data = [0; EGRESS_QUANTUM];
        data[..src.len()].copy_from_slice(src);
        Self::Inline {
            data,
            len: src.len() as u16,
        }
    }

    pub(crate) fn static_bytes(src: &'static [u8]) -> Self {
        Self::Static {
            data: src.as_ptr(),
            len: src.len(),
        }
    }

    pub(crate) fn retained_ref(&self) -> Option<&B> {
        match self {
            Self::Retained { value, .. } => Some(value),
            _ => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Retained { len, .. } | Self::Wire { len, .. } | Self::Static { len, .. } => *len,
            Self::Inline { len, .. } => *len as usize,
        }
    }

    pub(crate) fn wire_len(&self) -> Option<usize> {
        match self {
            Self::Wire { len, .. } => Some(*len),
            _ => None,
        }
    }

    pub(crate) fn iov(&self, offset: usize, cap: usize) -> Option<(IoVec, usize)> {
        let (data, len) = match self {
            Self::Retained { data, len, .. }
            | Self::Wire { data, len }
            | Self::Static { data, len } => (*data, *len),
            Self::Inline { data, len } => (data.as_ptr(), *len as usize),
        };
        if offset >= len {
            return None;
        }
        let available = len - offset;
        let take = available.min(cap);
        let bytes = unsafe { from_raw_parts(data.add(offset), take) };
        Some((IoVec::from_slice(bytes), available))
    }
}

impl<B: AsRef<[u8]>> Entry<B> {
    pub(crate) fn prepare_buffer(pool: &MetadataPool<Self>, value: B) -> Result<PreparedEntry, B> {
        let (index, bytes) = pool.reserve_from(value, Self::retained, Self::prepare_retained)?;
        pool.set_sizes(index, bytes, bytes);
        if bytes == 0 {
            drop(pool.take_node(index));
            return Ok(PreparedEntry::Empty);
        }
        Ok(PreparedEntry::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    pub(crate) fn prepare_wire(
        pool: &MetadataPool<Self>,
        data: *const u8,
        len: usize,
    ) -> Option<PreparedEntry> {
        if len == 0 {
            return Some(PreparedEntry::Empty);
        }
        let (index, bytes) = pool
            .reserve_from((data, len), |(data, len)| Self::wire(data, len), |_| len)
            .ok()?;
        pool.set_sizes(index, bytes, bytes);
        Some(PreparedEntry::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    pub(crate) fn prepare_copy(pool: &MetadataPool<Self>, src: &[u8]) -> Option<PreparedEntry> {
        debug_assert!(!src.is_empty() && src.len() <= EGRESS_QUANTUM);
        let (index, bytes) = pool
            .reserve_from(src, Self::inline, |entry| entry.len())
            .ok()?;
        pool.set_sizes(index, bytes, bytes);
        Some(PreparedEntry::Node {
            index,
            bytes,
            resident: bytes,
        })
    }

    pub(crate) fn prepare_static(
        pool: &MetadataPool<Self>,
        src: &'static [u8],
    ) -> Option<PreparedEntry> {
        if src.is_empty() {
            return Some(PreparedEntry::Empty);
        }
        let (index, bytes) = pool
            .reserve_from(src, Self::static_bytes, |entry| entry.len())
            .ok()?;
        pool.set_sizes(index, bytes, 0);
        Some(PreparedEntry::Node {
            index,
            bytes,
            resident: 0,
        })
    }

    pub(crate) fn prepare_retained(&mut self) -> usize {
        let Self::Retained { value, data, len } = self else {
            return 0;
        };
        let src = value.as_ref();
        *data = src.as_ptr();
        *len = src.len();
        *len
    }
}
