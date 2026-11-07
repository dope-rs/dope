use std::num::NonZero;

use o3::{alloc, buffer::write};

use crate::stream::outbound::sender::Range;
use crate::{
    conn::Appended,
    stream::{outbound::Finish, send},
    wire::{stream::Id, varint},
};

/// One selected prefix and the remaining admitted source interval.
pub(crate) struct Prepared {
    range: Range,
    pending: varint::Span,
}

/// The original selection after its complete wire image was admitted.
pub(crate) struct Written {
    pub(super) pending: varint::Span,
    wire_len: NonZero<u16>,
    bytes: u16,
    pub(super) boundary: send::Boundary,
}

#[derive(Clone, Copy)]
pub(crate) struct View {
    pub(super) pending: varint::Span,
    pub(super) finish: Finish,
}

impl View {
    pub(crate) const fn next_offset(self) -> varint::VarInt {
        self.pending.start()
    }

    pub(crate) const fn unsent_len(self) -> usize {
        self.pending.len() as usize
    }

    pub(crate) const fn has_pending(self) -> bool {
        self.pending.len() != 0 || matches!(self.finish, Finish::Marked)
    }

    pub(crate) const fn boundary(self, take: usize) -> send::Boundary {
        if take >= self.unsent_len() && matches!(self.finish, Finish::Marked) {
            send::Boundary::End
        } else {
            send::Boundary::More
        }
    }

    pub(crate) fn prepare(self, limit: usize) -> Prepared {
        let (prefix, pending) = self.pending.split_prefix(limit as u64);
        let boundary = self.boundary(prefix.len() as usize);
        let mark = match boundary {
            send::Boundary::More => 0,
            send::Boundary::End => Range::FIN,
        };
        Prepared {
            range: Range {
                encoded_offset: prefix.start().get() | mark,
                end: prefix.end(),
            },
            pending,
        }
    }
}

impl Prepared {
    pub(crate) const fn range(&self) -> Range {
        self.range
    }

    pub(crate) fn write(
        self,
        out: &mut write::Append<'_>,
        limit: usize,
        id: Id,
        copy: impl FnOnce(Range, &mut write::Append<'_>) -> Result<Option<Appended>, alloc::Error>,
    ) -> Result<Option<Written>, alloc::Error> {
        let Some((wire_len, bytes)) = self.range.write(out, limit, id, copy)? else {
            return Ok(None);
        };
        Ok(Some(Written {
            pending: self.pending,
            wire_len,
            bytes,
            boundary: self.range.boundary(),
        }))
    }
}

impl Written {
    pub(crate) const fn len(&self) -> usize {
        self.bytes as usize
    }

    pub(crate) const fn wire_len(&self) -> NonZero<u16> {
        self.wire_len
    }

    pub(crate) const fn range(&self) -> Range {
        let mark = match self.boundary {
            send::Boundary::More => 0,
            send::Boundary::End => Range::FIN,
        };
        Range {
            encoded_offset: (self.pending.start().get() - self.bytes as u64) | mark,
            end: self.pending.start(),
        }
    }
}
