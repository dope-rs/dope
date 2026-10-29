use std::{cell::Cell, num::NonZero};

use dope_net::link::egress::{
    self,
    data::{self, exec, pair},
    load::lease,
    metadata,
};
use o3::{
    buffer::{hold, pool, segments::scatter, storage, write::consume},
    mem::{self, credit::fair},
};

pub static A: &[u8] = b"a";
pub static B: &[u8] = b"bb";
pub static C: &[u8] = b"ccc";
pub static D: &[u8] = b"dddd";
pub static REJECTED: &[u8] = b"rejected";
pub const LANE: metadata::Lane = metadata::Lane::at(NonZero::<u32>::MIN);

pub struct Restore<'a> {
    pub body: &'a mut Vec<storage::Shared>,
    pub commits: &'a mut Option<usize>,
}

impl mem::Return for Restore<'_> {
    type Item = pair::Source<storage::Shared>;

    fn return_item(self, value: Self::Item) {
        self.body.push(value.into_parts().1);
    }
}

impl lease::Commit for Restore<'_> {
    fn commit(self) {
        *self.commits = self.commits.and_then(|count| count.checked_add(1));
    }
}

pub struct Payload<'a> {
    pub bytes: &'a [u8],
    pub lengths: &'a Cell<Option<usize>>,
    pub charges: &'a Cell<Option<usize>>,
}

// SAFETY: Only observation counters change; the borrowed bytes, length, and charge remain fixed.
unsafe impl<'a: 'd, 'd> data::raw::Payload<'d> for Payload<'a> {
    type Mode = exec::Fallible;
    type Cursor<'b>
        = &'b [u8]
    where
        Self: 'b;

    fn len(&self) -> usize {
        self.lengths
            .set(self.lengths.get().and_then(|count| count.checked_add(1)));
        self.bytes.len()
    }

    fn charge(&self) -> hold::Charge {
        self.charges
            .set(self.charges.get().and_then(|count| count.checked_add(1)));
        hold::Charge::from_owner(self.bytes.len(), 0)
    }

    fn start(&self) -> Self::Cursor<'_> {
        self.bytes
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        offset.tail(self.bytes)
    }

    unsafe fn resume(
        &self,
        _: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: resume requires this offset to lie inside the unchanged
        // slice retained by the complete shared borrow.
        unsafe { self.bytes.get_unchecked(offset.get()..) }
    }
}

#[derive(Debug)]
pub enum Error {
    Empty,
    Extend(consume::Error),
}

pub trait Fill {
    fn try_cursor(&self, bytes: &[u8]) -> Result<pool::Cursor, Error>;
}

impl Fill for pool::Pool {
    fn try_cursor(&self, bytes: &[u8]) -> Result<pool::Cursor, Error> {
        let mut cursor = self.cursor().ok_or(Error::Empty)?;
        cursor.try_append(bytes).map_err(Error::Extend)?.commit();
        Ok(cursor)
    }
}

pub trait Expected {
    fn credit<const ENTRIES: u32, const RESIDENT: u32>(self, free: u32) -> egress::Admission;

    fn capacity<const ENTRIES: u32, const RESIDENT: u32>(
        self,
        charge: egress::Charge,
    ) -> egress::Rejection;
}

impl Expected for metadata::Lane {
    fn credit<const ENTRIES: u32, const RESIDENT: u32>(self, free: u32) -> egress::Admission {
        egress::Admission::Credit {
            lane: self,
            requested: egress::Credit::fixed::<ENTRIES, RESIDENT>(),
            cause: fair::Cause::Exhausted {
                axis: fair::Axis::at::<0>(),
                required: ENTRIES,
                available: free,
            },
        }
    }

    fn capacity<const ENTRIES: u32, const RESIDENT: u32>(
        self,
        charge: egress::Charge,
    ) -> egress::Rejection {
        egress::Rejection::Capacity {
            charge,
            cause: egress::Admission::Storage {
                lane: self,
                requested: egress::Credit::fixed::<ENTRIES, RESIDENT>(),
                available: egress::Entries::new(0),
            },
        }
    }
}
