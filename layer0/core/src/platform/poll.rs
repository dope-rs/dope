use std::os::fd;

use dope_sys::io::{poll, poll::Poll as _};
use o3::alloc::{self, VecExt};

use crate::{Backend, Errno};

/// Owns readiness storage while borrowing every registered descriptor.
pub struct Set<'fd> {
    entries: Vec<poll::Entry<'fd>>,
}

impl<'fd> Set<'fd> {
    pub fn try_new(
        first: impl Iterator<Item = fd::BorrowedFd<'fd>>,
        second: impl Iterator<Item = fd::BorrowedFd<'fd>>,
    ) -> Result<Self, alloc::Error> {
        let sources = first.chain(second);
        let mut entries = Vec::try_new(sources.size_hint().0)?;
        for source in sources {
            alloc::Vacant::try_new(&mut entries)?.insert(poll::Entry::readable(source));
        }
        if poll::nfds_t::try_from(entries.len()).is_err() {
            return Err(alloc::Error::Overflow);
        }
        Ok(Self { entries })
    }

    /// Borrows the exact first ready registration for handling or retirement.
    pub fn poll(&mut self, timeout: i32) -> Result<Option<Ready<'_, 'fd>>, Errno> {
        if Backend::wait::<_, Errno>(self, timeout)?.count() == 0 {
            return Ok(None);
        }
        Ok(self
            .entries
            .iter_mut()
            .enumerate()
            .find_map(|(index, entry)| entry.is_ready().then_some(Ready { index, entry })))
    }
}

/// Exclusive handling authority for one ready registration in its original set.
pub struct Ready<'set, 'fd> {
    index: usize,
    entry: &'set mut poll::Entry<'fd>,
}

impl Ready<'_, '_> {
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Retires this registration without changing any other registration's index.
    pub fn retire(self) {
        self.entry.retire();
    }
}

// SAFETY: Set construction rejects any length exceeding nfds_t and its private
// storage cannot grow after construction.
unsafe impl<'fd> poll::raw::Entries<'fd> for Set<'fd> {
    const MAX: usize = poll::MAX_ENTRIES;
    fn entries(&mut self) -> &mut [poll::Entry<'fd>] {
        &mut self.entries
    }
}
