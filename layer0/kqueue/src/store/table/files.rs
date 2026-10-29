use std::os::fd::{self, AsFd as _, AsRawFd as _};

use dope_sys::{
    io::{fd::Fd as _, socket::Prepare as _},
    table::{
        self, Error, Install,
        change::{self, Changes as _, Filter, Id as _, Index},
        closed,
    },
};
use o3::{
    alloc,
    depot::{
        capacity,
        index::{self, bitmap, set},
    },
};

use crate::{
    Kqueue,
    sockets::Handle,
    store::change::{Changes, Id, Interest},
};

/// A descriptor and its write interest borrowed together from one file table.
pub(crate) type Output<'a, S> = (fd::BorrowedFd<'a>, Interest<'a, S, { libc::EVFILT_WRITE }>);

pub struct Files<S: table::raw::Slots> {
    table: Box<[Option<Handle>]>,
    accepted: S::Capacity,
    available: set::State,
    capacity: S::Capacity,
    changes: Changes<S>,
}

impl<S: table::raw::Slots> table::Files<S> for Files<S> {
    type Handle = Handle;
    type Changes = Changes<S>;

    fn try_new(layout: S::Layout) -> Result<Self, alloc::Error> {
        let (capacity, accepted) = layout.into();
        let changes = Changes::try_new(capacity)?;
        let count = S::count(capacity);
        let table = alloc::BoxSliceExt::try_default(count)?;
        let domain = capacity::Count::new(S::count(accepted) as u32).into();
        Ok(Self {
            table,
            accepted,
            available: set::State::try_new(domain, [bitmap::Fill::Full])?,
            capacity,
            changes,
        })
    }

    fn has_accept_capacity(&self) -> bool {
        !self.available.get::<0>().is_empty()
    }

    fn establish(&mut self, handle: Handle) -> Result<S::Accepted, Handle> {
        let Some(vacant) = self.available.get::<0>().pop_from(index::Ordinal::new(0)) else {
            return Err(handle);
        };
        // SAFETY: this vacancy comes from the bitmap created with exactly
        // count(self.accepted) rows and retains that original prefix domain.
        let index = unsafe { S::nth(self.accepted, vacant.index().raw()) };
        if let Ok(accepted) = S::Accepted::try_from((index, self.accepted))
            && let Some(entry) = self.table.get_mut(S::offset(index))
        {
            *entry = Some(handle);
            return Ok(accepted);
        }
        vacant.restore();
        Err(handle)
    }

    fn close(&mut self, slot: S::Physical) -> closed::File<S::Missing> {
        use crate::store::change;

        let index = (slot).into();
        if let Some(cause) = S::excludes(self.capacity, index) {
            return closed::File::Missing(cause);
        }
        let offset = S::offset(index);
        let Some(entry) = self.table.get_mut(offset) else {
            return closed::File::Missing(S::backing(index));
        };
        let Some(handle) = entry.take() else {
            return closed::File::Missing(S::vacant(index));
        };
        // SAFETY: this admitted offset belongs to the table constructed with the
        // same immutable capacity as Changes; neither backing can resize.
        unsafe { change::raw::discard(&mut self.changes, offset) };
        self.availability(index, true);
        closed::File::Closed(Kqueue::close(Kqueue::into_fd(handle)))
    }

    fn install(&mut self, slot: S::Owned, handle: Handle) -> Result<(), Install<S::Index>> {
        let physical: S::Physical = slot.into();
        let index = physical.into();
        let Some(entry) = self.table.get_mut(S::offset(index)) else {
            return Err(Install::Index(index));
        };
        if entry.is_some() {
            return Err(Install::Occupied(index));
        }
        *entry = Some(handle);
        self.availability(index, false);
        Ok(())
    }

    fn raw(&self, slot: S::Physical) -> Option<fd::RawFd> {
        self.borrow(slot).map(|fd| fd.as_raw_fd())
    }

    fn borrow(&self, slot: S::Physical) -> Option<fd::BorrowedFd<'_>> {
        self.table
            .get(S::offset((slot).into()))
            .and_then(Option::as_ref)
            .map(fd::AsFd::as_fd)
    }

    fn try_socket(&self, id: Id<S::Physical>) -> Result<&Handle, S::Missing> {
        let index = (id.slot()).into();
        let handle = Self::handle(&self.table, self.capacity, index)?;
        let actual = handle.as_fd().as_raw_fd();
        if actual != id.fd() {
            return Err(S::replaced(index, id.fd(), actual));
        }
        Ok(handle)
    }

    fn try_read(
        &mut self,
        slot: S::Physical,
    ) -> Result<<Self::Changes as change::Changes>::Read<'_>, Error<S::Index, S::Missing>> {
        let index = (slot).into();
        let raw = self.descriptor(index).map_err(Error::Missing)?;
        self.changes
            .read(Id::new(slot, raw))
            .ok_or(Error::Index(Index::new(index, Filter::Read)))
    }

    fn try_write(
        &mut self,
        slot: S::Physical,
    ) -> Result<<Self::Changes as change::Changes>::Write<'_>, Error<S::Index, S::Missing>> {
        self.output(slot).map(|(_, interest)| interest)
    }

    fn read(
        &mut self,
        id: Id<S::Physical>,
    ) -> Result<Option<<Self::Changes as change::Changes>::Read<'_>>, Index<S::Index>> {
        let index = (id.slot()).into();
        let error = Index::new(index, Filter::Read);
        if !Self::matches(&self.table, id, error)? {
            return Ok(None);
        }
        self.changes.read(id).map(Some).ok_or(error)
    }

    fn write(
        &mut self,
        id: Id<S::Physical>,
    ) -> Result<Option<<Self::Changes as change::Changes>::Write<'_>>, Index<S::Index>> {
        let index = (id.slot()).into();
        let error = Index::new(index, Filter::Write);
        if !Self::matches(&self.table, id, error)? {
            return Ok(None);
        }
        self.changes.write(id).map(Some).ok_or(error)
    }

    fn try_delete(
        &mut self,
        entries: impl Iterator<Item = (Id<S::Physical>, Filter)>,
    ) -> Result<<Changes<S> as change::Changes>::Deletion<'_>, change::Index<S::Index>> {
        let table = &self.table;
        self.changes.try_delete(entries.map(|(id, filter)| {
            let index = (id.slot()).into();
            let error = Index::new(index, filter);
            Ok(Self::matches(table, id, error)?.then_some((id, filter)))
        }))
    }

    fn changes(&self) -> &Self::Changes {
        &self.changes
    }

    fn edit(&mut self) -> &mut Self::Changes {
        &mut self.changes
    }
}

impl<S: table::raw::Slots> Files<S> {
    pub(crate) fn output(
        &mut self,
        slot: S::Physical,
    ) -> Result<Output<'_, S>, Error<S::Index, S::Missing>> {
        let index = slot.into();
        let socket = Self::handle(&self.table, self.capacity, index)
            .map_err(Error::Missing)?
            .as_fd();
        let interest = self
            .changes
            .write(Id::new(slot, socket.as_raw_fd()))
            .ok_or(Error::Index(Index::new(index, Filter::Write)))?;
        Ok((socket, interest))
    }

    fn descriptor(&self, index: S::Index) -> Result<fd::RawFd, S::Missing> {
        Ok(Self::handle(&self.table, self.capacity, index)?
            .as_fd()
            .as_raw_fd())
    }

    /// Records one availability transition for `index`.
    ///
    /// The bitmap spans exactly the accepted prefix. Outbound slots live beyond
    /// that prefix and are never tracked, so their transitions leave it intact.
    fn availability(&self, index: S::Index, free: bool) {
        let Ok(vacant) = self
            .available
            .try_entry(index::Ordinal::new(S::offset(index)))
        else {
            return;
        };
        if free {
            vacant.set(set::Id::at::<0>());
        } else {
            vacant.clear(set::Id::at::<0>());
        }
    }

    fn handle(
        table: &[Option<Handle>],
        capacity: S::Capacity,
        index: S::Index,
    ) -> Result<&Handle, S::Missing> {
        if let Some(cause) = S::excludes(capacity, index) {
            return Err(cause);
        }
        let Some(entry) = table.get(S::offset(index)) else {
            return Err(S::backing(index));
        };
        let Some(handle) = entry else {
            return Err(S::vacant(index));
        };
        Ok(handle)
    }

    fn matches(
        table: &[Option<Handle>],
        id: Id<S::Physical>,
        error: Index<S::Index>,
    ) -> Result<bool, Index<S::Index>> {
        let entry = table.get(S::offset((id.slot()).into())).ok_or(error)?;
        Ok(entry
            .as_ref()
            .is_some_and(|handle| handle.as_fd().as_raw_fd() == id.fd()))
    }
}
