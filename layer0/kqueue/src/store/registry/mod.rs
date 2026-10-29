mod occupied;
mod vacancy;
use std::os::fd;

use dope_sys::{
    io::reactor::read,
    table::{
        Files as _,
        change::{self, Id as _, Interest as _},
        registry::{self, Entry as _, Failure, Occupied as _},
    },
};
use o3::{
    alloc,
    depot::{
        self,
        map::{Plan, flat},
        queue::indexed::fifo,
    },
};
pub(crate) use occupied::Occupied;
pub(crate) use vacancy::Vacancy;

use crate::{
    reactor::input::{Call, Slot, entry::Id},
    store::{hash, table::Files},
};

pub struct Registry<C: read::Context> {
    slots: flat::Table<Slot<C>>,
    by_fd: flat::Table<(usize, Id)>,
    resume: fifo::List<Id>,
}

impl<C: read::Context> registry::State<C::Slots, Slot<C>> for Registry<C> {
    type Files = Files<C::Slots>;
    type Vacancy<'a>
        = Vacancy<'a, C>
    where
        Self: 'a;
    type Occupied<'a>
        = Occupied<'a, C>
    where
        Self: 'a;

    fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: flat::Table::try_new(plan)?,
            by_fd: flat::Table::try_new(plan)?,
            resume: fifo::List::try_count(plan.capacity())?,
        })
    }

    fn try_vacant(&mut self, slot: Slot<C>) -> Result<Self::Vacancy<'_>, (Slot<C>, Failure)> {
        let read = slot.id();
        if slot.index() as usize >= self.resume.capacity() {
            return Err((slot, Failure::Slot));
        }
        let key = Slot::<C>::key(read);
        let vacant = match self
            .slots
            .entry(hash(key), |entry| Slot::<C>::key(entry.id()) == key)
        {
            Some(flat::Entry::Vacant(vacant)) => vacant,
            Some(flat::Entry::Occupied(_)) => return Err((slot, Failure::TargetOccupied)),
            None => return Err((slot, Failure::TargetFull)),
        };
        let raw = slot.file().fd() as usize;
        // SAFETY: both indices have the same capacity and change together. The
        // retained primary vacancy proves one spare entry in the fd index.
        let source = unsafe { depot::raw::Proof::new((&mut self.by_fd, vacant)) };
        let (fd, vacant) = flat::Table::admit(source, hash(raw), |entry| entry.0 == raw);
        let fd = match fd {
            flat::Entry::Vacant(fd) => fd,
            flat::Entry::Occupied(_) => return Err((slot, Failure::FileOccupied)),
        };
        Ok(Vacancy {
            value: slot,
            id: read,
            slot: vacant,
            fd: (raw, fd),
        })
    }

    fn occupied(&mut self, echo: C::Key, family: read::Family) -> Option<Self::Occupied<'_>> {
        let slot = self.slots.occupied(hash(echo.into()), |slot| {
            slot.echo() == echo && slot.family() == family
        })?;
        Some(Occupied::new(slot, &mut self.by_fd, &mut self.resume))
    }

    fn take(&mut self, id: Id) -> Option<Call<C>> {
        let key = Slot::<C>::key(id);
        let slot = self.slots.occupied(hash(key), |slot| slot.id() == id)?;
        Some(
            Occupied::new(slot, &mut self.by_fd, &mut self.resume)
                .remove()
                .into_call(),
        )
    }

    fn remove_fd(
        &mut self,
        raw: fd::RawFd,
        owner: &mut Self::Files,
    ) -> Result<Option<C::Key>, change::Index<<C::Slots as change::raw::Slots>::Index>> {
        let key = raw as usize;
        let Some(fd) = self.by_fd.occupied(hash(key), |entry| entry.0 == key) else {
            return Ok(None);
        };
        let id = fd.get().1;
        // SAFETY: the retained fd entry keeps the primary insertion hash and
        // immutable descriptor. Pair removal preserves this membership; matching
        // the descriptor does not repeat an arbitrary Key conversion.
        let source = unsafe {
            depot::raw::Proof::new((
                &mut self.slots,
                fd,
                hash(Slot::<C>::key(id)),
                |slot: &Slot<C>| slot.file().fd() == raw,
            ))
        };
        let (slot, fd) = flat::Table::member(source);
        let interest = owner.read(slot.get().file())?;
        let slot = Occupied {
            slot,
            fd,
            resume: &mut self.resume,
        }
        .remove();
        if let Some(file) = interest {
            file.discard();
        }
        Ok(Some(slot.echo()))
    }

    fn operation(&self, id: Id) -> Option<Call<C>> {
        let slot = self
            .slots
            .get(hash(Slot::<C>::key(id)), |slot| slot.id() == id)?;
        Some(slot.operation())
    }

    fn resolve(&self, echo: C::Key) -> Option<(Id, Call<C>)> {
        self.slots
            .get(hash(echo.into()), |slot| slot.echo() == echo)
            .map(|slot| (slot.id(), slot.operation()))
    }

    fn find(
        &self,
        echo: C::Key,
        family: read::Family,
    ) -> Option<<Slot<C> as registry::Entry>::File> {
        let slot = self.slots.get(hash(echo.into()), |slot| {
            slot.echo() == echo && slot.family() == family
        })?;
        Some(slot.file())
    }

    fn queue_resume(&mut self, id: Id) {
        let Some(slot) = self
            .slots
            .get(hash(Slot::<C>::key(id)), |slot| slot.id() == id)
        else {
            return;
        };
        let Some(entry) = self.resume.vacant_entry(slot.index()) else {
            return;
        };
        entry.push_back(id);
    }

    fn pop_resume(&mut self) -> Option<Id> {
        self.resume.pop_front()
    }

    fn can_resume(&self) -> bool {
        !self.resume.is_empty()
    }

    fn files(&self) -> impl Iterator<Item = <Slot<C> as registry::Entry>::File> {
        self.slots.values().map(Slot::<C>::file)
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.by_fd.clear();
        while self.resume.pop_front().is_some() {}
    }
}
