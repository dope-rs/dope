pub(in crate::output) mod removal;
pub(in crate::output) mod vacancy;

use std::{mem, os::fd};

use dope_sys::io::reactor::retry;
use o3::{
    alloc,
    depot::{
        self,
        map::{Plan, flat},
    },
};

use crate::{
    output::{Removal, Vacancy},
    store::hash,
};

pub struct State<E: retry::Entry> {
    by_target: flat::Table<(usize, E::Record)>,
    by_fd: flat::Table<(usize, E::Target)>,
}

impl<E: retry::Entry> retry::State<E> for State<E> {
    type Vacancy<'a>
        = Vacancy<'a, E>
    where
        Self: 'a;
    type Occupied<'a>
        = Removal<'a, E>
    where
        Self: 'a;

    fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        const {
            assert!(
                mem::size_of::<Self>()
                    == mem::size_of::<flat::Table<(usize, E::Record)>>()
                        + mem::size_of::<flat::Table<(usize, E::Target)>>()
            );
        }
        Ok(Self {
            by_target: flat::Table::try_new(plan)?,
            by_fd: flat::Table::try_new(plan)?,
        })
    }

    fn vacancy(&mut self, pending: E) -> Result<Self::Vacancy<'_>, E> {
        let key = pending.target().into();
        let Some(flat::Entry::Vacant(task)) =
            self.by_target.entry(hash(key), |entry| entry.0 == key)
        else {
            return Err(pending);
        };
        let raw = pending.fd() as usize;
        // SAFETY: identical plans and paired commit/remove/clear keep both lengths
        // equal. The retained primary vacancy proves spare secondary capacity.
        let source = unsafe { depot::raw::Proof::new((&mut self.by_fd, task)) };
        let (fd, task) = flat::Table::admit(source, hash(raw), |entry| entry.0 == raw);
        let flat::Entry::Vacant(fd) = fd else {
            return Err(pending);
        };
        Ok(Vacancy {
            pending,
            task: (key, task),
            fd: (raw, fd),
        })
    }

    fn occupied(&mut self, target: E::Target) -> Option<Self::Occupied<'_>> {
        let key = target.into();
        let task = self.by_target.occupied(hash(key), |entry| entry.0 == key)?;
        let raw = E::descriptor(&task.get().1) as usize;
        let fd = self.by_fd.occupied(hash(raw), |entry| entry.0 == raw)?;
        if fd.get().1 != target {
            return None;
        }
        Some(Removal::<E> { task, fd })
    }

    fn occupied_fd(&mut self, raw: fd::RawFd) -> Option<Self::Occupied<'_>> {
        let key = raw as usize;
        let fd = self.by_fd.occupied(hash(key), |entry| entry.0 == key)?;
        let key = fd.get().1.into();
        let task = self.by_target.occupied(hash(key), |entry| entry.0 == key)?;
        if E::descriptor(&task.get().1) != raw {
            return None;
        }
        Some(Removal::<E> { task, fd })
    }

    fn files(&self) -> impl Iterator<Item = E::File> {
        self.by_target.values().map(|entry| E::file(&entry.1))
    }

    fn first(&self) -> Option<E::Target> {
        self.by_fd.values().next().map(|entry| entry.1)
    }

    fn clear(&mut self) {
        self.by_target.clear();
        self.by_fd.clear();
    }
}
