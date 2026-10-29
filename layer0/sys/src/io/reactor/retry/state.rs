use std::os::fd;

use o3::{alloc, depot::map::Plan};

use crate::io::reactor::retry::{Entry, Occupied, Vacancy};

pub trait State<E: Entry>: Sized {
    type Vacancy<'a>: Vacancy
    where
        Self: 'a;
    type Occupied<'a>: Occupied<Entry = E>
    where
        Self: 'a;

    fn try_new(plan: Plan) -> Result<Self, alloc::Error>;
    fn vacancy(&mut self, pending: E) -> Result<Self::Vacancy<'_>, E>;
    fn occupied(&mut self, target: E::Target) -> Option<Self::Occupied<'_>>;
    fn occupied_fd(&mut self, raw: fd::RawFd) -> Option<Self::Occupied<'_>>;
    fn files(&self) -> impl Iterator<Item = E::File>;
    fn first(&self) -> Option<E::Target>;
    fn clear(&mut self);
}
