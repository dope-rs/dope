use std::os::fd;

use o3::{alloc, depot::map::Plan};

use crate::{
    table,
    table::{
        change,
        registry::{Entry, Failure, Occupied, Vacancy},
    },
};

pub trait State<S: table::raw::Slots, E: Entry>: Sized {
    type Files: table::Files<S, Changes: change::Changes<Id = E::File>>;
    type Vacancy<'a>: Vacancy<Echo = E::Echo>
    where
        Self: 'a;
    type Occupied<'a>: Occupied<Entry = E>
    where
        Self: 'a;

    fn try_new(plan: Plan) -> Result<Self, alloc::Error>;
    fn try_vacant(&mut self, slot: E) -> Result<Self::Vacancy<'_>, (E, Failure)>;
    fn occupied(&mut self, echo: E::Echo, family: E::Family) -> Option<Self::Occupied<'_>>;
    fn take(&mut self, id: E::Id) -> Option<E::Call>;
    fn remove_fd(
        &mut self,
        raw: fd::RawFd,
        owner: &mut Self::Files,
    ) -> Result<Option<E::Echo>, change::Index<S::Index>>;
    fn operation(&self, id: E::Id) -> Option<E::Call>;
    fn resolve(&self, echo: E::Echo) -> Option<(E::Id, E::Call)>;
    fn find(&self, echo: E::Echo, family: E::Family) -> Option<E::File>;
    fn queue_resume(&mut self, id: E::Id);
    fn pop_resume(&mut self) -> Option<E::Id>;
    fn can_resume(&self) -> bool;
    fn files(&self) -> impl Iterator<Item = E::File>;
    fn clear(&mut self);
}
