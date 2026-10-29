use o3::depot::map::Plan;

use crate::{
    io::reactor::{
        complete,
        ready::{
            Driver, Failures,
            store::{Context, Create},
        },
    },
    memory::buffer,
    table,
};

type PoolContext<C> = <<<C as Driver>::Context as Context>::Pool as buffer::Pool>::Context;

pub trait Backend<C: Driver>: Sized {
    type Failure;
    type Slot;
    type Closed;
    fn try_new(
        recv: <<C::Context as Context>::Pool as buffer::Pool>::Layout,
        slots: <<C::Context as Context>::Slots as table::raw::Slots>::Layout,
        plan: Plan,
        capacity: <C::Queue as complete::Queue>::Capacity,
    ) -> Result<(Self, PoolContext<C>), Create>;
    fn state(&self) -> &C::State;
    fn state_mut(&mut self) -> &mut C::State;
    fn cursor(&mut self) -> usize;
    fn change_len(&self) -> usize;
    fn available(&self) -> usize;
    fn has_changes(&self) -> bool;
    fn has_pending(&self) -> bool;
    fn can_resume(&self) -> bool;
    fn check(&self) -> Result<(), Self::Failure>;
    fn quiesce<'a>(&'a mut self) -> Result<(), Failures<Self::Failure>>
    where
        C::Context: 'a;
    fn close<'a>(&'a mut self, slot: Self::Slot) -> Self::Closed
    where
        C::Context: 'a;
}
