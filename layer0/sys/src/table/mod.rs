pub mod change;
pub mod closed;
pub mod permit;
pub mod registry;
pub mod token;
use std::{os::fd, task};

use o3::alloc;
use reclaim::{Kind, View};

use crate::{io::socket, table::change::Index};

pub mod raw;
pub mod reclaim;

type Interest<T, S> =
    Result<T, Error<<S as change::raw::Slots>::Index, <S as raw::Slots>::Missing>>;

/// Exact table or changelist absence while acquiring a descriptor interest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<I, M> {
    Missing(M),
    Index(Index<I>),
}

pub trait Files<S: raw::Slots>: Sized {
    type Handle: socket::raw::Open + socket::raw::Receive;
    type Changes: change::Changes<Slots = S>;

    fn try_new(layout: S::Layout) -> Result<Self, alloc::Error>;
    fn has_accept_capacity(&self) -> bool;
    fn establish(&mut self, handle: Self::Handle) -> Result<S::Accepted, Self::Handle>;
    fn close(&mut self, slot: S::Physical) -> closed::File<S::Missing>;
    fn install(&mut self, slot: S::Owned, handle: Self::Handle) -> Result<(), Install<S::Index>>;
    fn raw(&self, slot: S::Physical) -> Option<fd::RawFd>;
    fn borrow(&self, slot: S::Physical) -> Option<fd::BorrowedFd<'_>>;
    fn try_socket(
        &self,
        id: <Self::Changes as change::Changes>::Id,
    ) -> Result<&Self::Handle, S::Missing>;
    fn try_read(
        &mut self,
        slot: S::Physical,
    ) -> Interest<<Self::Changes as change::Changes>::Read<'_>, S>;
    fn try_write(
        &mut self,
        slot: S::Physical,
    ) -> Interest<<Self::Changes as change::Changes>::Write<'_>, S>;
    fn read(
        &mut self,
        id: <Self::Changes as change::Changes>::Id,
    ) -> Result<Option<<Self::Changes as change::Changes>::Read<'_>>, change::Index<S::Index>>;
    fn write(
        &mut self,
        id: <Self::Changes as change::Changes>::Id,
    ) -> Result<Option<<Self::Changes as change::Changes>::Write<'_>>, change::Index<S::Index>>;
    fn try_delete(
        &mut self,
        entries: impl Iterator<Item = (<Self::Changes as change::Changes>::Id, change::Filter)>,
    ) -> Result<<Self::Changes as change::Changes>::Deletion<'_>, change::Index<S::Index>>;
    fn changes(&self) -> &Self::Changes;
    fn edit(&mut self) -> &mut Self::Changes;
}

/// Exact in-memory descriptor installation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Install<S> {
    Index(S),
    Occupied(S),
}

impl<S> From<Install<S>> for crate::kernel::Diagnostic<S> {
    fn from(error: Install<S>) -> Self {
        match error {
            Install::Index(index) => Self::FileIndex(index),
            Install::Occupied(index) => Self::FileOccupied(index),
        }
    }
}

const _: () = assert!(std::mem::size_of::<Install<u32>>() == 2 * std::mem::size_of::<u32>());

/// Retires one affine descriptor transaction before advancing its host domain.
pub trait Retire<'d, V: View> {
    type Output;

    fn retire<'a>(
        &'a mut self,
        pending: Kind<V::Close, V::Retire>,
        view: V,
    ) -> task::Poll<Self::Output>
    where
        'd: 'a;
}

pub trait Source {
    type Files<S: raw::Slots>: Files<S>;
}
