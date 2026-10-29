use std::mem;

use dope_core::driver::{
    result, route,
    route::{
        Identity, KeyTag,
        table::{self, conn},
    },
};
use o3::{
    depot,
    depot::{
        arena::slab::external::errors,
        queue::indexed::{exact, fifo},
    },
};
pub use receiver::Receiver;

use crate::{
    link::{event, pool, slot::recv::Submit},
    wire,
    wire::receive::{Chunk, mode::Disabled},
};

pub mod arrival;
pub mod delivery;
pub mod input;
mod receiver;
pub mod reserve;
pub mod storage;

/// Borrow of the generation key stored by one occupied connection slot.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Resident<'a, 'd, ID: Identity> {
    key: &'a pool::Key<'d, ID>,
}

impl<'a, 'd, ID: Identity> Resident<'a, 'd, ID> {
    pub(in crate::link) const fn new(key: &'a pool::Key<'d, ID>) -> Self {
        Self { key }
    }

    pub(in crate::link) const fn key(self) -> pool::Key<'d, ID> {
        *self.key
    }
}

const _: () =
    assert!(mem::size_of::<Resident<'static, 'static, KeyTag<0>>>() == mem::size_of::<usize>());
const _: () =
    assert!(mem::align_of::<Resident<'static, 'static, KeyTag<0>>>() == mem::align_of::<usize>());

pub enum Ready<'d> {
    Completion(event::Completion<'d>),
    Empty,
    Paused,
    Stale,
}

pub enum Readiness {
    Ready,
    Empty,
    Paused,
    Stale,
}

/// A receive target rejected by its owning connection table.
pub struct Missing<'d, ID: Identity> {
    pub key: pool::Key<'d, ID>,
    pub cause: errors::Lookup<route::Epoch, ID>,
}

/// Exact permanent failure to submit one connection's receive operation.
#[derive(Debug)]
pub struct Failure<'d, ID: Identity> {
    pub key: pool::Key<'d, ID>,
    pub cause: result::SubmitError,
}

/// Exact outcome of resuming one connection's receive path.
pub enum Resumption<'d, ID: Identity> {
    Unavailable(Missing<'d, ID>),
    Ready(delivery::Resumed<'d, ID>),
}

/// Exact outcome of storing one deferred receive chunk.
pub enum Deferral<'w, 'd, W: wire::Spec + 'w> {
    Ready,
    Unavailable(Chunk<'w, 'd, W, Disabled>),
    Occupied(Chunk<'w, 'd, W, Disabled>),
    Full(Chunk<'w, 'd, W, Disabled>),
}

pub(in crate::link::pool) struct State<'d, ID: Identity, P: storage::raw::Mode> {
    pub(in crate::link::pool) rearm:
        fifo::List<pool::Key<'d, ID>, pool::raw::Physical<'d, ID>, exact::Policy>,
    pub(in crate::link::pool) storage: P::Storage<'d>,
}

pub(in crate::link::pool) struct Borrow<'a, 'd: 'a, ID: Identity, P: storage::raw::Mode> {
    pub(in crate::link::pool) rearm: Rearm<'a, 'd, ID>,
    pub(in crate::link::pool) storage: P::Bound<'a, 'd>,
}

impl<'d, ID: Identity, P: storage::raw::Mode> State<'d, ID, P> {
    /// Pairs one admitted rearm allocation with the storage its mode built.
    pub(in crate::link::pool) fn try_new(
        cap: conn::Capacity,
        storage: P::Storage<'d>,
    ) -> crate::Result<Self> {
        Ok(Self {
            rearm: fifo::List::try_count(table::Capacity::from(cap))
                .map_err(crate::Resource::Allocation)?,
            storage,
        })
    }
}

/// A receive identity and its admitted slot in the original rearm allocation.
pub(in crate::link::pool) struct Rearm<'a, 'd, ID: Identity> {
    entry: exact::Entry<'a, pool::Key<'d, ID>, pool::raw::Physical<'d, ID>>,
    key: pool::Key<'d, ID>,
}

impl<'d, ID: Identity> Rearm<'_, 'd, ID> {
    pub(in crate::link::pool) fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub(in crate::link::pool) fn queue(&mut self) {
        self.entry.schedule(self.key);
    }

    pub(in crate::link::pool) fn remove(&mut self) {
        self.entry.unlink(self.key);
    }

    pub(in crate::link::pool) fn submitted(
        &mut self,
        submit: Result<Submit, result::SubmitError>,
    ) -> Result<(), Failure<'d, ID>> {
        let submit = match submit {
            Ok(submit) => submit,
            Err(cause) => {
                self.remove();
                return Err(Failure {
                    key: self.key,
                    cause,
                });
            }
        };
        match submit {
            Submit::Deferred => self.queue(),
            Submit::Armed | Submit::Waiting => {}
        }
        Ok(())
    }
}

// SAFETY: the input Live retains the original State and a resident of its paired table.
unsafe impl<'a, 'd: 'a, ID: Identity, P: storage::raw::Mode> depot::raw::Project<pool::Key<'d, ID>>
    for &'a mut State<'d, ID, P>
{
    type Output = Borrow<'a, 'd, ID, P>;

    unsafe fn project(self, key: pool::Key<'d, ID>) -> Self::Output {
        use o3::depot::raw::Proof;
        let index = pool::raw::Physical::bind(key);
        // SAFETY: the consumed projection retains this paired table's admitted resident index.
        let entry = unsafe { Proof::new((&mut self.rearm, index)) }.into();
        // SAFETY: table admission checked the selected storage against that same fixed extent.
        let slot = unsafe { Proof::new((&mut self.storage, key.index().into_usize())) };
        let storage = P::bind(slot);
        Borrow {
            rearm: Rearm { entry, key },
            storage,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Rearm<'static, 'static, KeyTag<0>>>() == 32);
    assert!(mem::size_of::<Option<Rearm<'static, 'static, KeyTag<0>>>>() == 32);
};
