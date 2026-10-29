use std::{cell, marker, num::NonZero};

use o3::{
    alloc,
    alloc::Error,
    depot,
    depot::queue::{links, round::Join},
};

use crate::{
    core::driver::schedule::ready::task::owner,
    task::storage::fixed::schedule::ready::{
        Global,
        owned::{extent::Extent, global, key},
    },
};

pub(super) struct Node {
    pub(super) hook: links::Hook,
    /// Admission writes the lane before publication; detached storage grants no read authority.
    pub(super) lane: cell::Cell<owner::Lane>,
}

pub(super) struct Owner {
    pub(super) generation: cell::Cell<Option<owner::Generation>>,
    pub(super) bound: cell::Cell<u64>,
    pub(super) chain: links::Chain,
    pub(super) hook: links::Hook,
}

pub(super) struct Hooks<'a, S>(pub(super) &'a Store<S>);
pub(super) struct OwnerHooks<'a, S>(pub(super) &'a Store<S>);

// SAFETY: each admitted task index names one stable hook in the exact task plane.
unsafe impl<S> depot::raw::Hooks for Hooks<'_, S> {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        &self.0.node(global(index)).hook
    }
}

// SAFETY: each admitted owner index names one stable, disjoint owner-plane hook.
unsafe impl<S> depot::raw::Hooks for OwnerHooks<'_, S> {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        // SAFETY: depot::raw::Hooks's caller retains membership in this exact owner plane.
        let owner = unsafe { &*self.0.block.as_ref().as_ptr().add(index.get()) };
        &owner.hook
    }
}

impl Node {
    pub(super) fn detach(&self, owner: &Owner, index: Global) {
        // SAFETY: the caller detached this exact resident from its owner chain
        // and retains the sole resident transition, whether parked or idle.
        links::Detached::from_source(unsafe { depot::raw::Proof::new((&self.hook, key(index))) })
            .idle();
        owner.bound.set(owner.bound.get() - 1);
    }
}

impl Owner {
    pub(super) fn join<'a, 's, S>(
        &'a self,
        hooks: &'a Hooks<'s, S>,
        owners: &'a OwnerHooks<'s, S>,
        lane: owner::Lane,
    ) -> Join<'a, Hooks<'s, S>, OwnerHooks<'s, S>> {
        Join {
            hooks,
            lanes: owners,
            entries: &self.chain,
            active: &hooks.0.first,
            index: lane.position().into(),
        }
    }

    pub(super) fn enqueue<S>(&self, store: &Store<S>, lane: owner::Lane, index: Global) {
        let hooks = Hooks(store);
        let owners = OwnerHooks(store);
        // SAFETY: the resident transition owns this detached hook in the exact
        // stable task plane; this owner's endpoints retain reciprocal links.
        Join::insert(unsafe {
            depot::raw::Proof::new((self.join(&hooks, &owners, lane), key(index)))
        });
    }

    pub(super) fn remove<S>(&self, store: &Store<S>, lane: owner::Lane, index: Global) {
        let hooks = Hooks(store);
        let owners = OwnerHooks(store);
        // SAFETY: the caller retains membership in this exact owner's task chain.
        Join::remove(unsafe {
            depot::raw::Proof::new((self.join(&hooks, &owners, lane), key(index)))
        })
        .park();
    }
}

pub(super) struct Store<S> {
    pub(super) block: alloc::raw::Block<Extent, (), (), Owner>,
    pub(super) first: links::Cycle,
    source: marker::PhantomData<fn(S) -> S>,
}

impl<S> Store<S> {
    /// Both nonzero u32 extent counts bound their initialization indices below u32::MAX,
    /// so each cast and one-based NonZero addition below preserves the exact position.
    pub(super) fn try_new(tasks: NonZero<u32>, owners: owner::Capacity) -> Result<Self, Error> {
        let extent = Extent { tasks, owners };
        let block = alloc::raw::Block::try_from_fn(
            extent,
            (),
            |index| {
                let index = NonZero::<u32>::MIN.saturating_add(index as u32).into();
                Owner {
                    generation: cell::Cell::new(None),
                    bound: cell::Cell::new(0),
                    chain: links::Chain::new(),
                    hook: links::Hook::new(index),
                }
            },
            |index| {
                let index = NonZero::<u32>::MIN.saturating_add(index as u32).into();
                Node {
                    hook: links::Hook::new(index),
                    lane: cell::Cell::new(owner::Lane::from(NonZero::<u32>::MIN)),
                }
            },
        )?;
        Ok(Self {
            block,
            first: links::Cycle::new(),
            source: marker::PhantomData,
        })
    }

    pub(super) fn node(&self, index: Global) -> &Node {
        // SAFETY: Queue admits Global against this extent before attachment;
        // every stored or woken link retains that exact queue ownership.
        unsafe { &*self.block.tail().as_ptr().add(index.get()) }
    }

    pub(super) fn get(&self, lane: owner::Lane) -> Option<&Owner> {
        self.block.as_ref().get(lane.index())
    }

    pub(super) fn owner(&self, lane: owner::Lane) -> &Owner {
        // SAFETY: Entry and active links contain only lanes admitted by get.
        unsafe { &*self.block.as_ref().as_ptr().add(lane.index()) }
    }
}
