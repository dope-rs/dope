use std::{cell, marker, mem, num::NonZero};

use o3::{
    alloc,
    alloc::Error,
    depot,
    depot::queue::{links, round::Join},
};

use crate::{
    core::driver::schedule::ready::task::owner,
    task::storage::fixed::schedule::ready::{Global, owned::extent::Extent},
};

pub(super) struct Node {
    pub(super) hook: links::Hook,
    pub(super) lane: owner::Lane,
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
        &self.0.node(Global(index)).hook
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
        links::Detached::from_source(unsafe { depot::raw::Proof::new((&self.hook, index.0)) })
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
            depot::raw::Proof::new((self.join(&hooks, &owners, lane), index.0))
        });
    }

    pub(super) fn remove<S>(&self, store: &Store<S>, lane: owner::Lane, index: Global) {
        let hooks = Hooks(store);
        let owners = OwnerHooks(store);
        // SAFETY: the caller retains membership in this exact owner's task chain.
        Join::remove(unsafe {
            depot::raw::Proof::new((self.join(&hooks, &owners, lane), index.0))
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
    pub(super) fn try_new(
        tasks: depot::capacity::Count,
        owners: owner::Capacity,
    ) -> Result<Self, Error> {
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
            |_| cell::Cell::new(mem::MaybeUninit::uninit()),
        )?;
        Ok(Self {
            block,
            first: links::Cycle::new(),
            source: marker::PhantomData,
        })
    }

    pub(super) fn cell(&self, index: Global) -> &cell::Cell<mem::MaybeUninit<Node>> {
        // SAFETY: Queue admits Global against this exact extent before attachment.
        unsafe { &*self.block.tail().as_ptr().add(index.get()) }
    }

    pub(super) fn node(&self, index: Global) -> &Node {
        // SAFETY: attached residents and their links retain the initialized node;
        // their loans end before the task vacancy permits another attachment.
        unsafe { (&*self.cell(index).as_ptr()).assume_init_ref() }
    }

    pub(super) fn get(&self, lane: owner::Lane) -> Option<&Owner> {
        self.block.as_ref().get(lane.index())
    }

    pub(super) fn owner(&self, lane: owner::Lane) -> &Owner {
        // SAFETY: Entry and active links contain only lanes admitted by get.
        unsafe { &*self.block.as_ref().as_ptr().add(lane.index()) }
    }
}
