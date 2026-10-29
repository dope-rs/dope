use core::pin;

use o3::{
    depot,
    depot::{
        index::Initialized,
        map::{self, flat},
        queue::{indexed::keyed, links},
    },
};

use crate::{
    abi,
    task::storage::{
        self, Handle,
        set::{
            group::order,
            slot::{Slot, State, Token, Vacated},
            views,
        },
    },
};

pub(in crate::task::storage::set) struct Done<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: &'b mut Token<'a, F, M>,
    slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
    key: Handle<'d, D>,
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> Done<'b, 'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) const fn new(
        view: views::View<'a, 'd, F, M, D, G>,
        token: &'b mut Token<'a, F, M>,
        slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
        key: Handle<'d, D>,
    ) -> Self {
        Self {
            view,
            token,
            slot,
            key,
        }
    }

    pub(in crate::task::storage::set) fn take(self) -> (M, F::Output) {
        let output = self.token.take(self.slot.project_ref().value);
        self.view.release(Vacated::new(self.slot));
        output
    }

    pub(in crate::task::storage::set) fn discard(self) {
        self.token.drop_output(self.slot.project_ref().value);
        self.view.release(Vacated::new(self.slot));
    }

    pub(in crate::task::storage::set) fn data(&self) -> &M {
        self.slot.data()
    }

    pub(in crate::task::storage::set) fn key(&self) -> Handle<'d, D> {
        self.key
    }

    pub(in crate::task::storage::set) fn queue(
        self,
        fifo: &mut keyed::Fifo<Handle<'d, D>, storage::Id<'d, D>>,
        group: G,
    ) where
        G: Copy,
    {
        // SAFETY: popping the completion produced this detached slot with its
        // original storage view and exclusive token, all consumed by Attach.
        let vacant = unsafe {
            keyed::Slot::from_source(depot::raw::Proof::<_, keyed::Membership<false>>::new(
                Attach {
                    done: self,
                    fifo,
                    group,
                },
            ))
        };
        vacant.push::<false>().release();
    }
}

struct Attach<'q, 'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    done: Done<'b, 'a, 'd, F, M, D, G>,
    fifo: &'q mut keyed::Fifo<Handle<'d, D>, storage::Id<'d, D>>,
    group: G,
}

// SAFETY: the retained completion binds its exact key and pinned link until commit.

// SAFETY: the completion, its original token and storage view, and the selected
// group FIFO remain borrowed together until the consuming state transition.
unsafe impl<'d, F: abi::Fiber<'d>, M, D, G> depot::raw::KeyOwner<storage::Id<'d, D>>
    for Attach<'_, '_, '_, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;

    type Fifo = keyed::Fifo<Self::Key, storage::Id<'d, D>>;

    fn parts(&self) -> (Self::Key, &keyed::Link<Self::Key, storage::Id<'d, D>>) {
        (self.done.key, &self.done.slot.get_ref().link)
    }

    fn fifo(&self) -> &keyed::Fifo<Self::Key, storage::Id<'d, D>> {
        self.fifo
    }

    fn entry<'s>(
        live: depot::raw::Proof<(&'s Self, links::Index), Initialized<storage::Id<'d, D>>>,
    ) -> keyed::Entry<'s, Self::Key, storage::Id<'d, D>> {
        let live = unsafe { live.map(|(attach, index)| (&attach.done.view, index)) };
        views::View::entry(live)
    }
}

// SAFETY: membership is committed before the same retained slot becomes external.
unsafe impl<'d, F: abi::Fiber<'d>, M, D, G: Copy> depot::raw::KeyFinish<true, storage::Id<'d, D>>
    for Attach<'_, '_, '_, 'd, F, M, D, G>
{
    type Output = ();

    unsafe fn finish(self) {
        self.done.slot.externalize(self.group);
    }
}

pub(in crate::task::storage::set) struct Listed<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: &'b mut Token<'a, F, M>,
    slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
    key: Handle<'d, D>,
    group: G,
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G: Copy> Listed<'b, 'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn get(
        view: views::View<'a, 'd, F, M, D, G>,
        token: &'b mut Token<'a, F, M>,
        key: Handle<'d, D>,
    ) -> Option<Self> {
        let slot = view.resolve(key)?;
        if slot.state() != State::External {
            return None;
        }
        Some(Self {
            view,
            token,
            slot,
            key,
            group: slot.take_group(),
        })
    }

    pub(in crate::task::storage::set) fn front<'q, K>(
        view: views::View<'a, 'd, F, M, D, G>,
        token: &'b mut Token<'a, F, M>,
        entry: Ordered<'q, 'd, K, D>,
    ) -> keyed::Slot<Removal<'q, 'b, 'a, 'd, F, M, D, G, K>, storage::Id<'d, D>> {
        let live = views::Resident::front(&view, &entry.get().1);
        let index = live.key().raw();
        let slot = views::View::project(live);
        let key = slot.current(index);
        Removal {
            view,
            token,
            slot,
            key,
            entry,
        }
        .into_slot()
    }
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D> Listed<'b, 'a, 'd, F, M, D, order::Membership<'d, D>> {
    #[inline(always)]
    pub(in crate::task::storage::set) fn detach<K>(
        self,
        orders: &mut flat::Table<(K, order::Order<'d, D>)>,
    ) -> Done<'b, 'a, 'd, F, M, D, order::Membership<'d, D>> {
        let (hash, identity) = self.group;
        let matches = |(_, order): &(K, order::Order<'d, D>)| order.identity == identity;
        // SAFETY: this live external slot retains its original order until detached.
        // Its stored hash and immutable complete identity survive directory backshift.
        let source = unsafe { depot::raw::Proof::new((orders, self, hash, matches)) };
        let (entry, listed) = flat::Table::member(source);
        Removal {
            view: listed.view,
            token: listed.token,
            slot: listed.slot,
            key: listed.key,
            entry,
        }
        .into_slot()
        .remove()
        .release()
    }
}

type Ordered<'q, 'd, K, D> = map::Found<&'q mut flat::Table<(K, order::Order<'d, D>)>>;

pub(in crate::task::storage::set) struct Removal<'q, 'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G, K> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: &'b mut Token<'a, F, M>,
    slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
    key: Handle<'d, D>,
    entry: Ordered<'q, 'd, K, D>,
}

impl<'d, F: abi::Fiber<'d>, M, D, G, K> Removal<'_, '_, '_, 'd, F, M, D, G, K> {
    fn into_slot(self) -> keyed::Slot<Self, storage::Id<'d, D>> {
        // SAFETY: the retained slot's immutable group identity selected this exact
        // order, or its endpoint selected this slot; the token excludes recycling.
        unsafe {
            keyed::Slot::from_source(depot::raw::Proof::<_, keyed::Membership<true>>::new(self))
        }
    }
}

// SAFETY: the retained removal binds its exact key and pinned link until detachment.

// SAFETY: the original pinned slot, storage view, exclusive token and selected
// group order remain owned together until this member has been detached.
unsafe impl<'d, F: abi::Fiber<'d>, M, D, G, K> depot::raw::KeyOwner<storage::Id<'d, D>>
    for Removal<'_, '_, '_, 'd, F, M, D, G, K>
{
    type Key = Handle<'d, D>;

    type Fifo = keyed::Fifo<Self::Key, storage::Id<'d, D>>;

    fn parts(&self) -> (Self::Key, &keyed::Link<Self::Key, storage::Id<'d, D>>) {
        (self.key, &self.slot.get_ref().link)
    }

    fn fifo(&self) -> &keyed::Fifo<Self::Key, storage::Id<'d, D>> {
        &self.entry.get().1.fifo
    }

    fn entry<'s>(
        live: depot::raw::Proof<(&'s Self, links::Index), Initialized<storage::Id<'d, D>>>,
    ) -> keyed::Entry<'s, Self::Key, storage::Id<'d, D>> {
        let live = unsafe { live.map(|(owner, index)| (&owner.view, index)) };
        views::View::entry(live)
    }
}

// SAFETY: canonical vacancy grants output extraction; the same entry retires its
// empty order before the completed slot leaves this native detachment.
unsafe impl<'q, 'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G, K>
    depot::raw::KeyFinish<false, storage::Id<'d, D>> for Removal<'q, 'b, 'a, 'd, F, M, D, G, K>
{
    type Output = Done<'b, 'a, 'd, F, M, D, G>;

    unsafe fn finish(self) -> Self::Output {
        if self.entry.get().1.fifo.is_empty() {
            self.entry.discard();
        }
        Done::new(self.view, self.token, self.slot, self.key)
    }
}
