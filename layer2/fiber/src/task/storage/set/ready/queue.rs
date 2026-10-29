use std::{marker, pin};

use o3::depot::queue::indexed::keyed::lanes;
use set::{
    entries::Entries,
    slot,
    slot::{Counted, Done, state::State},
};

use crate::{
    abi,
    context::wake,
    core::driver::schedule::ready::{task, task::topology},
    task::{
        storage,
        storage::{
            Handle, set,
            set::{
                ready,
                ready::{core, drain::Drain},
                views,
            },
        },
    },
};

type Domain<'d, D, F, M, G> = marker::PhantomData<fn(&'d (), D, F, M, G) -> (&'d (), D, F, M, G)>;

#[pin_project::pin_project(!Unpin)]
pub(in crate::task::storage::set) struct Queue<'d, F: abi::Fiber<'d>, M, D, G = ()> {
    core: core::Core<'d, D>,
    #[pin]
    slot: topology::Slot<'d, ready::raw::Index<D>>,
    domain: Domain<'d, D, F, M, G>,
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Queue<'d, F, M, D, G> {
    pub(in crate::task::storage::set) const fn new(
        work: lanes::Fifo<Handle<'d, D>, 3, storage::Id<'d, D>>,
    ) -> Self {
        Self {
            core: core::Core::new(work),
            slot: topology::Slot::new(),
            domain: marker::PhantomData,
        }
    }

    pub(in crate::task::storage::set) fn try_attach<'a>(
        view: views::View<'a, 'd, F, M, D, G>,
        index: ready::raw::Index<D>,
        parent: wake::RootWaker<'d>,
    ) -> Result<topology::Edge<'a, 'd, ready::raw::Index<D>>, task::Error> {
        let queue = Self::as_pin(&view.header().ready);
        let parent: wake::Waker<'d> = parent.into();
        queue.slot().try_attach(parent.0, index)
    }

    pub(in crate::task::storage::set) fn insert<'a>(
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &mut slot::Token<'a, F, M>,
        admitted: Counted<'a, 'd, F, M, D, G>,
        key: Handle<'d, D>,
    ) {
        Self::as_pin(&view.header().ready)
            .backend()
            .insert(view, token, admitted, key);
    }

    pub(in crate::task::storage::set) fn remove<'a>(
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &mut slot::Token<'a, F, M>,
        key: Handle<'d, D>,
    ) -> Option<State> {
        Some(core::Removal::resolve(view, token, key)?.remove(core::Kind::Flat))
    }

    pub(in crate::task::storage::set) fn pop<'b, 'a>(
        view: views::View<'a, 'd, F, M, D, G>,
        token: &'b mut slot::Token<'a, F, M>,
    ) -> Option<Done<'b, 'a, 'd, F, M, D, G>> {
        Self::as_pin(&view.header().ready)
            .backend()
            .pop(view, token)
    }

    pub(in crate::task::storage::set) fn is_empty(view: &views::View<'_, 'd, F, M, D, G>) -> bool {
        let queue = Self::as_pin(&view.header().ready);
        queue.slot().is_empty() && queue.backend().is_empty()
    }

    pub(in crate::task::storage::set) fn drain<'a>(
        entries: Entries<'a, 'd, F, M, D, G>,
        maximum: usize,
    ) -> Drain<'a, 'd, F, M, D, G> {
        let (view, mut token) = entries.into_parts();
        let queue = Self::as_pin(&view.header().ready);
        for index in (0..maximum).map_while(|_| queue.slot().pop()) {
            queue.backend().enqueue(&view, &mut token, index);
        }
        let side = queue.backend().side();
        Drain::new(view, token, side)
    }

    pub(in crate::task::storage::set) fn as_pin(queue: &Self) -> pin::Pin<&Self> {
        // SAFETY: the ready queue is pinned for the lifetime of the retained view.
        unsafe { pin::Pin::new_unchecked(queue) }
    }

    pub(super) fn backend(self: pin::Pin<&Self>) -> &core::Core<'d, D> {
        &self.get_ref().core
    }

    pub(super) fn slot(self: pin::Pin<&Self>) -> &topology::Slot<'d, ready::raw::Index<D>> {
        &self.get_ref().slot
    }
}

const _: () = {
    use std::mem;

    use slot::raw::Queued;

    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<ready::raw::Index<()>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<topology::Endpoint<'static, ready::raw::Index<()>>>() == 2 * word);
    assert!(mem::size_of::<topology::Slot<'static, ready::raw::Index<()>>>() == 2 * word);
    assert!(mem::size_of::<topology::Edge<'static, 'static, ready::raw::Index<()>>>() == 2 * word);
    assert!(mem::size_of::<task::raw::Admission<'static>>() == 3 * word);
    assert!(mem::size_of::<task::raw::Root<'static>>() == 3 * word);
    assert!(
        mem::size_of::<Queue<'static, abi::Ready<()>, (), (), [u64; 2]>>()
            == mem::size_of::<Queue<'static, abi::Ready<()>, (), (), ()>>()
    );
    assert!(mem::size_of::<Queued<'static, 'static, abi::Ready<()>, (), ()>>() == word);
};
