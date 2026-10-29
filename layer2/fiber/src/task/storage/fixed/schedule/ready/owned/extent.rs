use std::{alloc::Layout, mem, num::NonZero};

use o3::alloc;

use crate::{
    core::driver::schedule::ready::task::owner,
    task::storage::fixed::schedule::ready::owned::{
        Dequeued, Prepared, Queue,
        store::{Node, Owner, Store},
    },
};

pub(super) struct Extent {
    pub(super) tasks: NonZero<u32>,
    pub(super) owners: owner::Capacity,
}

// SAFETY: both immutable nonzero counts fit the bounded, disjoint Owner/Node layout.
unsafe impl alloc::raw::Shape for Extent {
    type Root = Owner;
    type Tail = Node;
    type Extra = ();
    type Last = ();

    fn len(&self) -> usize {
        self.owners.count().get() as usize
    }

    fn tail_len(&self) -> usize {
        self.tasks.get() as usize
    }

    fn layout(&self) -> Layout {
        const {
            assert!(usize::BITS >= 64);
            assert!(mem::align_of::<Owner>() >= mem::align_of::<Node>());
            assert!(
                (u32::MAX as usize) * (mem::size_of::<Node>() + mem::size_of::<Owner>())
                    + mem::align_of::<Owner>()
                    <= isize::MAX as usize
            );
        }
        let size = self.offset() + self.tasks.get() as usize * mem::size_of::<Node>();
        unsafe { Layout::from_size_align_unchecked(size, mem::align_of::<Owner>()) }
    }
}

const _: () = {
    assert!(mem::size_of::<Node>() == 3 * mem::size_of::<u32>());
    assert!(mem::size_of::<Owner>() == 4 * mem::size_of::<u64>());
    assert!(!mem::needs_drop::<Node>());
    assert!(!mem::needs_drop::<Owner>());
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Queue<'static, ()>>() == 6 * word);
    assert!(mem::size_of::<Prepared<'static, 'static, ()>>() == 2 * word);
    assert!(mem::size_of::<Dequeued<'static, 'static, ()>>() == 2 * word);
    assert!(mem::size_of::<Option<Dequeued<'static, 'static, ()>>>() == 2 * word);
    assert!(mem::size_of::<Store<()>>() == 4 * mem::size_of::<usize>());
};
