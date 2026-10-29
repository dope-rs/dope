//! Private attempt payload access; guards never cross a callback or public return.

use std::cell::UnsafeCell;

use dope_core::driver::route::Identity;
pub use key::Key;
use o3::depot::{
    self,
    arena::slab::{self, entries, key::Observation},
    index::Unborrowed,
    queue::{links, tagged},
};
pub(super) use occupied::{Occupied, access::Access, borrow::Borrow, linked::Linked};

use crate::client::attempt::{
    self,
    queue::{entry::Entry, table::Table},
};

type Live<O, P> = depot::raw::Proof<(O, links::Index), Unborrowed<P>>;
type Resident<'a, 'd, T, ID> = slab::Borrowed<
    entries::Busy<'a, Node<'d, T, ID>, { u32::MAX }, entries::Interior>,
    attempt::Tag<'d, ID>,
>;

mod key;
mod occupied;

/// Admits the resident of the index that a retained proof already initialized.
///
/// # Safety
/// The caller's proof names an initialized, unborrowed slot of `table`.
pub(in crate::client::attempt::queue) unsafe fn admit<
    'a,
    'd,
    T: dope_net::Transport,
    ID: Identity,
>(
    table: &'a Table<'d, T, ID>,
    index: links::Index,
) -> (Resident<'a, 'd, T, ID>, &'a Table<'d, T, ID>) {
    use o3::depot::raw::Admit as _;

    // SAFETY: the caller's retained proof names this initialized, unborrowed slot,
    // so interior admission consumes it without a second busy probe.
    let live = unsafe { Live::<_, ()>::new(((&table.nodes, table), index)) };
    let ((entry, table), _) = live.admit().into_inner();
    (entry, table)
}

/// The bounded request and complete physical lookup observation.
#[derive(Debug)]
pub struct Error<Tag, const MAX: u32 = { u32::MAX }> {
    pub key: Key<Tag, MAX>,
    pub observed: Observation<MAX>,
}

/// The hook remains disjoint from the exclusively borrowed attempt payload.
pub(super) struct Node<'d, T: dope_net::Transport, ID: Identity> {
    hook: tagged::Hook,
    value: UnsafeCell<Entry<'d, T, ID>>,
}

impl<'d, T: dope_net::Transport, ID: Identity> Node<'d, T, ID> {
    pub(super) fn new(index: links::Index, value: Entry<'d, T, ID>) -> Self {
        Self {
            hook: tagged::Hook::new(index),
            value: UnsafeCell::new(value),
        }
    }

    fn state<'a>(entry: &'a mut Resident<'_, 'd, T, ID>) -> &'a mut Entry<'d, T, ID> {
        // SAFETY: the resident excludes payload reentry; neighbor edits only project disjoint hooks.
        unsafe { &mut *entry.value().value.get() }
    }
}

mod raw;

const _: () = {
    use std::mem;

    use dope_core::driver::route::KeyTag;
    use dope_net::tcp::Tcp;

    assert!(mem::size_of::<Node<'static, Tcp, KeyTag<0>>>() == 72);
    assert!(mem::align_of::<Node<'static, Tcp, KeyTag<0>>>() == 8);
};
