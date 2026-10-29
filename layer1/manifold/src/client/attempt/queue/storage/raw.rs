use dope_core::driver::route::Identity;
use o3::depot::{
    self,
    arena::slab,
    queue::{links, tagged},
};

use crate::client::attempt::queue::{storage::Occupied, table::Table};

// SAFETY: construction validates the immutable slab extent and its two private heads.
unsafe impl<'d, T: dope_net::Transport, ID: Identity> depot::raw::TagStore for Table<'d, T, ID> {
    type Head = links::Chain;
    type Heads = [links::Chain; 2];
    fn heads(&self) -> &Self::Heads {
        &self.heads
    }
    fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    unsafe fn hook(&self, index: links::Index) -> &tagged::Hook {
        // SAFETY: the private link retains initialization; hook is disjoint from payload access.
        unsafe {
            slab::Cell::project(depot::raw::Proof::new((&self.nodes, index, |node| {
                &raw const (*node).hook
            })))
        }
    }
}

// SAFETY: the resident retains exclusive payload access to this exact owner's initialized slot.
unsafe impl<'d, T: dope_net::Transport, ID: Identity> depot::raw::TagEntry
    for Occupied<'_, 'd, T, ID>
{
    type Store = Table<'d, T, ID>;
    fn store(&self) -> &Self::Store {
        self.table
    }
    fn hook(&self) -> &tagged::Hook {
        &self.entry.value().hook
    }
    fn index(&self) -> links::Index {
        self.key.handle.index().into()
    }
}
