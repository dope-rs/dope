use crate::driver::schedule::ready;

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Record {
    parent: ready::Stored,
    topology: ready::RootIndex,
    local: u32,
}

impl Record {
    pub(super) const fn new(parent: ready::Stored, topology: ready::RootIndex, local: u32) -> Self {
        Self {
            parent,
            topology,
            local,
        }
    }

    pub(super) const fn topology(self) -> ready::RootIndex {
        self.topology
    }

    pub(super) const fn local(self) -> u32 {
        self.local
    }

    pub(super) const fn parent(self) -> ready::Stored {
        self.parent
    }
}

const _: () = assert!(core::mem::size_of::<Record>() == 2 * core::mem::size_of::<u64>());
