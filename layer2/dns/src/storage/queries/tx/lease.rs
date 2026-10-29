use std::mem;

use o3::depot::index::set;

use crate::{
    storage::queries::{
        stream,
        tx::{Cookie, Epoch},
    },
    wire::{cookies, tx},
};

#[must_use = "an allocated DNS transaction must remain owned until release"]
pub(in crate::storage::queries) enum Lease<'s> {
    Udp {
        removed:
            set::Entry<&'s set::State<tx::Id, u16, 2, set::Disjoint>, tx::Id, u16, 2, set::Id<2>>,
        epoch: Epoch,
        cookie: Cookie,
    },
    Tcp {
        id: tx::Id,
        capacity: Tcp,
    },
}

#[must_use = "an allocated UDP transaction must enter the waiting state"]
pub(in crate::storage::queries) struct Udp<'s> {
    pub(in crate::storage::queries) removed:
        set::Entry<&'s set::State<tx::Id, u16, 2, set::Disjoint>, tx::Id, u16, 2, set::Id<2>>,
    epoch: Epoch,
    cookie: Cookie,
}

#[must_use = "an allocated TCP transaction must enter the waiting state"]
pub(in crate::storage::queries) struct Tcp;

#[must_use = "an admitted TCP transaction must enter the waiting state"]
pub(crate) struct Commit(tx::Id);

#[must_use = "a cancelled TCP transaction must release its occupied ID"]
pub(in crate::storage::queries) struct Cancel(tx::Id);

impl Lease<'_> {
    pub(in crate::storage::queries) fn id(&self) -> tx::Id {
        match self {
            Self::Udp { removed, .. } => tx::Id::from_ordinal(removed.index()),
            Self::Tcp { id, .. } => *id,
        }
    }
}

impl<'s> Udp<'s> {
    pub(in crate::storage::queries) const fn client(&self) -> cookies::Client {
        self.cookie.client()
    }

    pub(in crate::storage::queries) const fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub(in crate::storage::queries::tx) const fn new(
        removed: set::Entry<
            &'s set::State<tx::Id, u16, 2, set::Disjoint>,
            tx::Id,
            u16,
            2,
            set::Id<2>,
        >,
        epoch: Epoch,
        cookie: Cookie,
    ) -> Self {
        Self {
            removed,
            epoch,
            cookie,
        }
    }
}

impl Tcp {
    pub(in crate::storage::queries::tx) const fn new() -> Self {
        Self
    }

    pub(in crate::storage::queries) const fn commit<'s>(self, commit: Commit) -> Lease<'s> {
        Lease::Tcp {
            id: commit.0,
            capacity: self,
        }
    }

    pub(in crate::storage::queries) const fn cancel(self, returned: stream::Frame) -> Cancel {
        let Self = self;
        Cancel(returned.cancel())
    }
}

impl Commit {
    pub(in crate::storage::queries) const fn new(id: tx::Id) -> Self {
        Self(id)
    }
}

impl Cancel {
    pub(in crate::storage::queries::tx) const fn id(self) -> tx::Id {
        self.0
    }
}

impl<'s> From<Udp<'s>> for Lease<'s> {
    fn from(lease: Udp<'s>) -> Self {
        let Udp {
            removed,
            epoch,
            cookie,
        } = lease;
        Self::Udp {
            removed,
            epoch,
            cookie,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Tcp>() == 0);
    assert!(mem::size_of::<Commit>() == mem::size_of::<tx::Id>());
    assert!(mem::size_of::<Cancel>() == mem::size_of::<tx::Id>());
};
