use std::mem;

use crate::storage::queries::index;

#[must_use]
#[repr(transparent)]
pub(in crate::storage::queries) struct Lease(index::Server);

impl Lease {
    pub(in crate::storage::queries::stream) const fn acquired(server: index::Server) -> Self {
        Self(server)
    }

    pub(in crate::storage::queries) const fn server(&self) -> index::Server {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Lease>() == 1);
const _: () = assert!(mem::size_of::<Option<Lease>>() == 1);
const _: () = assert!(!mem::needs_drop::<Lease>());
