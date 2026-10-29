use std::{mem, net, num};

use o3::depot::{self, array, array::copy, index::Initialized};

use crate::{config, storage::queries::index};

#[derive(Clone, Copy)]
struct Row {
    address: net::SocketAddr,
    index: index::Server,
    /// Absolute position of the next initialized row in the same cycle.
    next: u8,
}

/// A nonempty immutable cycle constructed from one validated server list.
#[derive(Clone, Copy)]
pub(crate) struct Configured {
    rows: copy::Vec<Row, { config::MAX_SERVERS }>,
}

/// Selects one initialized row of an immutable cycle while retaining the borrow
/// that keeps the complete cycle alive.
#[derive(Clone, Copy)]
pub(crate) struct Selected<'s> {
    row: &'s Row,
    owner: &'s Configured,
}

impl Configured {
    pub(crate) fn map<T>(
        &self,
        mut map: impl FnMut(index::Server, net::SocketAddr) -> T,
    ) -> array::Vec<T, { config::MAX_SERVERS }> {
        let mut values = array::Vec::new();
        values.map_from(&self.rows, |_, row| map(row.index, row.address));
        values
    }

    pub(in crate::config) fn try_new(servers: config::Servers) -> Result<Self, config::Error> {
        let count = num::NonZero::new(servers.len() as u8).ok_or(config::Error::NoServers)?;
        let mut rows = copy::Vec::new();
        rows.map_from(&servers.values, |slot, address| Row {
            address: *address,
            next: if slot.index() + 1 == usize::from(count.get()) {
                0
            } else {
                (slot.index() + 1) as u8
            },
            index: index::Server::from(slot),
        });
        Ok(Self { rows })
    }

    pub(crate) fn first(&self) -> Selected<'_> {
        // SAFETY: construction initialized a nonempty prefix; this borrow prevents mutation.
        let row = unsafe { depot::raw::Proof::<_, Initialized<()>>::new((&self.rows, 0)) }.into();
        Selected { row, owner: self }
    }
}

const _: () = {
    assert!(mem::size_of::<Selected<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Selected<'static>>>() == 2 * mem::size_of::<usize>());
};

impl<'s> Selected<'s> {
    pub(crate) const fn index(self) -> index::Server {
        self.row.index
    }

    pub(crate) const fn address(self) -> net::SocketAddr {
        self.row.address
    }

    /// Follows the successor stored in the validated cyclic server table.
    pub(crate) fn next(self) -> Self {
        // SAFETY: this private row's successor names an initialized entry in
        // its exact immutable owner, retained by the same borrow.
        let row = unsafe {
            depot::raw::Proof::<_, Initialized<()>>::new((
                &self.owner.rows,
                usize::from(self.row.next),
            ))
        }
        .into();
        Self {
            row,
            owner: self.owner,
        }
    }
}
