use o3::{
    alloc,
    depot::index::{bitmap::Fill, set},
};

use crate::{config, storage::queries::index, wire::tx};

/// Immutable configuration and transaction storage borrowed by one resolver state.
pub struct Store {
    pub(in crate::storage) config: config::Validated,
    pub(in crate::storage) ids: set::State<tx::Id, u16, 2, set::Disjoint>,
    pub(in crate::storage) pending: set::State<index::Branch, u16, { config::MAX_SERVERS + 1 }>,
}

impl Store {
    pub(in crate::storage) fn try_new(
        layout: index::Layout,
        config: config::Validated,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            config,
            ids: set::State::try_disjoint(layout.transactions(), set::Id::at::<0>())?,
            pending: set::State::try_new(
                layout.branches(),
                [Fill::Empty; config::MAX_SERVERS + 1],
            )?,
        })
    }
}
