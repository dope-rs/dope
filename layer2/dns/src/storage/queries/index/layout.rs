use std::mem;

use o3::depot::index::Domain;

use crate::{
    query::family,
    storage::{self, queries::index},
    wire::tx,
};

#[derive(Clone, Copy)]
pub(in crate::storage) struct Layout {
    lanes: Domain<index::Lane, u16>,
    branches: Domain<index::Branch, u16>,
    transactions: Domain<tx::Id, u16>,
}

impl Layout {
    const LANE_LIMIT: usize = (u16::MAX as usize - 1) / family::Version::COUNT;

    pub(in crate::storage) fn new(lanes: usize) -> Result<Self, storage::Error> {
        if lanes == 0 {
            return Err(storage::Error::Config(crate::config::Error::LaneBound {
                requested: lanes,
                minimum: 1,
            }));
        }
        let lanes = Domain::try_new(lanes).map_err(storage::Error::Lanes)?;
        let count = lanes.capacity() * family::Version::COUNT;
        if count >= usize::from(u16::MAX) {
            return Err(storage::Error::Config(
                crate::config::Error::LaneAddressSpace {
                    requested: lanes.capacity(),
                    limit: Self::LANE_LIMIT,
                },
            ));
        }
        let branches = Domain::try_new(count).map_err(storage::Error::Branches)?;
        let ids = Domain::try_new(1 << 16).map_err(storage::Error::Transactions)?;
        Ok(Self {
            lanes,
            branches,
            transactions: ids,
        })
    }

    pub(in crate::storage) const fn lanes(self) -> Domain<index::Lane, u16> {
        self.lanes
    }

    pub(in crate::storage) const fn branches(self) -> Domain<index::Branch, u16> {
        self.branches
    }

    pub(in crate::storage) const fn transactions(self) -> Domain<tx::Id, u16> {
        self.transactions
    }
}

const _: () = {
    let lane = index::Lane::from_raw(32_766);
    assert!(index::Branch::new(lane, family::Version::Four).raw() == 65_532);
    assert!(index::Branch::new(lane, family::Version::Six).raw() == 65_533);
    assert!(index::Branch::new(lane, family::Version::Six).raw() != u16::MAX);
};

const _: () = assert!(mem::size_of::<Layout>() == 3 * mem::size_of::<Domain>());
