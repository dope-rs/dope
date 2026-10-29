use dope_sys::{
    io::{reactor::ready::delivery::entry, socket},
    table::{self, change},
};
use o3::mem::Rejected;

use crate::reactor::entry::{Key, Slots};

pub(super) struct Create<C: entry::Context> {
    pub(super) slot: C::Outbound,
    pub(super) result: Result<C::Handle, socket::Create>,
}

impl<C: entry::Context> Create<C> {
    pub(super) fn try_creation<'q, 'd, B>(
        slot: C::Outbound,
        key: Key<C>,
        driver: C::Driver,
        target: C::Target,
    ) -> Result<C::Creation, Rejection<C::Creation, C::Error>>
    where
        C: entry::Socket<'q, 'd, B>,
    {
        let owned: <Slots<C> as table::raw::Slots>::Owned = slot.into();
        let queued: <Slots<C> as change::raw::Slots>::Physical = owned.into();
        let Some(creation) = C::creation(driver, target) else {
            return Err(Rejected::new(None, C::reject(key, queued, None, target)));
        };
        let owned: <Slots<C> as table::raw::Slots>::Owned = (*creation.as_ref()).into();
        let resolved: <Slots<C> as change::raw::Slots>::Physical = owned.into();
        if resolved != queued {
            return Err(Rejected::new(
                Some(creation),
                C::reject(key, queued, Some(resolved), target),
            ));
        }
        Ok(creation)
    }
}

/// A rejected installation returns the exact creation if it was acquired.
type Rejection<C, E> = Rejected<Option<C>, E>;
