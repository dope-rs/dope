//! Stable storage shared by reply slot views.

use o3::{alloc, depot};

use crate::reply::{
    self, layout,
    retain::pool::{Pool, Slots},
};

pub(in crate::reply) struct Backing<'credits, 'region, D> {
    pub(super) items: depot::raw::Arena<reply::Item<D>, (), false>,
    pub(in crate::reply) resources: Pool<'credits, 'region>,
}

impl<'credits, 'region, D> Backing<'credits, 'region, D> {
    pub(in crate::reply) fn try_new(
        layout: layout::Layout,
        slots: &'credits Slots<'region>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            items: depot::raw::Arena::try_new(layout.data())?,
            resources: Pool::try_new(layout, slots)?,
        })
    }
}
