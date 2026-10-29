use std::mem;

use o3::depot::arena::linked::fifo::ends;

use crate::link::egress::metadata::pool::store::index::Index;

#[repr(transparent)]
pub(in crate::link::egress::metadata) struct Chain<'d>(ends::Exclusive<Option<Index<'d>>>);

impl<'d> Chain<'d> {
    pub(super) const fn new(links: ends::Exclusive<Option<Index<'d>>>) -> Self {
        Self(links)
    }

    pub(in crate::link::egress::metadata) fn links(
        &mut self,
    ) -> &mut ends::Exclusive<Option<Index<'d>>> {
        &mut self.0
    }
}

const _: () = assert!(
    mem::size_of::<Chain<'static>>() == mem::size_of::<ends::Exclusive<Option<Index<'static>>>>()
);
