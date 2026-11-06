use core::pin;

use crate::{
    abi,
    task::storage::set::slot::{Running, Slot},
};

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Running<'a, 'd, F, M, D, G> {
    /// # Safety
    /// Retain the initialized fiber and root, excluding retirement. Mutation
    /// additionally requires this allocation's exclusive storage token.
    pub(in crate::task::storage::set) const unsafe fn new(
        slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
    ) -> Self {
        Self(slot)
    }
}
