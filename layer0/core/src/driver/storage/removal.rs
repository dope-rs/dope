use dope_sys::engine;

use crate::{
    RemoveSlot,
    driver::result::{IoOperation, Kernel},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Removal {
    operation: IoOperation,
    slot: RemoveSlot,
    cause: Kernel,
}

impl<S> Removal
where
    crate::Backend: engine::Removal<Slot = S>,
{
    pub(crate) fn native(((op, cause), slot): ((IoOperation, Kernel), S)) -> Self {
        Self {
            operation: op,
            slot,
            cause,
        }
    }
}
