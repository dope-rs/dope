//! Typed terminal reply-slot views.

use o3::mem::rc;

use crate::reply::{
    Slot, retain,
    retain::grant::payload,
    usage::{self, Footprint},
};

pub use crate::reply::slot::Fault;

/// A reply slot proven to have completed without a failure value.
pub struct Completion<'a, 's, 'arena, 'credits, 'region, D: Footprint, E = D> {
    pub(in crate::reply) slot: Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
}

/// The exact terminal state observed for one reply slot.
pub enum Terminal<'a, 's, 'arena, 'credits, 'region, D: Footprint, E = D> {
    Complete(Completion<'a, 's, 'arena, 'credits, 'region, D, E>),
    Failed(Fault<'a, 's, 'arena, 'credits, 'region, D, E>),
}

impl<'s, 'arena, 'credits, 'region, D: Footprint, E>
    Completion<'_, 's, 'arena, 'credits, 'region, D, E>
{
    pub fn pop(&mut self) -> Option<D> {
        self.slot.pop()
    }

    /// Moves one data item and its accounting into one inseparable owner.
    pub fn try_lease(
        &mut self,
    ) -> Result<Option<retain::Lease<'arena, 'credits, 'region, D>>, rc::Error> {
        self.slot.try_lease()
    }

    /// Transfers the first data item's accounting to one clone-aware owner and
    /// consumes the unique completion capability.
    pub fn try_retain(
        self,
    ) -> Result<Option<retain::Retained<'arena, 'credits, 'region, D>>, rc::Error> {
        let Slot {
            state,
            owners,
            owner: _,
        } = self.slot;
        let Some(front) = state.data.front() else {
            return Ok(None);
        };
        // SAFETY: Completion retains this exact slot owner and its unchanged
        // first member; failure restores the member before its front can pop.
        let amount = payload::Value::new(front.get().amount.usage());
        let held = unsafe { owners.payload.remove(amount) }.try_retain()?;
        Ok(Some(retain::Retained::new(front.pop().value, held)))
    }
}

impl<'arena, 'credits, 'region, D: Footprint<Amount = usage::Bytes>, E>
    Completion<'_, '_, 'arena, 'credits, 'region, D, E>
{
    pub fn try_node(
        &mut self,
    ) -> Result<Option<retain::Node<'arena, 'credits, 'region, D>>, rc::Error> {
        self.slot.try_node()
    }
}
