use std::{cell::Cell, convert};

use dope_fiber::{
    reply,
    reply::{
        recv, usage,
        usage::{Bytes, Usage, Values},
    },
};

use crate::common::storage::replies::support::{Counted, Drops};

pub struct Deferred;

impl<'s, 'arena, 'credits, 'region, 'a> recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>>
    for Deferred
{
    type Output = Counted<'a>;

    type Context = &'a Cell<bool>;

    fn extract(
        enabled: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        if enabled.get() {
            slot.pop_error()
        } else {
            None
        }
    }
}

pub struct Mismatch<'a>(pub(crate) &'a Drops);

impl<'a> usage::Build<Counted<'a>> for Mismatch<'a> {
    type Error = convert::Infallible;

    fn usage(&self) -> usage::Usage {
        Usage::new(Bytes::ZERO, Values::new(1))
    }

    fn try_build(self) -> Result<Counted<'a>, (Self, Self::Error)> {
        Ok(Counted {
            failure: Some(1),
            bytes: 0,
            drops: self.0,
        })
    }
}
