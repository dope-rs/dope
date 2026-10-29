use std::{task, task::Poll};

use dope_fiber::{
    abi, context, reply,
    reply::order::{Depth, Front},
};

use crate::common::storage::replies::support::{Counted, Drops};

pub struct Step<'a, 's, 'arena, 'credits, 'region> {
    pub(crate) drops: &'a Drops,
    pub(crate) lane: reply::Lane<'s, 'arena, 'credits, 'region, Counted<'a>>,
}

impl<'region> abi::Fiber<'region> for Step<'_, '_, '_, '_, 'region> {
    type Output = (usize, Front, Depth);

    fn poll(call: context::PollCall<'_, '_, 'region, Self>) -> task::Poll<Self::Output> {
        let (step, mut context) = call.into_parts();
        if step.drops.count() == 0 {
            context.as_ref().completion().wake();
            return Poll::Pending;
        }
        let token = context.as_mut().region_token();
        Poll::Ready((
            step.drops.count(),
            step.lane.select(token).front(),
            step.lane.complete(token),
        ))
    }
}
