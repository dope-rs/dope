use std::mem;

use crate::reply::{self, arena::state::order, order::Depth};

pub(super) enum Head<'a, 'arena, 'credits, 'region: 'arena, D, E, K = ()> {
    Empty(Depth),
    Attached(reply::Open<'a, 'arena, 'credits, 'region, D, E, K>),
    Draining(order::Slot<'a, 'arena, 'credits, 'region, K>),
    Drained(order::Slot<'a, 'arena, 'credits, 'region, K>),
}

const _: () = assert!(
    mem::size_of::<Head<'static, 'static, 'static, 'static, (), ()>>()
        <= 9 * mem::size_of::<usize>()
);
