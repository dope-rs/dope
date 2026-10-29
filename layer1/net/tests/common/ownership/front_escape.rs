use std::num::NonZero;

use dope_net::link::egress::metadata::{Lane, arena};
use o3::{alloc, base::cell::region};

fn reopen<'d>(
    store: &mut arena::Store<'d>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Result<Option<&'d [u8]>, alloc::Error> {
    let front = {
        let arena = arena::Arena::<&[u8]>::try_new(token, store)?;
        let Some(slot) = arena.get(Lane::at(NonZero::<u32>::MIN)) else {
            return Ok(None);
        };
        let Some(front) = slot.queue().take_front(token) else {
            return Ok(None);
        };
        front
    };
    drop(arena::Arena::<&[u8]>::try_new(token, store)?);
    Ok(Some(*front.get()))
}

fn main() {}
