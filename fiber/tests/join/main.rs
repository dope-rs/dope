#![deny(unsafe_code)]

use std::{mem::size_of, task::Poll};

use dope_fiber::{
    abi::{Fiber, Join},
    context::PollCall,
};

struct Large([u8; 256]);

impl<'d> Fiber<'d> for Large {
    type Output = [u8; 256];

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (self_, _) = call.into_parts();
        Poll::Ready(self_.0)
    }
}

#[test]
fn join_reuses_each_child_slot_for_its_output() {
    let separate_storage =
        2 * size_of::<Large>() + 2 * size_of::<Option<<Large as Fiber<'static>>::Output>>();
    assert!(size_of::<Join<'static, Large, Large>>() < separate_storage);
}
