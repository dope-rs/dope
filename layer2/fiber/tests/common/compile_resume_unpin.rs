use std::{marker::PhantomPinned, task::Poll};

use dope_fiber::{
    abi::{self, Resume},
    context::Admission,
};

struct Pinned(PhantomPinned);

impl<'d> Resume<'d> for Pinned {
    type Output = ();
    type Ready = ();

    fn poll<'slot, 'id>(
        _: &mut abi::Live<'slot, 'id, Self>,
        _: &mut Admission<'_, '_, 'd>,
    ) -> Poll<Self::Ready> {
        Poll::Ready(())
    }

    fn finish<'slot, 'id>(
        live: abi::Live<'slot, 'id, Self>,
        _: Self::Ready,
        _: Admission<'_, '_, 'd>,
    ) -> abi::Transition<'id, Self::Output> {
        let (vacant, value) = live.take();
        drop(value);
        vacant.complete(())
    }
}

fn main() {}
