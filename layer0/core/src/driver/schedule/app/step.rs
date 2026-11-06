use std::{marker, mem};

use crate::driver::schedule::app;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Flow {
    Batch,
    Yield,
}

#[must_use = "delegated admission must authorize exactly one transition"]
pub struct Delegated<'call, 'turn, 'd> {
    call: marker::PhantomData<fn(&'call mut ()) -> &'call mut ()>,
    brand: marker::PhantomData<app::Permit<'turn, 'd>>,
}

#[must_use = "delegated admission receipts must be settled"]
pub struct Receipt<'turn, 'd> {
    permit: app::Permit<'turn, 'd>,
}

impl<'turn, 'd> Receipt<'turn, 'd> {
    pub const fn direct(permit: app::Permit<'turn, 'd>) -> Self {
        Self { permit }
    }

    pub fn delegate<R>(
        permit: app::Permit<'turn, 'd>,
        run: impl for<'call> FnOnce(Delegated<'call, 'turn, 'd>) -> R,
    ) -> (R, Self) {
        let result = run(Delegated {
            call: marker::PhantomData,
            brand: marker::PhantomData,
        });
        (result, Self { permit })
    }

    pub fn settle(self, flow: Flow) -> Flow {
        let _permit = self.permit;
        flow
    }
}

const _: () = {
    assert!(mem::size_of::<Flow>() == 1);
    assert!(mem::size_of::<Delegated<'static, 'static, 'static>>() == 0);
    assert!(mem::size_of::<Receipt<'static, 'static>>() == 0);
};
