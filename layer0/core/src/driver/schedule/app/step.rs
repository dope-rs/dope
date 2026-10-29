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

pub trait Ready: Sized {
    type Item;

    fn rearm(self);

    fn take(self) -> Self::Item;
}

pub struct Checkpoint<T, Take, Rearm> {
    take: Take,
    rearm: Rearm,
    item: marker::PhantomData<fn() -> T>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Step<'turn, 'd> {
    work: app::Work<'turn, 'd>,
}

impl Delegated<'_, '_, '_> {
    pub fn run<R>(self, f: impl FnOnce() -> R) -> R {
        f()
    }
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
        self.permit.run(|| flow)
    }
}

impl<T, Take, Rearm> Checkpoint<T, Take, Rearm>
where
    Take: FnOnce() -> T,
    Rearm: FnOnce(),
{
    pub const fn new(take: Take, rearm: Rearm) -> Self {
        Self {
            take,
            rearm,
            item: marker::PhantomData,
        }
    }
}

impl<T, Take, Rearm> Ready for Checkpoint<T, Take, Rearm>
where
    Take: FnOnce() -> T,
    Rearm: FnOnce(),
{
    type Item = T;

    fn rearm(self) {
        (self.rearm)();
    }

    fn take(self) -> Self::Item {
        (self.take)()
    }
}

impl<'turn, 'd> Step<'turn, 'd> {
    pub const fn new(work: app::Work<'turn, 'd>) -> Self {
        Self { work }
    }

    pub fn batch<R: Ready>(self, ready: R) -> Option<R::Item> {
        match self.work.permit() {
            Some(permit) => Some(permit.run(|| ready.take())),
            None => {
                ready.rearm();
                None
            }
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Flow>() == 1);
    assert!(mem::size_of::<Delegated<'static, 'static, 'static>>() == 0);
    assert!(mem::size_of::<Receipt<'static, 'static>>() == 0);
    assert!(mem::size_of::<Step<'static, 'static>>() == mem::size_of::<usize>());
};
