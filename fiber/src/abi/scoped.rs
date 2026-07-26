use core::marker::PhantomPinned;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::Poll;

use super::Fiber;
use crate::Context;
use core::mem::replace;

pub trait ScopedFactory<O, F> {
    /// # Safety
    /// `owner` points to the pinned live owner for the returned fiber.
    unsafe fn make(self, owner: *mut O) -> F;
}

pub struct Scoped<O, M, F> {
    owner: MaybeUninit<O>,
    fiber: MaybeUninit<F>,
    owner_live: bool,
    fiber_live: bool,
    status: ScopedStatus<M>,
    _pin: PhantomPinned,
}

enum ScopedStatus<M> {
    Init(M),
    Live,
    Polling,
    Completed,
    Poisoned,
}

struct ScopedTransaction<M>(*mut ScopedStatus<M>, bool);

impl<M> ScopedTransaction<M> {
    fn finish(mut self, status: ScopedStatus<M>) {
        unsafe { *self.0 = status };
        self.1 = false;
    }
}

impl<M> Drop for ScopedTransaction<M> {
    fn drop(&mut self) {
        if self.1 {
            unsafe { *self.0 = ScopedStatus::Poisoned };
        }
    }
}

impl<O, M, F> Scoped<O, M, F> {
    pub fn new<'d>(_: *mut Context<'_, 'd>, owner: O, make: M) -> Self
    where
        M: ScopedFactory<O, F>,
        F: Fiber<'d>,
    {
        Self {
            owner: MaybeUninit::new(owner),
            fiber: MaybeUninit::uninit(),
            owner_live: true,
            fiber_live: false,
            status: ScopedStatus::Init(make),
            _pin: PhantomPinned,
        }
    }
}

impl<'d, O, M, F> Fiber<'d> for Scoped<O, M, F>
where
    M: ScopedFactory<O, F>,
    F: Fiber<'d>,
{
    type Output = (F::Output, O);

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match this.status {
            ScopedStatus::Completed => panic!("scoped fiber polled after completion"),
            ScopedStatus::Polling => panic!("scoped fiber polled reentrantly"),
            ScopedStatus::Poisoned => panic!("scoped fiber polled after panic"),
            ScopedStatus::Init(_) | ScopedStatus::Live => {}
        }
        let status = replace(&mut this.status, ScopedStatus::Polling);
        let transaction = ScopedTransaction(&mut this.status, true);
        if let ScopedStatus::Init(make) = status {
            this.fiber
                .write(unsafe { make.make(this.owner.as_mut_ptr()) });
            this.fiber_live = true;
        }
        let fiber = unsafe { Pin::new_unchecked(this.fiber.assume_init_mut()) };
        let Poll::Ready(output) = Fiber::poll(fiber, cx) else {
            transaction.finish(ScopedStatus::Live);
            return Poll::Pending;
        };
        this.fiber_live = false;
        unsafe { this.fiber.assume_init_drop() };
        this.owner_live = false;
        let owner = unsafe { this.owner.assume_init_read() };
        transaction.finish(ScopedStatus::Completed);
        Poll::Ready((output, owner))
    }
}

impl<O, M, F> Drop for Scoped<O, M, F> {
    fn drop(&mut self) {
        if self.fiber_live {
            self.fiber_live = false;
            unsafe { self.fiber.assume_init_drop() };
        }
        if self.owner_live {
            self.owner_live = false;
            unsafe { self.owner.assume_init_drop() };
        }
    }
}
