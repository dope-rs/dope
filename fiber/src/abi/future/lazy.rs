use core::pin::Pin;
use core::task::Poll;

use crate::{Context, Fiber};

pub struct Lazy<F, Fb> {
    state: LazyState<F, Fb>,
}

enum LazyState<F, Fb> {
    Pending(Option<F>),
    Active(Fb),
}

impl<F, Fb> Lazy<F, Fb> {
    pub const fn new(f: F) -> Self {
        Self {
            state: LazyState::Pending(Some(f)),
        }
    }
}

impl<'d, F, Fb> Fiber<'d> for Lazy<F, Fb>
where
    F: FnOnce() -> Fb,
    Fb: Fiber<'d>,
{
    type Output = Fb::Output;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if let LazyState::Pending(f) = &mut this.state {
            this.state = LazyState::Active(f.take().expect("lazy fiber polled after completion")());
        }
        let LazyState::Active(fiber) = &mut this.state else {
            unreachable!()
        };
        Fiber::poll(unsafe { Pin::new_unchecked(fiber) }, cx)
    }
}
