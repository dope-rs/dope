use std::convert::Infallible;
use std::pin::Pin;
use std::task::Poll;

extern crate dope;
use dope_fiber::{Context, Fiber, FiberScope, OwnerFiber, SplitBytes};
use dope_test::with_session;
use o3::buffer::Shared;

struct Escaping<'a>(&'a [u8]);

impl<'a, 'd> Fiber<'d> for Escaping<'a> {
    type Output = &'a [u8];

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Poll::Ready(self.0)
    }
}

fn require_fiber<'d>(_: impl Fiber<'d>) {}

fn main() {
    with_session(|session| {
        let owner = SplitBytes::new(Shared::copy_from_slice(b"request"), None, 7);
        let task =
            OwnerFiber::try_from_split(owner, FiberScope::from_driver(session.driver()), |view| {
                Ok::<_, Infallible>(Escaping(view.head()))
            })
            .unwrap();
        require_fiber(task);
    });
}
