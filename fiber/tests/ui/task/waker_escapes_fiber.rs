use std::cell::Cell;
use std::pin::Pin;
use std::task::Poll;

extern crate dope;
use dope_fiber::abi::Fiber;
use dope_fiber::task::{Context, Waker};

struct Escape<'d>(&'d Cell<Option<Waker<'d>>>);

impl<'d> Fiber<'d> for Escape<'d> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        self.0.set(Some(cx.waker()));
        Poll::Pending
    }
}

fn main() {}
