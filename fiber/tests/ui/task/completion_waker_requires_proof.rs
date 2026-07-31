use std::pin::Pin;

use dope_fiber::raw::task::Context;

fn extract<'d>(context: Pin<&mut Context<'_, 'd>>) {
    let _ = context.as_ref().completion_waker_unchecked();
}

fn main() {}
