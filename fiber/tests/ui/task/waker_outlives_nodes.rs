use std::pin::pin;

extern crate dope;
use dope_fiber::task::queue::TaskQueue;
use dope_fiber::task::{TaskContext, Waker};

fn escape<'d>() -> Waker<'d> {
    let queue = pin!(TaskQueue::new());
    let task = pin!(TaskContext::new());
    let binding = task.as_ref().bind(queue.as_ref(), 0, None);
    binding.waker()
}

fn main() {}
