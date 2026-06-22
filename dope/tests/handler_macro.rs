use std::task::{Context, Poll, Waker};

use o3::task::InlineFuture;

#[dope_gen::handler]
async fn echo(x: u32) -> u32 {
    x + 1
}

#[dope_gen::handler]
async fn await_chain(x: u32) -> u32 {
    let a = ready_value(x).await;
    ready_value(a + 10).await
}

#[dope_gen::handler(size = 512)]
async fn big_handler(x: u32) -> u32 {
    let buf = [x; 16];
    let sum: u32 = buf.iter().sum();
    sum
}

async fn ready_value(v: u32) -> u32 {
    v
}

fn poll_until_ready<Out: 'static>(mut fut: InlineFuture<'static, Out, 256>) -> Out {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        match unsafe { fut.poll_pinned(&mut cx) } {
            Poll::Ready(v) => return v,
            Poll::Pending => continue,
        }
    }
}

#[test]
fn echo_returns_inline_future() {
    let _fut: InlineFuture<'static, u32, 256> = echo(5);
}

#[test]
fn echo_polls_to_ready() {
    assert_eq!(poll_until_ready(echo(7)), 8);
}

#[test]
fn nested_awaits_run_to_completion() {
    assert_eq!(poll_until_ready(await_chain(1)), 11);
}

fn poll_512<Out: 'static>(mut fut: InlineFuture<'static, Out, 512>) -> Out {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        match unsafe { fut.poll_pinned(&mut cx) } {
            Poll::Ready(v) => return v,
            Poll::Pending => continue,
        }
    }
}

#[test]
fn handler_with_explicit_size() {
    let _typecheck: InlineFuture<'static, u32, 512> = big_handler(3);
    assert_eq!(poll_512(big_handler(3)), 48);
}
