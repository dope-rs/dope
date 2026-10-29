use std::{mem, task};

use dope_manifold::client::attempt::{self, queue};
use dope_net::tcp::Tcp;
use dope_runtime::executor::root;

pub mod empty;

pub struct Cancellation<'source, 'd> {
    pub(crate) state: Stage<'source, 'd>,
}

pub enum Stage<'source, 'd> {
    Pending(queue::Lease<'source, 'd, Tcp>),
    Armed(queue::Lease<'source, 'd, Tcp>),
    Done,
}

#[derive(Debug)]
pub struct Completed;

impl<'d> root::Root<'d> for Cancellation<'_, 'd> {
    type Output = Result<attempt::Id<'d>, Completed>;

    fn poll(context: root::Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, wake, _, _) = context.into_parts();
        let this = root.get_mut();
        match mem::replace(&mut this.state, Stage::Done) {
            Stage::Pending(lease) => {
                this.state = Stage::Armed(lease);
                wake.wake();
                task::Poll::Pending
            }
            Stage::Armed(lease) => {
                let key = lease.id();
                lease.cancel();
                task::Poll::Ready(Ok(key))
            }
            Stage::Done => task::Poll::Ready(Err(Completed)),
        }
    }
}
