use std::task::Poll;

use crate::{
    abi,
    context::{self, wake},
    core::driver::schedule::ready::task::{pool, topology},
    task::{batch, storage::slots::Index},
};

/// Reusable driver-scoped task credits and stable-ready storage.
pub struct Pool<'d, const N: usize> {
    pub(super) tasks: pool::Pool<'d, batch::Tag, N>,
    pub(super) ready: topology::Endpoint<'d, Index<N>>,
}

struct Acquire<const N: usize>;

impl<'d, const N: usize> Pool<'d, N> {
    /// Reserves a batch pool under an existing driver-owned root.
    pub fn try_new(parent: wake::RootWaker<'d>) -> Result<Self, batch::Error> {
        let parent: wake::Waker<'d> = parent.into();
        let ready = topology::Endpoint::try_new(parent.0).map_err(batch::Error::Task)?;
        let tasks = pool::Pool::try_new(parent.0).map_err(batch::Error::Task)?;
        Ok(Self { tasks, ready })
    }

    /// Reserves one reusable fixed-capacity batch pool in the current driver.
    pub fn acquire() -> impl abi::Fiber<'d, Output = Result<Self, batch::Error>> {
        Acquire::<N>
    }
}

impl<'d, const N: usize> abi::Fiber<'d> for Acquire<N> {
    type Output = Result<Pool<'d, N>, batch::Error>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let context = call.into_context();
        Poll::Ready(Pool::try_new(context.as_ref().root_waker()))
    }
}

const _: () =
    assert!(core::mem::size_of::<Pool<'static, 1>>() == 4 * core::mem::size_of::<usize>());
const _: () = assert!(
    core::mem::size_of::<Pool<'static, 4096>>() == core::mem::size_of::<Pool<'static, 1>>()
);
