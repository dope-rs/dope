use dope_core::driver::schedule::app::Work;
use o3::base::cell::region;

pub mod creation;
pub mod dispatch;
pub mod env;
mod host;
pub mod receive;

pub use host::Host;

/// Executor-owned scheduling capabilities lent to one application callback.
pub struct Park<'turn, 'r, 'd> {
    work: Work<'turn, 'd>,
    regions: &'r mut (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ),
}

impl<'turn, 'r, 'd> Park<'turn, 'r, 'd> {
    pub(crate) fn new(
        work: Work<'turn, 'd>,
        regions: &'r mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) -> Self {
        Self { work, regions }
    }

    pub fn work(&self) -> Work<'turn, 'd> {
        self.work
    }

    pub fn regions(
        &mut self,
    ) -> &mut (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ) {
        self.regions
    }
}

const _: () = assert!(size_of::<Park<'static, 'static, 'static>>() == 2 * size_of::<usize>());
