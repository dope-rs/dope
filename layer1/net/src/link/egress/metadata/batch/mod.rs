pub(in crate::link::egress::metadata) mod chain;
mod draft;
mod empty;
mod error;
mod plan;
mod rejected;

pub(in crate::link::egress) use draft::Draft;
pub use empty::Empty;
pub use error::Error;
pub use plan::Plan;
pub use rejected::Rejected;

use crate::link::egress;

pub struct Sink<'batch, 'queue, 'token, 'p, 'd, T> {
    draft: &'batch mut Draft<'queue, 'token, 'p, 'd, T>,
}

impl<'batch, 'queue, 'token, 'p, 'd, T> Sink<'batch, 'queue, 'token, 'p, 'd, T> {
    pub(in crate::link::egress::metadata) fn new(
        draft: &'batch mut Draft<'queue, 'token, 'p, 'd, T>,
    ) -> Self {
        Self { draft }
    }

    pub fn try_push(&mut self, load: egress::NonEmpty<T>) -> Result<(), Rejected<T>> {
        self.draft.try_push(load)
    }
}

const _: () = assert!(
    std::mem::size_of::<Sink<'static, 'static, 'static, 'static, 'static, ()>>()
        == std::mem::size_of::<usize>()
);
