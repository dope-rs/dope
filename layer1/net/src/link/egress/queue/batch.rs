use crate::link::egress::{self, load::credit, metadata::batch};

/// A borrowed final-link batch that publishes all of its entries together.
pub struct Batch<'batch, 'queue, 'token, 'p, 'd, B> {
    draft: &'batch mut batch::Draft<'queue, 'token, 'p, 'd, B, credit::Ready<'p, 'd>>,
}

impl<'batch, 'queue, 'token, 'p, 'd, B> Batch<'batch, 'queue, 'token, 'p, 'd, B> {
    pub(in crate::link::egress) fn new(
        draft: &'batch mut batch::Draft<'queue, 'token, 'p, 'd, B, credit::Ready<'p, 'd>>,
    ) -> Self {
        Self { draft }
    }

    pub fn try_push(&mut self, load: egress::NonEmpty<B>) -> Result<(), batch::Rejected<B>> {
        self.draft.try_push(load)
    }
}

const _: () = assert!(
    std::mem::size_of::<Batch<'static, 'static, 'static, 'static, 'static, ()>>()
        == std::mem::size_of::<usize>()
);
