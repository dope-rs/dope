use std::marker;

use dope_sys::table::token::host;

use crate::{
    driver::{
        self,
        retained::flight::{GENERATION_BITS, INDEX_BITS, Mismatch, Target, storage},
        route::{self, kind},
    },
    io::event::batch::Flow,
};

#[must_use = "a validated flight completion must be committed"]
pub(crate) struct Complete<'q, 'd, C: kind::Class = kind::Any> {
    pub(super) completion: storage::Complete<'q, Target, INDEX_BITS, GENERATION_BITS>,
    pub(super) driver: driver::Reference<'d>,
    pub(super) class: marker::PhantomData<C>,
}

impl<'q, 'd> Complete<'q, 'd> {
    pub(crate) fn bind<C: kind::Flight>(self, proof: kind::Witness<C>) -> Complete<'q, 'd, C> {
        Complete {
            completion: self.completion,
            driver: self.driver,
            class: proof.marker(),
        }
    }

    pub(crate) fn kind(&self) -> kind::Native {
        self.completion.value().kind
    }
}

impl<'q, 'd, C: kind::Flight> TryFrom<Complete<'q, 'd>> for Complete<'q, 'd, C> {
    type Error = Mismatch<'q, 'd>;

    fn try_from(bound: Complete<'q, 'd>) -> Result<Self, Self::Error> {
        let observed = bound.kind();
        let Some(proof) = observed.witness() else {
            return Err(Mismatch {
                bound,
                expected: C::FLIGHT,
                observed,
            });
        };
        Ok(bound.bind(proof))
    }
}

impl<'d, C: kind::Class> From<&Complete<'_, 'd, C>> for route::Erased<'d, C> {
    fn from(bound: &Complete<'_, 'd, C>) -> Self {
        bound.completion.value().bind(bound.driver.brand)
    }
}

impl<'d, C: kind::Class> Complete<'_, 'd, C> {
    pub(crate) const fn driver(&self) -> driver::Reference<'d> {
        self.driver
    }

    pub(crate) fn complete(self) -> route::Erased<'d, C> {
        self.completion.complete().bind(self.driver.brand)
    }

    pub(crate) fn settle(self, flow: Flow) -> route::Erased<'d, C> {
        self.completion.settle(flow).bind(self.driver.brand)
    }
}

impl<C: kind::Class> host::Flight<Flow> for Complete<'_, '_, C> {
    type Key = super::raw::Echo;

    fn finish(self) {
        self.completion.finish();
    }

    fn end(self, flow: Flow) -> Self::Key {
        let echo = super::raw::Echo::from_inner(self.completion.echo());
        if flow == Flow::Last {
            self.completion.finish();
        }
        echo
    }
}
