use std::marker;

use crate::client::session::ingress;

/// One-shot session construction with its exact ingress proof.
pub struct Setup<'d, N, S> {
    session: N,
    ingress: ingress::Plan<S>,
    brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, N, S> Setup<'d, N, S> {
    pub const fn new(inner: N, plan: ingress::Plan<S>) -> Self {
        Self {
            session: inner,
            ingress: plan,
            brand: marker::PhantomData,
        }
    }

    pub(super) fn into_parts(self) -> (N, ingress::Plan<S>) {
        (self.session, self.ingress)
    }
}
