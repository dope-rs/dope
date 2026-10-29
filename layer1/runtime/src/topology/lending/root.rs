use std::pin;

use crate::client;

/// Projects the unique client provider embedded in a pinned owner.
pub trait Root<'d> {
    type Provider: client::Provider<'d> + ?Sized;

    fn provider(self: pin::Pin<&Self>) -> pin::Pin<&Self::Provider>;
}

impl<'d, P> Root<'d> for P
where
    P: client::Provider<'d>,
{
    type Provider = P;

    fn provider(self: pin::Pin<&Self>) -> pin::Pin<&Self::Provider> {
        self
    }
}
