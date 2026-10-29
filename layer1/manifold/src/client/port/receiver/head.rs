use std::task;

use dope_core::driver::schedule::ready;
use dope_net::link::egress::{self, data, metadata::front, reserve::exact};
use o3::base::cell::region;

use crate::{app::dispatch::identity, client::port::receiver::pause::Pause};

/// An inspected head branded by the exact paused source that owns it.
pub(super) struct Head<'source, 'entry, 'e, 'd, S, K, I: identity::Identity> {
    source: &'source Pause<'entry, 'e, 'd, S, I>,
    kind: K,
}

impl<'source, 'entry, 'e, 'd, S: data::raw::Payload<'d>, K, I: identity::Identity>
    Head<'source, 'entry, 'e, 'd, S, K, I>
{
    pub(super) const fn new(source: &'source Pause<'entry, 'e, 'd, S, I>, kind: K) -> Self {
        Self { source, kind }
    }

    pub(super) const fn kind(&self) -> K
    where
        K: Copy,
    {
        self.kind
    }

    pub(super) fn take<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Option<front::Front<'entry, 'token, 'e, 'd, S>> {
        if !self.source.is_current() {
            return None;
        }
        self.source.slot().queue().take_front(token)
    }

    pub(super) fn transfer_to<'p, T, const IOV: usize>(
        self,
        token: &'_ mut region::Token<region::Identity<'d>>,
        queue: &egress::Queue<'_, 'p, 'd, IOV, T>,
        target: ready::Target<'d>,
    ) -> Option<task::Poll<Result<(), exact::Failure>>>
    where
        T: data::raw::Payload<'d>,
        for<'queue, 'token> front::Front<'queue, 'token, 'e, 'd, S>: front::Transfer<'d, T>,
    {
        let source = self.take(token)?;
        Some(front::Transfer::transfer_to(source, queue, target))
    }
}
