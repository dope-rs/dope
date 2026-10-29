use std::{marker, pin};

use dope_core::driver::{route::Identity, schedule, schedule::clock};
use o3::base::cell::region;

use crate::network::datagram::{self, hook};

pub struct Schedule<T = hook::Disabled>(marker::PhantomData<T>);

impl<'d, ID: Identity, H, T> hook::Schedule<'d, ID, H> for Schedule<T>
where
    H: datagram::Ingress<'d, ID> + datagram::Application<'d, ID>,
{
    type Plan<'turn> = clock::Clock<'turn, 'd>;

    fn select<'turn>(clock: clock::Clock<'turn, 'd>) -> Option<Self::Plan<'turn>> {
        Some(clock)
    }

    fn progress<'turn>(
        endpoint: pin::Pin<&hook::Endpoint<'d, ID, H>>,
        clock: Self::Plan<'turn>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        endpoint.project_ref().handler.progress(clock, token)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Schedule<hook::Enabled>>() == 0);
    assert!(!std::mem::needs_drop::<Schedule<hook::Enabled>>());
};
