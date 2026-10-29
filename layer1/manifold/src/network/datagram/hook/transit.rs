use std::{marker, pin};

use dope_core::driver::{
    route::Identity,
    schedule::{
        clock, {self},
    },
};
use o3::base::cell::region;

use crate::network::{
    datagram,
    datagram::hook::{Disabled, Endpoint, Schedule},
};

pub struct Transit<T = Disabled>(marker::PhantomData<T>);

impl<'d, ID: Identity, H, T> Schedule<'d, ID, H> for Transit<T>
where
    H: datagram::Ingress<'d, ID> + datagram::Transport<'d, ID>,
{
    type Plan<'turn> = clock::Clock<'turn, 'd>;

    fn select<'turn>(clock: clock::Clock<'turn, 'd>) -> Option<Self::Plan<'turn>> {
        Some(clock)
    }

    fn progress<'turn>(
        endpoint: pin::Pin<&Endpoint<'d, ID, H>>,
        clock: Self::Plan<'turn>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        endpoint.project_ref().handler.progress(clock, token)
    }
}
