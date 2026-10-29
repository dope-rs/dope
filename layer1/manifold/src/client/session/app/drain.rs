use dope_core::driver::route::{Identity, KeyTag};
use o3::base::cell::region;

use crate::client::{app, session};

pub struct Ready;
pub struct Application;

/// Selects a drain remainder only where the concrete session proves it valid.
pub trait Mode<'d, ID: Identity, N> {
    fn select<'session, 'barrier, 'region>(
        session: &'session N,
        barrier: app::Barrier<'barrier, 'd, ID>,
        region: &'region mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'barrier, 'd, ID>
    where
        'd: 'region,
        N: 'session;
}

pub trait Quiescence<'d, ID: Identity = KeyTag<0>>: session::Session<'d, ID> {
    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'a, 'd, ID>;
}

impl<'d, ID: Identity, N> Mode<'d, ID, N> for Ready {
    fn select<'session, 'barrier, 'region>(
        _: &'session N,
        barrier: app::Barrier<'barrier, 'd, ID>,
        _: &'region mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'barrier, 'd, ID>
    where
        'd: 'region,
        N: 'session,
    {
        barrier.ready()
    }
}

impl<'d, ID: Identity, N> Mode<'d, ID, N> for Application
where
    N: Quiescence<'d, ID>,
{
    fn select<'session, 'barrier, 'region>(
        session: &'session N,
        barrier: app::Barrier<'barrier, 'd, ID>,
        region: &'region mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'barrier, 'd, ID>
    where
        'd: 'region,
        N: 'session,
    {
        session.quiesce(barrier, region)
    }
}
