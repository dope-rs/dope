use std::marker;

use dope_core::driver::{
    route::Identity,
    settings::{self, claim, errors},
};

use crate::file::appender::InFlight;

/// Canonical driver-resource layout for one asynchronous appender route.
pub struct Layout<ID: Identity>(marker::PhantomData<fn() -> ID>);

impl<ID: Identity> Default for Layout<ID> {
    fn default() -> Self {
        Self(marker::PhantomData)
    }
}

impl<ID: Identity> settings::Resource for Layout<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(claim::Claim::new(settings::Routes::singleton::<ID>()))
    }
}

const _: () = assert!(
    std::mem::size_of::<InFlight<'static, 'static, 1>>() == 4 * std::mem::size_of::<usize>()
);
