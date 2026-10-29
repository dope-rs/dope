use std::marker;

use dope_core::driver::{
    route::Identity,
    settings::{self, claim, demands, errors},
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
        needs.include(
            claim::Claim::new(settings::Routes::singleton::<ID>())
                .files(demands::Files::only_out(demands::Outbound::fixed::<1>()))
                .schedule(demands::Schedule::fixed::<1, 1>()),
        )
    }
}

const _: () = assert!(
    std::mem::size_of::<InFlight<'static, 'static, 1, 1>>() == 4 * std::mem::size_of::<usize>()
);
