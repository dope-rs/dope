use dope_core::driver::{
    route::KeyTag,
    settings,
    settings::{claim::Needs, errors},
};
use dope_manifold::{
    client::session::{Output, policy::output, turns},
    timing,
};
use o3::base::Call;

use crate::common::contract::Report;

pub(crate) fn flush<'d, S, O>(
    observe: O,
) -> Option<impl for<'a, 'c, 't, 'e> Call<output::Args<'a, 'c, 't, 'e, 'd, S, KeyTag<0>>>>
where
    S: Output<'d>,
    O: for<'a, 'c, 't, 'e> Fn(&'a mut S, &'c mut turns::Context<'t, 'e, 'd, S::ConnState, S::Send>),
{
    Some(
        move |(owner, context, _): output::Args<'_, '_, '_, '_, 'd, S, KeyTag<0>>| {
            observe(owner, context)
        },
    )
}

impl Report<error::Error, 16> {
    pub fn checked(&self, expected: (usize, usize)) {
        let faults = self.faults.values().expect("fault observations");
        assert!(faults.is_empty(), "{faults:?}");
        assert_eq!(
            (self.sent.get(), self.cancelled.get()),
            (Some(expected.0), Some(expected.1))
        );
    }
}

pub mod backoff;
pub mod error;
pub mod kind;
pub mod probe;
pub mod send;
pub mod side;
pub mod switch;

pub trait Configure: settings::Resource {
    fn configured(&self) -> Result<settings::Config, errors::Claim>;
}
impl<T: settings::Resource> Configure for T {
    fn configured(&self) -> Result<settings::Config, errors::Claim> {
        Needs::from_tree(self)
            .and_then(|needs| needs.apply_to(settings::Config::for_profile::<timing::Balanced>()))
    }
}

pub mod connect;
pub mod finish;
