use std::convert::Infallible;

use dope_core::driver::{
    settings,
    settings::{claim::Needs, errors},
};
use dope_manifold::{
    client::session::{Output, policy::output, turns},
    timing,
};
use o3::base::Call;

use crate::common::contract::Report;

/// Builds the flush observation every fixture shares.
///
/// The flush call has the same shape in every scenario fixture; only the
/// assertion about the flushing owner differs, so the fixture supplies that.
pub(crate) fn flush<'d, S, O>(
    observe: O,
) -> impl for<'a, 'c, 't, 'e> Call<(
    &'a mut S,
    &'c mut turns::Context<'t, 'e, 'd, S::ConnState, S::Send>,
)>
where
    S: Output<'d>,
    O: for<'a, 'c, 't, 'e> Fn(&'a mut S, &'c mut turns::Context<'t, 'e, 'd, S::ConnState, S::Send>),
{
    output::Flush::<Infallible, _>::Context(
        move |(owner, context): (
            &mut S,
            &mut turns::Context<'_, '_, 'd, S::ConnState, S::Send>,
        )| observe(owner, context),
    )
}

impl Report<error::Error, 16> {
    pub fn checked(&self, expected: (usize, usize)) {
        let faults = self.faults.values().expect("fault observations");
        let result = error::Error::check(&faults);
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            (self.sent.get(), self.cancelled.get()),
            (Some(expected.0), Some(expected.1))
        );
    }
}

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
