use dope_core::{
    driver,
    driver::route::{Identity, KeyTag},
};
use dope_net::wire::{self, failure, open};

use crate::client::attempt;

/// Final typed sink for one wire-open failure.
pub trait Sink<
    'w,
    'd,
    W: wire::Spec<OpenFailure = M> + 'w,
    ID: Identity = KeyTag<0>,
    M = <W as wire::Spec>::OpenFailure,
>
{
    fn fail(
        &self,
        attempt: attempt::Completion<'_, 'd, ID>,
        cause: open::Error<'w, W>,
        driver: &mut driver::Context<'_, 'd>,
    );
}

impl<'w, 'd, W, P: ?Sized, ID: Identity> Sink<'w, 'd, W, ID, failure::Never> for P
where
    W: wire::Spec<OpenFailure = failure::Never> + 'w,
{
    fn fail(
        &self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: open::Error<'w, W>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        cause.eliminate()
    }
}
