use dope_core::driver::route::Identity;

use crate::{
    client::session::{self, policy},
    service::bindings::Bound,
};

impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Target<'d, ID, MAX>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Output<'life, 'd, P, ID, MAX>
        + policy::Lifecycle<'life, 'd, P, ID, MAX>
        + policy::Scheduling<'life, 'd, P, ID, MAX>,
    Self: session::Parser<'d, ID> + session::Retirement<'d, ID>,
{
}
