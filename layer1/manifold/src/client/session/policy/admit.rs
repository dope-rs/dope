use bindings::endpoint::State;
use dope_core::driver::route::Identity;

use crate::{
    client::{app, conn, session::protocol},
    service::bindings,
};

pub trait Register<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX>
{
    fn register(
        owner: &State<'life, 'd, P, Self, ID, MAX>,
        active: &conn::Activation<'_, 'd, ID>,
    ) -> app::Admission;
}
