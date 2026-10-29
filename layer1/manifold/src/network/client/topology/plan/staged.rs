use dope_core::driver::route::Identity;

use crate::client::session::{self, codec};

pub trait Staged<'d, ID: Identity, P>:
    session::Session<'d, ID, Codec: codec::Codec<Staging = P>>
{
}

impl<'d, ID: Identity, N, P> Staged<'d, ID, P> for N
where
    N: session::Session<'d, ID>,
    N::Codec: codec::Codec<Staging = P>,
{
}
