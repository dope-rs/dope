use dope::{core::driver::route::Identity, manifold::app::dispatch};

use crate::{Report, net::udp::Udp};

// SAFETY: the fixed Transit hooks submit only through the owned DNS socket.
// Its probe includes every send flight and receive arm through retirement;
// query storage and reports have no retained native operation of their own.
unsafe impl<
    'life,
    's,
    'd: 'life,
    ID: Identity,
    const LANES: usize,
    const ADDRESSES: usize,
    R: Report,
> dispatch::raw::Resume<'d> for Udp<'life, 's, 'd, ID, LANES, ADDRESSES, R>
{
}
