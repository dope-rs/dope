use crate::io::{
    request::{Context, Lower},
    socket,
};

/// Selects native requests and converts their proven input representations.
pub trait Source<C: Context> {
    type Raw: Lower<Domain = socket::Domain>;
    const GROUP: u16;

    fn target(slot: C::Target) -> <Self::Raw as Lower>::Target;
    fn outbound(slot: C::Outbound) -> <Self::Raw as Lower>::Outbound;
    fn datagram<'d>(fd: &C::Datagram<'d>) -> <Self::Raw as Lower>::Datagram;
}
