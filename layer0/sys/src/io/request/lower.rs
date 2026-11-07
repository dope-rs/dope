/// Native requests whose input borrows are consumed during lowering.
pub trait Lower: Sized {
    type Target;
    type Outbound;
    type Domain;
    type Datagram;

    fn socket(slot: Self::Outbound, domain: Self::Domain) -> Self;
    fn accept_multi(listener: Self::Target) -> Self;
    fn recv<const GROUP: u16>(slot: Self::Target) -> Self;
    fn recv_msg<const GROUP: u16>(datagram: Self::Datagram) -> Self;
    fn shutdown(slot: Self::Target) -> Self;
}
