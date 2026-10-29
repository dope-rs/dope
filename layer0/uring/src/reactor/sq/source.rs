use dope_sys::{
    io::request::{Context, Source},
    memory::buffer::provided,
};

use crate::{Uring, reactor::sq};

impl<C: Context> Source<C> for Uring {
    type Raw = sq::Entry;
    const GROUP: u16 = <Self as provided::Source>::PROVIDED;

    fn target(slot: C::Target) -> sq::Fixed {
        sq::Fixed::new(sq::Index::from_le(slot.into()))
    }

    fn outbound(slot: C::Outbound) -> sq::Fixed {
        sq::Fixed::new(sq::Index::from_le(slot.into()))
    }

    fn datagram<'d>(fd: &C::Datagram<'d>) -> sq::Fixed {
        <Self as Source<C>>::target(C::datagram(fd))
    }
}
