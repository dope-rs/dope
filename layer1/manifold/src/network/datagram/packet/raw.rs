use o3::buffer::{self, storage::provided};

use crate::network::datagram::packet;

// SAFETY: shared access forwards the unchanged source retained by this packet.
unsafe impl<T: buffer::raw::Source> buffer::raw::Source for packet::Packet<'_, '_, T> {}

// SAFETY: AsMut forwards the same mutable descriptor and performs no writes.
unsafe impl<T: buffer::raw::Mutable> buffer::raw::Mutable for packet::Packet<'_, '_, T> {}

// SAFETY: shared access forwards the unchanged source retained by this prefix.
unsafe impl<T: buffer::raw::Source> buffer::raw::Source for packet::Head<'_, '_, '_, T> {}

// SAFETY: AsMut forwards the same mutable descriptor and performs no writes.
unsafe impl<T: buffer::raw::Mutable> buffer::raw::Mutable for packet::Head<'_, '_, '_, T> {}

// SAFETY: shared access forwards the unchanged frozen source retained by this packet.
unsafe impl<T: buffer::raw::Source> buffer::raw::Source for packet::Frozen<'_, '_, T> {}

// SAFETY: the view's Freeze contract preserves bytes and ownership; the turn is moved unchanged.
unsafe impl<'a, 'd, T: provided::Mutable<'d>> buffer::raw::Freeze for packet::Packet<'a, 'd, T> {
    type Output = packet::Frozen<'a, 'd, T::Frozen>;

    fn freeze(self) -> Self::Output {
        packet::Packet::freeze(self)
    }
}

// SAFETY: the view's Freeze contract preserves bytes and ownership; the head retains its turn borrow.
unsafe impl<'a, 'turn, 'd, T: provided::Mutable<'d>> buffer::raw::Freeze
    for packet::Head<'a, 'turn, 'd, T>
{
    type Output = packet::Head<'a, 'turn, 'd, T::Frozen>;

    fn freeze(self) -> Self::Output {
        packet::Head::freeze(self)
    }
}
