use dope_net::link::egress;

use crate::listener::writer::resources;

// SAFETY: the lease pins the header storage and removes every mutation path
// until egress drops this owner; RAII then returns the slot to its pool.
unsafe impl<'d, const ID: u8> egress::raw::Sealed<'d> for resources::Header<'d, ID> {
    fn retained_bytes(&self) -> usize {
        resources::WRITE_BUF_CAP
    }
}

// SAFETY: each variant preserves its owner's stable immutable bytes and
// reports the storage retained by that owner.
unsafe impl<'d, const ID: u8> egress::raw::Sealed<'d> for resources::Payload<'d, ID> {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Header(header) => egress::raw::Sealed::retained_bytes(header),
            Self::Body(body) => egress::raw::Sealed::retained_bytes(body),
        }
    }
}
