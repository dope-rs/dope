//! Driver-owned request lowering and retained submission authority.
use std::mem;

use dope_sys::io::{request, request::Source};

use crate::{
    Backend,
    io::{fd::handles, socket},
};

pub(crate) mod bound;
mod connect;
mod datagram;
pub(crate) use connect::Connect;
pub(crate) mod permits;
pub(crate) use datagram::Datagram;

pub(crate) enum Context {}

pub(crate) type Borrowed<'a> =
    request::Borrowed<'a, <<Backend as Source<Context>>::Raw as request::Lower>::Target>;

impl request::Context for Context {
    type Target = handles::PhysicalSlot;
    type Outbound = handles::OutboundSlot;
    type Datagram<'d> = handles::Datagram<'d>;

    fn datagram<'d>(fd: &Self::Datagram<'d>) -> Self::Target {
        *fd.slot_ref()
    }
}

impl Lowered {
    pub(crate) fn target(
        slot: &handles::PhysicalSlot,
    ) -> <<Backend as Source<Context>>::Raw as request::Lower>::Target {
        <Backend as Source<Context>>::target(*slot)
    }

    pub(crate) fn socket(slot: handles::OutboundSlot, socket: socket::StreamSpec) -> Self {
        Self(request::Lower::socket(
            <Backend as Source<Context>>::outbound(slot),
            socket.domain(),
        ))
    }

    pub(crate) fn accept_multi(listener: &handles::PhysicalSlot) -> Self {
        Self(request::Lower::accept_multi(Self::target(listener)))
    }

    pub(crate) fn recv(slot: &handles::PhysicalSlot) -> Self {
        Self(request::Lower::recv::<
            { <Backend as Source<Context>>::GROUP },
        >(Self::target(slot)))
    }

    pub(super) fn recv_msg(fd: &handles::Datagram<'_>) -> Self {
        Self(request::Lower::recv_msg::<
            { <Backend as Source<Context>>::GROUP },
        >(<Backend as Source<Context>>::datagram(fd)))
    }

    pub(crate) fn shutdown(slot: &handles::PhysicalSlot) -> Self {
        Self(request::Lower::shutdown(Self::target(slot)))
    }
}

/// Request representation selected and proven by the active backend.
#[repr(transparent)]
pub(crate) struct Lowered(<Backend as Source<Context>>::Raw);

impl Lowered {
    fn into_inner(self) -> <Backend as Source<Context>>::Raw {
        self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Lowered>() == mem::size_of::<<Backend as Source<Context>>::Raw>());
    assert!(mem::align_of::<Lowered>() == mem::align_of::<<Backend as Source<Context>>::Raw>());
};
