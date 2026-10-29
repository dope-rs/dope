//! Driver-owned request lowering and retained submission authority.
use std::mem;

use dope_sys::io::{
    request,
    request::Source,
    socket::addr::{Length, Storage},
};

use crate::{
    Backend,
    io::{fd::handles, socket, socket::msg, transfer},
};

pub(crate) mod bound;
mod connect;
mod datagram;
pub(crate) use connect::Connect;
pub(crate) mod permits;
pub(crate) mod raw;
pub(crate) use datagram::Datagram;

pub(crate) enum Context {}

impl request::Context for Context {
    type Target = handles::PhysicalSlot;
    type Outbound = handles::OutboundSlot;
    type Datagram<'d> = handles::Datagram<'d>;

    fn datagram<'d>(fd: &Self::Datagram<'d>) -> Self::Target {
        *fd.slot_ref()
    }
}

impl Lowered {
    fn target(
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

impl<'a> Borrowed<'a> {
    pub(crate) fn accept(
        listener: &'a handles::PhysicalSlot,
        peer: &'a mut Storage<Length>,
    ) -> Self {
        let (address, len) = peer.output_mut();
        Self(request::Borrow::accept(
            Lowered::target(listener),
            address,
            len,
        ))
    }

    pub(crate) fn send(slot: &'a handles::PhysicalSlot, buffer: transfer::View<'a>) -> Self {
        Self(request::Borrow::send(
            Lowered::target(slot),
            buffer.into_prefix(),
        ))
    }

    pub(crate) fn send_msg(slot: &'a handles::PhysicalSlot, message: msg::Message<'a>) -> Self {
        Self(request::Borrow::send_msg(Lowered::target(slot), message))
    }

    pub(crate) fn connect<A: socket::Prepared + 'a>(
        slot: handles::PhysicalSlot,
        addr: &'a A,
    ) -> Self {
        Self(request::Borrow::connect(
            Lowered::target(&slot),
            addr.view().native(),
        ))
    }
}

/// Request representation selected and proven by the active backend.
#[repr(transparent)]
pub(crate) struct Lowered(<Backend as Source<Context>>::Raw);

/// Request representation retaining every caller-owned borrow in its type.
#[repr(transparent)]
pub(crate) struct Borrowed<'a>(<<Backend as Source<Context>>::Raw as request::Lower>::Borrowed<'a>);

impl Lowered {
    fn into_inner(self) -> <Backend as Source<Context>>::Raw {
        self.0
    }
}

impl<'a> Borrowed<'a> {
    fn into_inner(self) -> <<Backend as Source<Context>>::Raw as request::Lower>::Borrowed<'a> {
        self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Lowered>() == mem::size_of::<<Backend as Source<Context>>::Raw>());
    assert!(mem::align_of::<Lowered>() == mem::align_of::<<Backend as Source<Context>>::Raw>());
    assert!(
        mem::size_of::<Borrowed<'static>>()
            == mem::size_of::<
                <<Backend as Source<Context>>::Raw as request::Lower>::Borrowed<'static>,
            >()
    );
    assert!(
        mem::align_of::<Borrowed<'static>>()
            == mem::align_of::<
                <<Backend as Source<Context>>::Raw as request::Lower>::Borrowed<'static>,
            >()
    );
};
