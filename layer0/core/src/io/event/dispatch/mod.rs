use std::{marker::PhantomData, mem, ops};

use o3::mem::slot;

use crate::{driver::route, io};

use super::{
    Kind,
    file::{open, read, sync, write},
    net::{accept, connect, recv, send, socket, tuning},
};

mod raw;

/// Owns a completion and its immutable route through dispatch.
#[must_use]
pub struct Dispatch<'event, 'd> {
    event: slot::Occupied<'event, io::Event<'d>>,
    route: route::Route,
}

#[repr(transparent)]
pub struct Member<'event, 'd, T> {
    dispatch: Dispatch<'event, 'd>,
    value: PhantomData<fn() -> T>,
}

#[repr(u8)]
pub enum Selected<'event, 'd> {
    Accept(Member<'event, 'd, accept::Completion<'d>>),
    Recv(Member<'event, 'd, recv::Completion<'d>>),
    RetiredRecv(Member<'event, 'd, recv::Completion<'d>>),
    Send(Member<'event, 'd, send::Completion<'d>>),
    Socket(Member<'event, 'd, socket::Completion<'d>>),
    Tuning(Member<'event, 'd, tuning::Completion<'d>>),
    Connect(Member<'event, 'd, connect::Completion<'d>>),
    Open(Member<'event, 'd, open::Completion<'d>>),
    Write(Member<'event, 'd, write::Completion<'d>>),
    Read(Member<'event, 'd, read::Completion<'d>>),
    Sync(Member<'event, 'd, sync::Completion<'d>>),
    Shutdown(Dispatch<'event, 'd>),
}

impl<'event, 'd> Dispatch<'event, 'd> {
    pub fn new(event: slot::Occupied<'event, io::Event<'d>>) -> Self {
        Self {
            route: event.route(),
            event,
        }
    }

    pub const fn route(&self) -> route::Route {
        self.route
    }

    pub fn into_event(self) -> slot::Occupied<'event, io::Event<'d>> {
        self.event
    }

    pub fn select(self) -> Selected<'event, 'd> {
        match self.event.as_ref() {
            Kind::Accept(_) => Selected::Accept(Member::new(self)),
            Kind::Recv(_) => Selected::Recv(Member::new(self)),
            Kind::RetiredRecv(_) => Selected::RetiredRecv(Member::new(self)),
            Kind::Send(_) => Selected::Send(Member::new(self)),
            Kind::Socket(_) => Selected::Socket(Member::new(self)),
            Kind::Tuning(_) => Selected::Tuning(Member::new(self)),
            Kind::Connect(_) => Selected::Connect(Member::new(self)),
            Kind::Open(_) => Selected::Open(Member::new(self)),
            Kind::Write(_) => Selected::Write(Member::new(self)),
            Kind::Read(_) => Selected::Read(Member::new(self)),
            Kind::Sync(_) => Selected::Sync(Member::new(self)),
            Kind::Shutdown => Selected::Shutdown(self),
        }
    }
}

impl<'d> ops::Deref for Dispatch<'_, 'd> {
    type Target = io::Event<'d>;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl<'event, 'd, T> Member<'event, 'd, T> {
    fn new(dispatch: Dispatch<'event, 'd>) -> Self {
        Self {
            dispatch,
            value: PhantomData,
        }
    }

    pub fn into_dispatch(self) -> Dispatch<'event, 'd> {
        self.dispatch
    }
}

impl<'event, 'd> Selected<'event, 'd> {
    pub fn into_dispatch(self) -> Dispatch<'event, 'd> {
        match self {
            Self::Accept(member) => member.into_dispatch(),
            Self::Recv(member) | Self::RetiredRecv(member) => member.into_dispatch(),
            Self::Send(member) => member.into_dispatch(),
            Self::Socket(member) => member.into_dispatch(),
            Self::Tuning(member) => member.into_dispatch(),
            Self::Connect(member) => member.into_dispatch(),
            Self::Open(member) => member.into_dispatch(),
            Self::Write(member) => member.into_dispatch(),
            Self::Read(member) => member.into_dispatch(),
            Self::Sync(member) => member.into_dispatch(),
            Self::Shutdown(dispatch) => dispatch,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Dispatch<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Selected<'static, 'static>>() == 3 * mem::size_of::<usize>());
};
