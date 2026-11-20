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
pub struct Dispatch<'event, 'd, S: route::Selection = route::Route> {
    event: slot::Occupied<'event, io::Event<'d>>,
    route: S,
}

#[repr(transparent)]
pub struct Member<'event, 'd, T, S: route::Selection = route::Route> {
    dispatch: Dispatch<'event, 'd, S>,
    value: PhantomData<fn() -> T>,
}

#[repr(u8)]
pub enum Selected<'event, 'd, S: route::Selection = route::Route> {
    Accept(Member<'event, 'd, accept::Completion<'d>, S>),
    Recv(Member<'event, 'd, recv::Completion<'d>, S>),
    RetiredRecv(Member<'event, 'd, recv::Completion<'d>, S>),
    Send(Member<'event, 'd, send::Completion<'d>, S>),
    Socket(Member<'event, 'd, socket::Completion<'d>, S>),
    Tuning(Member<'event, 'd, tuning::Completion<'d>, S>),
    Connect(Member<'event, 'd, connect::Completion<'d>, S>),
    Open(Member<'event, 'd, open::Completion<'d>, S>),
    Write(Member<'event, 'd, write::Completion<'d>, S>),
    Read(Member<'event, 'd, read::Completion<'d>, S>),
    Sync(Member<'event, 'd, sync::Completion<'d>, S>),
    Shutdown(Dispatch<'event, 'd, S>),
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

    #[inline]
    pub fn within<S: route::Selection>(
        self,
        accepts: impl FnOnce(route::Route) -> bool,
    ) -> Result<Dispatch<'event, 'd, S>, Self> {
        if let Some(route) = S::select(self.route)
            && accepts(route.into_route())
        {
            Ok(Dispatch { event: self.event, route })
        } else {
            Err(self)
        }
    }
}

impl<'event, 'd, S: route::Selection> Dispatch<'event, 'd, S> {
    pub fn into_dispatch(self) -> Dispatch<'event, 'd> {
        Dispatch { event: self.event, route: self.route.into_route() }
    }

    pub fn into_event(self) -> slot::Occupied<'event, io::Event<'d>> {
        self.event
    }

    pub fn select(self) -> Selected<'event, 'd, S> {
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

impl<'d, S: route::Selection> ops::Deref for Dispatch<'_, 'd, S> {
    type Target = io::Event<'d>;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl<'event, 'd, T, S: route::Selection> Member<'event, 'd, T, S> {
    fn new(dispatch: Dispatch<'event, 'd, S>) -> Self {
        Self {
            dispatch,
            value: PhantomData,
        }
    }

    pub fn into_dispatch(self) -> Dispatch<'event, 'd> {
        self.dispatch.into_dispatch()
    }
}

impl<'event, 'd, S: route::Selection> Member<'event, 'd, recv::Completion<'d>, S> {
    pub fn take_routed(self) -> (recv::Completion<'d, S>, slot::Vacant<'event, io::Event<'d>>) {
        let scope = self.dispatch.route;
        let (done, vacant) = self.take();
        (done.in_scope(scope), vacant)
    }
}

impl<'event, 'd, S: route::Selection> Member<'event, 'd, send::Completion<'d>, S> {
    pub fn take_routed(self) -> (send::Completion<'d, S>, slot::Vacant<'event, io::Event<'d>>) {
        let scope = self.dispatch.route;
        let (done, vacant) = self.take();
        (done.in_scope(scope), vacant)
    }
}

impl<'event, 'd, S: route::Selection> Selected<'event, 'd, S> {
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
            Self::Shutdown(dispatch) => dispatch.into_dispatch(),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Dispatch<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Selected<'static, 'static>>() == 3 * mem::size_of::<usize>());
};
