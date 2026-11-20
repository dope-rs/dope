use std::{mem, ops::ControlFlow};

use dope_sys::table::permit::Queue as _;

use crate::{
    driver::{self, receive, route, route::kind},
    io,
    io::{
        event,
        event::{
            batch::{Flow, Item},
            entry::Entry,
        },
        recv,
    },
};

pub enum Outcome<'d> {
    Data(recv::Lease<'d>),
    Eof,
    Cancelled,
    Exhausted(receive::Exhausted<'d>),
    Starved(Retryable),
    Failed(crate::Errno),
}

#[repr(transparent)]
pub struct Completion<'d, S: route::Selection = route::Route>(Entry<'d, kind::Recv, Item<Outcome<'d>>, S>);

#[repr(transparent)]
pub struct Data<'d, S: route::Selection = route::Route>(Entry<'d, kind::Recv, Item<recv::Lease<'d>>, S>);

#[repr(transparent)]
pub struct Control<'d, S: route::Selection = route::Route>(Entry<'d, kind::Recv, Item<ControlKind<'d>>, S>);

#[derive(Clone, Copy)]
pub enum Status {
    Eof,
    Cancelled,
    Exhausted,
    Starved(Retryable),
    Failed(crate::Errno),
}

pub enum ControlKind<'d> {
    Eof,
    Cancelled,
    Exhausted(receive::Exhausted<'d>),
    Starved(Retryable),
    Failed(crate::Errno),
}

pub enum Kind<'d, S: route::Selection = route::Route> {
    Data(Data<'d, S>),
    Control(Control<'d, S>),
}

/// The exact native error proving that a receive operation can retry.
///
/// Arbitrary errors cannot enter a retryable completion:
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Retryable(crate::Errno);

impl From<Retryable> for crate::Errno {
    fn from(error: Retryable) -> Self {
        error.0
    }
}

impl<'d> Completion<'d> {
    pub(crate) fn control(
        target: route::Erased<'d, kind::Recv>,
        outcome: Result<(), crate::Errno>,
        flow: Flow,
        driver: driver::Reference<'d>,
    ) -> Self {
        let outcome = match outcome {
            Ok(()) => Outcome::Eof,
            Err(errno) if errno == crate::Errno::NO_BUFFER_SPACE => {
                if flow == Flow::Last && crate::Permits::try_acquire().is_ok() {
                    Outcome::Exhausted(receive::Exhausted::from_driver(driver))
                } else {
                    Outcome::Starved(Retryable(errno))
                }
            }
            Err(errno) if errno == crate::Errno::CANCELED => Outcome::Cancelled,
            Err(errno) if errno.is_intr() || errno.is_block() => Outcome::Starved(Retryable(errno)),
            Err(errno) => Outcome::Failed(errno),
        };
        Self::new(target, flow, outcome)
    }
}

impl<'d, S: route::Selection> Completion<'d, S> {
    pub(in crate::io::event) fn in_scope<N: route::Selection>(self, scope: N) -> Completion<'d, N> {
        Completion(self.0.in_scope(scope))
    }

    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Recv, S>,
        flow: Flow,
        outcome: Outcome<'d>,
    ) -> Self {
        Self(Entry::new(target, Item::new(flow, outcome)))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Recv, S> {
        self.0.token()
    }

    pub const fn flow(&self) -> Flow {
        self.0.value().flow()
    }

    /// Transfers the entire undelivered completion to driver reclamation.
    pub fn retire(self) -> io::Event<'d> {
        io::Event(event::Kind::RetiredRecv(Completion(self.0.erase_route())))
    }

    /// Retains the exact completion when its outcome cannot be delivered.
    pub fn dispatch<T>(
        self,
        dispatch: impl FnOnce(
            route::Erased<'d, kind::Recv, S>,
            Flow,
            Outcome<'d>,
        ) -> ControlFlow<Outcome<'d>, T>,
    ) -> ControlFlow<Self, T> {
        let (target, flow, outcome) = self.into_parts();
        dispatch(target, flow, outcome).map_break(|outcome| Self::new(target, flow, outcome))
    }

    pub fn into_parts(self) -> (route::Erased<'d, kind::Recv, S>, Flow, Outcome<'d>) {
        let (target, batch) = self.0.split();
        let (flow, outcome) = batch.into_parts();
        (target, flow, outcome)
    }

    pub fn classify(self) -> Kind<'d, S> {
        let (target, flow, outcome) = self.into_parts();
        match outcome {
            Outcome::Data(lease) => Kind::Data(Data(Entry::new(target, Item::new(flow, lease)))),
            Outcome::Eof => Kind::Control(Control::new(target, flow, ControlKind::Eof)),
            Outcome::Cancelled => Kind::Control(Control::new(target, flow, ControlKind::Cancelled)),
            Outcome::Exhausted(state) => {
                Kind::Control(Control::new(target, flow, ControlKind::Exhausted(state)))
            }
            Outcome::Starved(errno) => {
                Kind::Control(Control::new(target, flow, ControlKind::Starved(errno)))
            }
            Outcome::Failed(errno) => {
                Kind::Control(Control::new(target, flow, ControlKind::Failed(errno)))
            }
        }
    }
}

impl<'d, S: route::Selection> Data<'d, S> {
    pub fn into_parts(self) -> (route::Erased<'d, kind::Recv, S>, Flow, recv::Lease<'d>) {
        let (target, batch) = self.0.split();
        let (flow, lease) = batch.into_parts();
        (target, flow, lease)
    }

    pub fn rejoin(self) -> Completion<'d, S> {
        let (target, flow, lease) = self.into_parts();
        Completion::new(target, flow, Outcome::Data(lease))
    }
}

impl<'d, S: route::Selection> Control<'d, S> {
    pub fn erase_route(self) -> Control<'d> {
        Control(self.0.erase_route())
    }

    fn new(target: route::Erased<'d, kind::Recv, S>, flow: Flow, outcome: ControlKind<'d>) -> Self {
        Self(Entry::new(target, Item::new(flow, outcome)))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Recv, S> {
        self.0.token()
    }

    pub const fn flow(&self) -> Flow {
        self.0.value().flow()
    }

    pub const fn status(&self) -> Status {
        match self.0.value().value() {
            ControlKind::Eof => Status::Eof,
            ControlKind::Cancelled => Status::Cancelled,
            ControlKind::Exhausted(_) => Status::Exhausted,
            ControlKind::Starved(error) => Status::Starved(*error),
            ControlKind::Failed(error) => Status::Failed(*error),
        }
    }

    pub fn into_parts(self) -> (route::Erased<'d, kind::Recv, S>, Flow, Status, ControlKind<'d>) {
        let status = self.status();
        let (target, batch) = self.0.split();
        let (flow, outcome) = batch.into_parts();
        (target, flow, status, outcome)
    }

    pub fn rejoin(self) -> Completion<'d, S> {
        let (target, batch) = self.0.split();
        let (flow, outcome) = batch.into_parts();
        Completion::new(target, flow, outcome.into())
    }

    pub fn dispatch<T, E>(
        self,
        dispatch: impl FnOnce(ControlKind<'d>) -> ControlFlow<(ControlKind<'d>, E), T>,
    ) -> ControlFlow<(Self, E), T> {
        let (target, batch) = self.0.split();
        let (flow, outcome) = batch.into_parts();
        dispatch(outcome).map_break(|(outcome, cause)| (Self::new(target, flow, outcome), cause))
    }
}

impl<'d> From<ControlKind<'d>> for Outcome<'d> {
    fn from(kind: ControlKind<'d>) -> Self {
        match kind {
            ControlKind::Eof => Self::Eof,
            ControlKind::Cancelled => Self::Cancelled,
            ControlKind::Exhausted(proof) => Self::Exhausted(proof),
            ControlKind::Starved(error) => Self::Starved(error),
            ControlKind::Failed(error) => Self::Failed(error),
        }
    }
}

impl<'d, S: route::Selection> From<Completion<'d, S>> for io::Event<'d> {
    fn from(done: Completion<'d, S>) -> Self {
        Self(event::Kind::Recv(Completion(done.0.erase_route())))
    }
}

const _: () = {
    assert!(mem::size_of::<Retryable>() == mem::size_of::<crate::Errno>());
    assert!(mem::align_of::<Retryable>() == mem::align_of::<crate::Errno>());
    assert!(mem::size_of::<Option<Retryable>>() == mem::size_of::<Option<crate::Errno>>());
    assert!(mem::size_of::<[Retryable; 4]>() == mem::size_of::<[crate::Errno; 4]>());
    assert!(
        mem::size_of::<Completion<'static>>()
            == mem::size_of::<(route::Erased<'static, kind::Recv>, Flow, Outcome<'static>)>()
    );
    assert!(
        mem::align_of::<Completion<'static>>()
            == mem::align_of::<(route::Erased<'static, kind::Recv>, Flow, Outcome<'static>)>()
    );
    assert!(
        mem::size_of::<Data<'static>>()
            == mem::size_of::<(
                route::Erased<'static, kind::Recv>,
                Flow,
                recv::Lease<'static>,
            )>()
    );
    assert!(
        mem::align_of::<Data<'static>>()
            == mem::align_of::<(
                route::Erased<'static, kind::Recv>,
                Flow,
                recv::Lease<'static>,
            )>()
    );
    assert!(
        mem::size_of::<Control<'static>>()
            == mem::size_of::<(
                route::Erased<'static, kind::Recv>,
                Flow,
                ControlKind<'static>,
            )>()
    );
    assert!(
        mem::align_of::<Control<'static>>()
            == mem::align_of::<(
                route::Erased<'static, kind::Recv>,
                Flow,
                ControlKind<'static>,
            )>()
    );
    assert!(mem::size_of::<Kind<'static>>() <= mem::size_of::<Completion<'static>>());
};
