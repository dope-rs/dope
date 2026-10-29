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
pub struct Completion<'d>(Entry<'d, kind::Recv, Item<Outcome<'d>>>);

#[repr(transparent)]
pub struct Data<'d>(Entry<'d, kind::Recv, Item<recv::Lease<'d>>>);

#[repr(transparent)]
pub struct Control<'d>(Entry<'d, kind::Recv, Item<ControlKind<'d>>>);

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

pub enum Kind<'d> {
    Data(Data<'d>),
    Control(Control<'d>),
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

    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Recv>,
        flow: Flow,
        outcome: Outcome<'d>,
    ) -> Self {
        Self(Entry::new(target, Item::new(flow, outcome)))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Recv> {
        self.0.target()
    }

    pub const fn flow(&self) -> Flow {
        self.0.value().flow()
    }

    /// Transfers the entire undelivered completion to driver reclamation.
    pub fn retire(self) -> io::Event<'d> {
        io::Event(event::Kind::RetiredRecv(self))
    }

    /// Retains the exact completion when its outcome cannot be delivered.
    pub fn dispatch<T>(
        self,
        dispatch: impl FnOnce(
            route::Erased<'d, kind::Recv>,
            Flow,
            Outcome<'d>,
        ) -> ControlFlow<Outcome<'d>, T>,
    ) -> ControlFlow<Self, T> {
        let (target, flow, outcome) = self.into_parts();
        dispatch(target, flow, outcome).map_break(|outcome| Self::new(target, flow, outcome))
    }

    pub fn into_parts(self) -> (route::Erased<'d, kind::Recv>, Flow, Outcome<'d>) {
        let (target, batch) = self.0.into_parts();
        let (flow, outcome) = batch.into_parts();
        (target, flow, outcome)
    }

    pub fn classify(self) -> Kind<'d> {
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

impl<'d> Data<'d> {
    pub fn into_parts(self) -> (route::Erased<'d, kind::Recv>, Flow, recv::Lease<'d>) {
        let (target, batch) = self.0.into_parts();
        let (flow, lease) = batch.into_parts();
        (target, flow, lease)
    }

    pub fn rejoin(self) -> Completion<'d> {
        let (target, flow, lease) = self.into_parts();
        Completion::new(target, flow, Outcome::Data(lease))
    }
}

impl<'d> Control<'d> {
    fn new(target: route::Erased<'d, kind::Recv>, flow: Flow, outcome: ControlKind<'d>) -> Self {
        Self(Entry::new(target, Item::new(flow, outcome)))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Recv> {
        self.0.target()
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

    pub fn into_parts(self) -> (route::Erased<'d, kind::Recv>, Flow, Status, ControlKind<'d>) {
        let status = self.status();
        let (target, batch) = self.0.into_parts();
        let (flow, outcome) = batch.into_parts();
        (target, flow, status, outcome)
    }

    pub fn rejoin(self) -> Completion<'d> {
        let (target, batch) = self.0.into_parts();
        let (flow, outcome) = batch.into_parts();
        Completion::new(target, flow, outcome.into())
    }

    pub fn dispatch<T, E>(
        self,
        dispatch: impl FnOnce(ControlKind<'d>) -> ControlFlow<(ControlKind<'d>, E), T>,
    ) -> ControlFlow<(Self, E), T> {
        let (target, batch) = self.0.into_parts();
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

impl<'d> From<Completion<'d>> for io::Event<'d> {
    fn from(done: Completion<'d>) -> Self {
        Self(event::Kind::Recv(done))
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
