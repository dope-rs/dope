use std::mem;

use dope_sys::{
    Cloexec, Nonblock,
    engine::life::Resolver as _,
    io::socket::{SetupOrigin, accept},
    table::registry,
};

use crate::{
    driver::{self, retained::accept::Readiness, route, route::kind},
    io::{
        event::{
            batch::{Flow, Item},
            entry::Entry,
        },
        fd::handles,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Syscall(crate::Errno),
    Registration(registry::Failure),
    DescriptorFlags(crate::Errno),
    CloseOnExec(crate::Errno),
    Nonblocking(crate::Errno),
    Sigpipe(crate::Errno),
    Capacity { close: Result<(), crate::Errno> },
    Retired(handles::PhysicalSlot),
    Quarantined(handles::PhysicalSlot),
}

impl Failure {
    /// Settles pending returns before checking the native local accepted-slot capacity.
    /// Native syscall exhaustion retains its backend-specific retirement policy.
    pub fn can_defer(&self, driver: &mut driver::Context<'_, '_>) -> bool {
        if matches!(self, Self::Capacity { close: Ok(()) }) {
            driver.native().resolve().retire();
            return driver.native().as_mut().has_local_accept_capacity();
        }
        self.is_resource() && !matches!(Readiness::from(driver.driver_ref()), Readiness::Ready)
    }

    /// Whether capacity recovery permits a later attempt, without proving readiness now.
    pub fn is_resource(&self) -> bool {
        matches!(self, Self::Capacity { close: Ok(()) })
            || matches!(self, Self::Syscall(error) if <crate::Backend as accept::Source>::can_defer(*error))
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Syscall(error) => <crate::Backend as accept::Source>::is_retryable(*error),
            Self::Registration(_)
            | Self::DescriptorFlags(_)
            | Self::CloseOnExec(_)
            | Self::Nonblocking(_)
            | Self::Sigpipe(_)
            | Self::Capacity { .. }
            | Self::Retired(_)
            | Self::Quarantined(_) => false,
        }
    }
}

impl From<handles::Reclaimed> for Failure {
    fn from(retired: handles::Reclaimed) -> Self {
        match retired {
            handles::Reclaimed::Queued(slot) => Self::Retired(slot),
            handles::Reclaimed::Quarantined(slot) => Self::Quarantined(slot),
        }
    }
}

impl From<dope_sys::Errno> for Failure {
    fn from(error: dope_sys::Errno) -> Self {
        Self::Syscall(error)
    }
}

impl From<accept::Error> for Failure {
    fn from(error: accept::Error) -> Self {
        match error {
            accept::Error::Syscall(error) => Self::Syscall(error),
            accept::Error::Registration(error) => Self::Registration(error),
            accept::Error::Cloexec(Cloexec::Get(error)) => Self::DescriptorFlags(error),
            accept::Error::Cloexec(Cloexec::Set(error)) => Self::CloseOnExec(error),
            accept::Error::Setup(error) => match error.origin() {
                SetupOrigin::Cloexec(Cloexec::Get(error)) => Self::DescriptorFlags(error),
                SetupOrigin::Cloexec(Cloexec::Set(error)) => Self::CloseOnExec(error),
                SetupOrigin::Nonblock(Nonblock::Get(error)) => Self::DescriptorFlags(error),
                SetupOrigin::Nonblock(Nonblock::Set(error)) => Self::Nonblocking(error),
                SetupOrigin::Sigpipe(error) => Self::Sigpipe(error),
            },
            accept::Error::Capacity { close } => Self::Capacity { close },
        }
    }
}

pub enum Outcome<'d> {
    Accepted(handles::AcceptedSlot<'d>),
    Failed(Failure),
}

#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Accept, Item<Outcome<'d>>>);

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Accept>,
        flow: Flow,
        outcome: Outcome<'d>,
    ) -> Self {
        Self(Entry::new(target, Item::new(flow, outcome)))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Accept> {
        self.0.target()
    }

    pub fn into_parts(self) -> (route::Erased<'d, kind::Accept>, Flow, Outcome<'d>) {
        let (target, batch) = self.0.into_parts();
        let (flow, outcome) = batch.into_parts();
        (target, flow, outcome)
    }
}

const _: () = {
    assert!(mem::size_of::<Failure>() == 2 * mem::size_of::<dope_sys::Errno>());
    assert!(mem::align_of::<Failure>() == mem::align_of::<dope_sys::Errno>());
    assert!(mem::size_of::<Outcome<'_>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Outcome<'_>>() == mem::align_of::<usize>());
    assert!(
        mem::size_of::<Completion<'_>>()
            == mem::size_of::<(route::Erased<'_, kind::Accept>, Flow, Outcome<'_>)>()
    );
    assert!(
        mem::align_of::<Completion<'_>>()
            == mem::align_of::<(route::Erased<'_, kind::Accept>, Flow, Outcome<'_>)>()
    );
};
