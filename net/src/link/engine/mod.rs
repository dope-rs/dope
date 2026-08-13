use std::mem;

mod discard;
mod lifecycle;
mod receive;
pub(in crate::link) mod sending;

use dope_core::{
    driver::{self, flight, ops, route, schedule::ready},
    io::{fd::handles, socket::option},
};
pub(in crate::link) use receive::Receive;

use crate::link::{self, pool};

pub(in crate::link) struct Flights<'d> {
    recv: Option<flight::Flight<'d>>,
    send: Option<flight::Flight<'d>>,
}

#[must_use = "accepted descriptor must be installed or reclaimed"]
#[repr(transparent)]
pub(in crate::link) struct Accepted<'d>(handles::Descriptor<'d>);

pub(in crate::link) enum AcceptedTuning {
    Ready,
    Pending,
    Failed,
}

pub struct Engine<'d> {
    pub(in crate::link) recv: receive::Receive,
    pub(in crate::link) discard: discard::Discard,
    pub(in crate::link) lifecycle: lifecycle::Lifecycle,
    pub(in crate::link) sending: sending::Sending,
    pub(in crate::link) flights: Flights<'d>,
    pub(in crate::link) establish: link::Setup<'d>,
}

impl<'d> Engine<'d> {
    pub(in crate::link) fn accepted(fd: handles::Descriptor<'d>) -> Accepted<'d> {
        Accepted(fd)
    }

    pub(in crate::link) fn outbound(socket: handles::CreatingSocket<'d>) -> Self {
        Self::with_establish(link::Setup::creating(socket))
    }

    pub(in crate::link) fn outbound_targeted<const ID: u8>(
        socket: handles::CreatingSocket<'d>,
        stored: pool::StoredAddress<'d, ID>,
    ) -> Self {
        Self::with_establish(link::Setup::creating_targeted(socket, stored))
    }

    fn with_establish(establish: link::Setup<'d>) -> Self {
        Self {
            recv: receive::Receive::new(),
            discard: discard::Discard::new(),
            lifecycle: lifecycle::Lifecycle::new(),
            sending: sending::Sending::new(),
            flights: Flights {
                recv: None,
                send: None,
            },
            establish,
        }
    }

    pub(in crate::link) fn fd(&self) -> Option<&handles::Descriptor<'d>> {
        self.establish.fd()
    }

    pub(in crate::link) fn driver(&self) -> driver::Reference<'d> {
        self.establish.driver()
    }

    pub(in crate::link) fn ready_handle(&self) -> ready::Handle<'d> {
        self.establish.ready_handle()
    }

    pub(in crate::link) fn into_authority(self) -> link::Authority<'d> {
        self.establish.into_authority()
    }

    pub(in crate::link) fn close(self, driver: &mut driver::Context<'_, 'd>) {
        match self.into_authority() {
            link::Authority::Creating(socket) => drop(socket),
            link::Authority::Live(fd) => ops::Files::close(driver, fd),
        }
    }
}

impl<'d> Accepted<'d> {
    pub(in crate::link) fn into_fd(self) -> handles::Descriptor<'d> {
        self.0
    }

    pub(in crate::link) fn tune<const ID: u8>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        options: option::StreamOptions,
        target: route::Target<'d, route::KeyTag<ID>>,
    ) -> (Engine<'d>, AcceptedTuning) {
        let result = ops::Control::submit_tuning(driver, target.bind(self.0), options);
        Self::resolve(result)
    }

    fn resolve(
        result: Result<option::Tuning<'d>, handles::Descriptor<'d>>,
    ) -> (Engine<'d>, AcceptedTuning) {
        let (establish, outcome) = match result {
            Ok(tuning) => match tuning {
                option::Tuning::Applied(fd) => (link::Setup::done(fd), AcceptedTuning::Ready),
                option::Tuning::Pending(pending) => {
                    (link::Setup::tuning(pending), AcceptedTuning::Pending)
                }
            },
            Err(fd) => (link::Setup::idle(fd), AcceptedTuning::Failed),
        };
        (Engine::with_establish(establish), outcome)
    }
}

const _: () =
    assert!(mem::size_of::<Accepted<'static>>() == mem::size_of::<handles::Descriptor<'static>>());
const _: () = assert!(
    mem::align_of::<Accepted<'static>>() == mem::align_of::<handles::Descriptor<'static>>()
);
