mod appender;
mod buffer;
mod durable;
mod events;
mod hold;
mod locked;
mod opening;
mod operation;

use std::{io, ops, pin};

use dope_core::{
    driver::{
        self, lifecycle, retained,
        route::{self, kind, table},
        schedule::{self, ready::completion},
        storage,
    },
    io::fs,
};
use o3::cell::region;

use crate::{
    dispatch::typed,
    file::{self, cancellation, open, read},
};

pub(in crate::file) struct Tables<'d, const ID: u8, F>
where
    F: fs::Mode,
{
    opens: operation::OperationTable<'d, opening::Opening<F>, route::KeyTag<ID>>,
    reads: operation::OperationTable<'d, hold::Hold<F>, route::KeyTag<ID, { kind::READ }>>,
}

pub(in crate::file) use locked::Locked;

impl<'d, const ID: u8, F> Tables<'d, ID, F>
where
    F: fs::Mode,
{
    pub(in crate::file) fn try_new(
        capacity: table::Capacity,
        context: &mut storage::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Ok(Self {
            opens: operation::OperationTable::try_with_capacity(
                capacity,
                context.flight_slots(capacity.get())?,
            )?,
            reads: operation::OperationTable::try_with_capacity(
                capacity,
                context.flight_slots(capacity.get())?,
            )?,
        })
    }

    pub(in crate::file) fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        self.opens
            .progress(region)
            .reduce(self.reads.progress(region))
    }

    pub(in crate::file) fn cancel_all(&self, signal: &cancellation::Cancellation) {
        self.opens.begin_shutdown(signal);
        self.reads.begin_shutdown(signal);
    }

    pub(in crate::file) fn begin_open(
        &self,
        path: fs::OpenPath,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> io::Result<file::Key<'d, open::Operation, ID>> {
        let request = opening::Opening::<F>::new(path);
        self.opens
            .begin(request, driver)
            .map(file::Key::new)
            .map_err(|(_, error)| error)
    }

    pub(in crate::file) fn begin_read(
        &self,
        file: file::Regular,
        buffer: Vec<u8>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<file::Key<'d, read::Operation, ID>, (file::Regular, Vec<u8>, io::Error)> {
        self.reads
            .begin(hold::Hold::<F>::new(file, buffer), driver)
            .map(file::Key::new)
            .map_err(|(hold, error)| {
                let (file, buffer) = hold.into_parts();
                (file, buffer, error)
            })
    }

    pub(in crate::file) fn poll_open(
        &self,
        token: file::Key<'d, open::Operation, ID>,
        wake: completion::Waker<'d>,
    ) -> file::Outcome<open::Done> {
        match self.opens.poll(token.raw(), wake) {
            Some((_, done)) => file::Outcome::Done(done),
            None => file::Outcome::Pending,
        }
    }

    pub(in crate::file) fn poll_read(
        &self,
        token: file::Key<'d, read::Operation, ID>,
        wake: completion::Waker<'d>,
    ) -> file::Outcome<(Vec<u8>, read::Done)> {
        match self.reads.poll(token.raw(), wake) {
            Some((hold, done)) => {
                let (_file, buffer) = hold.into_parts();
                file::Outcome::Done((buffer, done))
            }
            None => file::Outcome::Pending,
        }
    }

    pub(in crate::file) fn cancel_open(
        &self,
        token: file::Key<'d, open::Operation, ID>,
        signal: &cancellation::Cancellation,
    ) {
        let _ = self.opens.request_cancel(token.raw(), signal);
    }

    pub(in crate::file) fn cancel_read(
        &self,
        token: file::Key<'d, read::Operation, ID>,
        signal: &cancellation::Cancellation,
    ) {
        let _ = self.reads.request_cancel(token.raw(), signal);
    }

    pub(in crate::file) fn flush_cancellations(
        &self,
        signal: &cancellation::Cancellation,
        work: schedule::Maintenance<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        self.opens.flush_cancellations(signal, work, driver)
            && self.reads.flush_cancellations(signal, work, driver)
    }
}

// SAFETY: the borrowed file tables are driven through route quiescence.
unsafe impl<'d, const ID: u8, const N: usize, F> crate::dispatch::raw::Manifold<'d>
    for file::Manifold<'d, ID, N, F>
where
    F: fs::Mode,
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Retained;
    type Activate = crate::dispatch::raw::Plain;
    type PrePark = crate::dispatch::raw::Plain;
    type Shutdown = crate::dispatch::raw::Plain;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.as_ref().get_ref().files.route.install(install);
    }

    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        ev: crate::DriverEvent<'d>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        use dope_core::io::event::Kind;

        let this = self.as_ref().get_ref().files;
        match ev.into_kind() {
            Kind::Open(completion) => {
                let (token, outcome) = completion.into_parts();
                this.tables
                    .opens
                    .complete(token, events::Opening::from_open(outcome), driver)
            }
            Kind::Read(token, e) => this.tables.reads.complete(token, e, driver),
            Kind::Stat(token, e) => {
                this.tables
                    .opens
                    .complete(token, events::Opening::Stat(e), driver)
            }
            _ => {}
        }
        ops::ControlFlow::Continue(())
    }

    unsafe fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let files = self.as_ref().get_ref().files;
        files.flush_cancellations(turn.maintenance(), driver);
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        self.get_ref().files.tables.progress(region)
    }

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _target: typed::Token<'d, Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let _ = self;
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        let files = self.as_ref().get_ref().files;
        files.tables.cancel_all(&files.cancellations);
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        let files = self.as_ref().get_ref().files;
        finish.stage_route(&files.route);
    }
}
