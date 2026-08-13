use std::{io, ops, os::fd, pin, process};

use dope_core::{
    driver::{
        self, lifecycle, retained,
        route::{self, kind},
        schedule,
    },
    io::{event, fs},
};
use o3::cell::region;

use crate::{dispatch::typed, file::appender};

type Tag<const ID: u8> = route::KeyTag<ID, { kind::WRITE }>;

fn submit<'borrow, 'owner, 'd: 'owner, const ID: u8, const N: usize, const B: usize>(
    appender: &'borrow mut appender::Appender<'d, ID, N, B>,
    driver: &mut retained::Context<'_, 'owner, 'd>,
) -> Result<(), driver::SubmitError>
where
    'owner: 'borrow,
{
    let Some(in_flight) = appender.queue.in_flight.as_ref() else {
        process::abort();
    };
    if in_flight.flight.is_some() {
        process::abort();
    }
    let block = in_flight.block;
    let written = in_flight.written;
    let Ok(written_offset) = u64::try_from(written) else {
        appender.fail(io::Error::new(
            io::ErrorKind::FileTooLarge,
            "dope::file: append progress exceeds file offsets",
        ));
        return Ok(());
    };
    let Some(offset) = appender.destination.offset.checked_add(written_offset) else {
        appender.fail(io::Error::new(
            io::ErrorKind::FileTooLarge,
            "dope::file: append offset overflow",
        ));
        return Ok(());
    };
    let target = route::Space::<Tag<ID>>::for_driver(driver.driver().driver_ref())
        .bind(route::SlotIndex::ZERO, route::Epoch::INITIAL);
    let bytes = &appender.queue.blocks[block][written..];
    let Ok(submission) = fs::Submission::<fs::Native, Tag<ID>>::write(
        fd::AsFd::as_fd(&appender.destination.file),
        bytes,
        offset,
        target,
    ) else {
        process::abort();
    };
    // SAFETY: the installed, pinned Appender owns the descriptor and every
    // fixed block until terminal completion or runtime quiescence.
    let flight = unsafe {
        use retained::raw;

        raw::Owner::submit_file(driver, &appender.flights, submission)
    }?;
    let Some(in_flight) = appender.queue.in_flight.as_mut() else {
        process::abort();
    };
    in_flight.flight = Some(flight);
    Ok(())
}

// SAFETY: Appender is installed pinned and owns its descriptor and fixed blocks,
// and flight slots through shutdown drain and finish.
unsafe impl<'d, const ID: u8, const N: usize, const B: usize> crate::dispatch::raw::Manifold<'d>
    for appender::Appender<'d, ID, N, B>
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Retained;
    type Activate = crate::dispatch::raw::Plain;
    type PrePark = crate::dispatch::raw::Retained;
    type Shutdown = crate::dispatch::raw::Plain;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.as_ref().get_ref().route.install(install);
    }

    unsafe fn dispatch<'turn>(
        mut self: pin::Pin<&mut Self>,
        event: crate::DriverEvent<'d>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        use dope_core::io::WriteEvent;

        let event::Kind::Write(token, event) = event.into_kind() else {
            return ops::ControlFlow::Continue(());
        };
        let this = self.as_mut().get_mut();
        let Some(in_flight) = this.queue.in_flight.as_mut() else {
            process::abort();
        };
        let Some(flight) = in_flight.flight.take() else {
            process::abort();
        };
        if !flight.matches(token) {
            process::abort();
        }
        let _completed = flight.complete();
        match event {
            WriteEvent::Written(0) => {
                this.fail(io::Error::from(io::ErrorKind::WriteZero));
            }
            WriteEvent::Written(amount) => {
                let Ok(amount) = usize::try_from(amount) else {
                    process::abort();
                };
                let remaining = this.queue.blocks[in_flight.block].len() - in_flight.written;
                if amount > remaining {
                    process::abort();
                }
                in_flight.written += amount;
                if in_flight.written == this.queue.blocks[in_flight.block].len() {
                    this.complete_block();
                } else {
                    match submit(this, driver) {
                        Ok(()) => {}
                        Err(driver::SubmitError) => return ops::ControlFlow::Continue(()),
                    }
                }
            }
            WriteEvent::Failed(errno) => {
                this.fail(io::Error::from_raw_os_error(errno));
            }
        }
        ops::ControlFlow::Continue(())
    }

    unsafe fn pre_park<'turn>(
        mut self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let this = self.as_mut().get_mut();
        if this.prepare() {
            let _ = submit(this, driver);
        }
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        match self.get_ref().progress() {
            appender::State::Runnable => schedule::Progress::Runnable,
            appender::State::Waiting => schedule::Progress::waiting(region),
            appender::State::Quiescent => schedule::Progress::Quiescent,
        }
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
        mut self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        self.as_mut().get_mut().close();
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        finish.retire_route(&self.as_ref().get_ref().route);
    }
}

// SAFETY: Control retains the exclusive pinned borrow for one coordination
// step and only exposes bounded copies into non-flight blocks.
unsafe impl<'d, const ID: u8, const N: usize, const B: usize> crate::dispatch::raw::Controlled<'d>
    for appender::Appender<'d, ID, N, B>
{
    type Control<'step>
        = appender::Control<'step, 'd, ID, N, B>
    where
        Self: 'step,
        'd: 'step;

    unsafe fn control<'step>(self: pin::Pin<&'step mut Self>) -> Self::Control<'step>
    where
        'd: 'step,
    {
        appender::Control { inner: self }
    }
}
