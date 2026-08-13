use std::{ops, os::fd, pin, process};

use dope_core::{
    driver::{
        self, lifecycle, retained,
        route::{self, kind},
        schedule,
    },
    io::{self, event, fs},
};
use o3::cell::region;

use crate::{dispatch::typed, file::durable};

type WriteTag<const ID: u8> = route::KeyTag<ID, { kind::WRITE }>;
type SyncTag<const ID: u8> = route::KeyTag<ID, { kind::SYNC }>;

fn submit_write<'owner, 'd: 'owner, const ID: u8, const N: usize, const B: usize>(
    appender: &'owner durable::Appender<'d, ID, N, B>,
    driver: &mut retained::Context<'_, 'owner, 'd>,
) -> Result<(), driver::SubmitError> {
    let mut inner = appender.inner.borrow_mut();
    let Some(in_flight) = inner.queue.in_flight.as_ref() else {
        process::abort();
    };
    debug_assert_eq!(in_flight.phase, durable::Phase::Write);
    debug_assert!(in_flight.flight.is_none());
    let block = in_flight.block;
    let written = in_flight.written;
    let block_len = inner.queue.blocks[block].bytes.len();
    let end_is_representable = u64::try_from(block_len)
        .ok()
        .and_then(|length| inner.destination.offset.checked_add(length))
        .is_some();
    if !end_is_representable {
        inner.fail(durable::Failure::FileTooLarge);
        return Ok(());
    }
    let Some(offset) = u64::try_from(written)
        .ok()
        .and_then(|written| inner.destination.offset.checked_add(written))
    else {
        inner.fail(durable::Failure::FileTooLarge);
        return Ok(());
    };
    let target = route::Space::<WriteTag<ID>>::for_driver(driver.driver().driver_ref())
        .bind(route::SlotIndex::ZERO, route::Epoch::INITIAL);
    let bytes = &inner.queue.blocks[block].bytes[written..];
    let Ok(submission) = fs::Submission::<fs::Native, WriteTag<ID>>::write(
        fd::AsFd::as_fd(&inner.destination.file),
        bytes,
        offset,
        target,
    ) else {
        inner.fail(durable::Failure::FileTooLarge);
        return Ok(());
    };
    // SAFETY: storage owns the descriptor and pre-reserved blocks through
    // route quiescence. In-flight blocks are never exposed to appenders.
    let flight =
        unsafe { retained::raw::Owner::submit_file(driver, &appender.writes, submission) }?;
    let Some(in_flight) = inner.queue.in_flight.as_mut() else {
        process::abort();
    };
    in_flight.flight = Some(flight);
    Ok(())
}

fn submit_sync<'owner, 'd: 'owner, const ID: u8, const N: usize, const B: usize>(
    appender: &'owner durable::Appender<'d, ID, N, B>,
    driver: &mut retained::Context<'_, 'owner, 'd>,
) -> Result<(), driver::SubmitError> {
    let mut inner = appender.inner.borrow_mut();
    let Some(in_flight) = inner.queue.in_flight.as_ref() else {
        process::abort();
    };
    debug_assert_eq!(in_flight.phase, durable::Phase::Sync);
    debug_assert!(in_flight.flight.is_none());
    let target = route::Space::<SyncTag<ID>>::for_driver(driver.driver().driver_ref())
        .bind(route::SlotIndex::ZERO, route::Epoch::INITIAL);
    let submission = fs::Submission::<fs::Native, SyncTag<ID>>::sync(
        fd::AsFd::as_fd(&inner.destination.file),
        fs::Sync::Data,
        target,
    );
    // SAFETY: storage owns the descriptor through terminal completion or
    // driver quiescence, and sync captures no mutable buffer.
    let flight = unsafe { retained::raw::Owner::submit_file(driver, &appender.syncs, submission) }?;
    let Some(in_flight) = inner.queue.in_flight.as_mut() else {
        process::abort();
    };
    in_flight.flight = Some(flight);
    Ok(())
}

// SAFETY: the manifold borrows driver-branded storage. Storage owns the file,
// fixed-capacity blocks, wait slots, and retained-flight slots through staged
// route finalization. A single in-flight block is immutable until completion.
unsafe impl<'d, const ID: u8, const N: usize, const B: usize> crate::dispatch::raw::Manifold<'d>
    for durable::Manifold<'d, ID, N, B>
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Retained;
    type Activate = crate::dispatch::raw::Plain;
    type PrePark = crate::dispatch::raw::Retained;
    type Shutdown = crate::dispatch::raw::Plain;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.as_ref().get_ref().appender.route.install(install);
    }

    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        event: crate::DriverEvent<'d>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        let appender = self.as_ref().get_ref().appender;
        match event.into_kind() {
            event::Kind::Write(token, result) => {
                let mut inner = appender.inner.borrow_mut();
                let (block, written, flight) = {
                    let Some(in_flight) = inner.queue.in_flight.as_mut() else {
                        process::abort();
                    };
                    debug_assert_eq!(in_flight.phase, durable::Phase::Write);
                    let Some(flight) = in_flight.flight.take() else {
                        process::abort();
                    };
                    (in_flight.block, in_flight.written, flight)
                };
                if !flight.matches(token) {
                    process::abort();
                }
                let _completed = flight.complete();
                match result {
                    io::WriteEvent::Written(0) => inner.fail(durable::Failure::WriteZero),
                    io::WriteEvent::Written(amount) => {
                        let Ok(amount) = usize::try_from(amount) else {
                            process::abort();
                        };
                        let length = inner.queue.blocks[block].bytes.len();
                        let Some(remaining) = length.checked_sub(written) else {
                            process::abort();
                        };
                        if amount > remaining {
                            process::abort();
                        }
                        let Some(in_flight) = inner.queue.in_flight.as_mut() else {
                            process::abort();
                        };
                        in_flight.written = written + amount;
                        if in_flight.written == length {
                            in_flight.phase = durable::Phase::Sync;
                        }
                    }
                    io::WriteEvent::Failed(errno) => inner.fail(durable::Failure::Os(errno)),
                }
                drop(inner);
                let phase = appender
                    .inner
                    .borrow()
                    .queue
                    .in_flight
                    .as_ref()
                    .filter(|flight| flight.flight.is_none())
                    .map(|flight| flight.phase);
                match phase {
                    Some(durable::Phase::Write) => {
                        let _ = submit_write(appender, driver);
                    }
                    Some(durable::Phase::Sync) => {
                        let _ = submit_sync(appender, driver);
                    }
                    None => {}
                }
            }
            event::Kind::Sync(token, result) => {
                let mut inner = appender.inner.borrow_mut();
                let Some(in_flight) = inner.queue.in_flight.as_mut() else {
                    process::abort();
                };
                debug_assert_eq!(in_flight.phase, durable::Phase::Sync);
                let Some(flight) = in_flight.flight.take() else {
                    process::abort();
                };
                if !flight.matches(token) {
                    process::abort();
                }
                let _completed = flight.complete();
                match result {
                    io::Sync::Done => inner.complete_block(),
                    io::Sync::Failed(errno) => inner.fail(durable::Failure::Os(errno)),
                }
            }
            _ => {}
        }
        ops::ControlFlow::Continue(())
    }

    unsafe fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let appender = self.as_ref().get_ref().appender;
        let phase = appender.inner.borrow_mut().prepare();
        match phase {
            Some(durable::Phase::Write) => {
                let _ = submit_write(appender, driver);
            }
            Some(durable::Phase::Sync) => {
                let _ = submit_sync(appender, driver);
            }
            None => {}
        }
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let inner = self.get_ref().appender.inner.borrow();
        if inner.failure.is_some() {
            schedule::Progress::Quiescent
        } else if inner.queue.current.is_some()
            || !inner.queue.pending.is_empty()
            || inner
                .queue
                .in_flight
                .as_ref()
                .is_some_and(|flight| flight.flight.is_none())
        {
            schedule::Progress::Runnable
        } else if inner.queue.in_flight.is_some() {
            schedule::Progress::waiting(region)
        } else {
            schedule::Progress::Quiescent
        }
    }

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _target: typed::Token<'d, Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        self.as_ref().get_ref().appender.inner.borrow_mut().close();
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        finish.stage_route(&self.as_ref().get_ref().appender.route);
    }
}
