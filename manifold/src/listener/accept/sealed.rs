use std::{pin, process};

use dope_core::{
    driver::{self, ops, retained, route, schedule},
    io::{self, event::accept},
};

use crate::dispatch::typed::arms;

pub(in crate::listener) trait Source<'d, const ID: u8> {
    fn arm(
        self: pin::Pin<&mut Self>,
        driver: &mut retained::Context<'_, '_, 'd>,
        available: usize,
        work: schedule::Maintenance<'_, 'd>,
    );

    fn complete_source(
        self: pin::Pin<&mut Self>,
        token: route::Token,
        completion: accept::Completion<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> super::AcceptOutcome<'d>;
}

impl<'d, const ID: u8> Source<'d, ID> for super::Accept<'d, ID> {
    fn arm(
        self: pin::Pin<&mut Self>,
        driver: &mut retained::Context<'_, '_, 'd>,
        available: usize,
        work: schedule::Maintenance<'_, 'd>,
    ) {
        let this = self.project();
        let fd = this.fd.as_ref().get_ref();
        match this.mode {
            super::Mode::Multishot { arm, enabled } => {
                *enabled = available != 0;
                if !*enabled {
                    return;
                }
                let Some(arming) = arm.begin_if(|| work.take()) else {
                    return;
                };
                let flight = ops::Submit::submit_accept_multishot(
                    driver.driver(),
                    this.flights,
                    fd,
                    arming.identity(),
                );
                arming.resolve_submission(flight);
            }
            super::Mode::Oneshot(lanes) => {
                lanes.target = available.min(lanes.entries.len());
                if !lanes.accepting {
                    return;
                }
                let needed = lanes.target.saturating_sub(lanes.active.get());
                let mut remaining = needed;
                for _ in 0..lanes.entries.len() {
                    if remaining == 0 || work.remaining() == 0 {
                        break;
                    }
                    let index = lanes.cursor;
                    lanes.cursor = (index + 1) % lanes.entries.len();
                    let Some(lane) = lanes.entries.get_mut(index) else {
                        process::abort();
                    };
                    let mut lane = lane.project();
                    let Some(arming) = lane.arm.begin_if(|| work.take()) else {
                        continue;
                    };
                    unsafe { lane.peer_addr.as_mut().reset() };
                    let submission = retained::raw::Submission::accept_oneshot(
                        fd,
                        lane.peer_addr.as_mut(),
                        arming.identity(),
                    );
                    // SAFETY: pinned::Slice keeps this lane and its peer address
                    // fixed. Its Arm prevents reuse until terminal completion,
                    // while the installed Accept owner retains both the lane
                    // slice and its pinned descriptor through quiescence.
                    let flight =
                        unsafe { retained::raw::Owner::submit(driver, this.flights, submission) };
                    let Some(armed) = arming.resolve_submission(flight) else {
                        lanes.cursor = index;
                        break;
                    };
                    lanes.active.activate(armed);
                    remaining -= 1;
                }
            }
        }
    }

    fn complete_source(
        self: pin::Pin<&mut Self>,
        token: route::Token,
        completion: accept::Completion<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> super::AcceptOutcome<'d> {
        let (more, event) = completion.into_parts();
        let this = self.project();
        match this.mode {
            super::Mode::Multishot { arm, .. } => {
                let disposition = arm.complete_multishot(token, more);
                let io::AcceptEvent::Accepted(slot) = event else {
                    return super::AcceptOutcome::Rejected;
                };
                let fd = slot.bind();
                if disposition == arms::Disposition::Discard || fd.index() >= this.accept_slot.raw()
                {
                    ops::Files::close(driver, fd);
                    return super::AcceptOutcome::Rejected;
                }
                super::AcceptOutcome::Accepted(fd, None)
            }
            super::Mode::Oneshot(lanes) => {
                debug_assert!(!more);
                if token.slot() != *this.accept_slot {
                    return close_if_accepted(event, driver);
                }
                let Some(epoch) = token.epoch() else {
                    return close_if_accepted(event, driver);
                };
                let Some(index) = usize::try_from(epoch.raw().saturating_sub(1)).ok() else {
                    return close_if_accepted(event, driver);
                };
                let Some(lane) = lanes.entries.get_mut(index) else {
                    return close_if_accepted(event, driver);
                };
                let lane = lane.project();
                let Some(retirement) = lane.arm.complete_oneshot(token) else {
                    return close_if_accepted(event, driver);
                };
                lanes.cursor = index;
                let terminal = lanes.active.retire(retirement);
                let io::AcceptEvent::Accepted(slot) = event else {
                    return super::AcceptOutcome::Rejected;
                };
                let fd = slot.bind();
                let Some(terminal) = terminal else {
                    ops::Files::close(driver, fd);
                    return super::AcceptOutcome::Rejected;
                };
                if fd.index() >= this.accept_slot.raw() {
                    ops::Files::close(driver, fd);
                    return super::AcceptOutcome::Rejected;
                }
                let peer_ip = unsafe { lane.peer_addr.as_ref().snapshot() }
                    .into_std()
                    .ok()
                    .map(|address| address.ip());
                drop(terminal);
                super::AcceptOutcome::Accepted(fd, peer_ip)
            }
        }
    }
}

fn close_if_accepted<'d>(
    event: io::AcceptEvent<'d>,
    driver: &mut driver::Context<'_, 'd>,
) -> super::AcceptOutcome<'d> {
    if let io::AcceptEvent::Accepted(slot) = event {
        ops::Files::close(driver, slot.bind());
    }
    super::AcceptOutcome::Rejected
}
