use std::mem;

use dope_core::{
    driver::{
        self,
        result::SubmitCause,
        retained::{
            self,
            flight::{self, cancel},
        },
        route,
        route::Identity,
        schedule::clock,
    },
    io::{
        event::net::{connect, send::Completion, socket},
        fd::handles,
        socket::Prepared,
        transfer,
    },
};

use crate::{
    link::{
        egress::data,
        oneshot::{
            Error, Request, phase,
            phase::Phase,
            states::{Connecting, Sending},
        },
    },
    wire::send::{self, Payload as _, Plain, Submitted},
};

/// The route slab retains this cell in place until terminal completion or quiescence.
/// Only `phase` moves during an operation; CONNECT borrows the inline address.
pub(in crate::link::oneshot) struct Cell<'d, A: 'd, P: Prepared, B: 'd> {
    pub(in crate::link::oneshot) authority: Option<A>,
    address: P,
    phase: phase::Phase<'d>,
    payload: B,
    offset: usize,
}

impl<'d, A: 'd, P, B: 'd> Cell<'d, A, P, B>
where
    P: Prepared,
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    pub(in crate::link::oneshot) fn new(
        req: Request<'d, A, P, B>,
        socket: handles::Creation<'d>,
    ) -> (Self, clock::Deadline<'d>) {
        (
            Self {
                authority: Some(req.authority),
                address: req.address,
                phase: Phase::creating(socket, req.options, req.send_window),
                payload: req.payload,
                offset: 0,
            },
            req.connect_deadline,
        )
    }

    pub(in crate::link::oneshot) fn socket<ID: Identity>(
        &mut self,
        done: socket::Completion<'d>,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> phase::Socket<'d> {
        let current = mem::replace(&mut self.phase, Phase::Retired);
        let (phase, progress) = current.socket(
            done,
            &self.address,
            target,
            slots,
            driver,
            self.authority.is_none(),
        );
        self.phase = phase;
        progress
    }

    pub(in crate::link::oneshot) fn connect<ID: Identity>(
        &mut self,
        done: connect::Completion<'d>,
        now: clock::Moment<'d>,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<phase::Progress<'d>, connect::Completion<'d>> {
        use phase::Progress;

        let current = mem::replace(&mut self.phase, Phase::Retired);
        let progress = match current {
            Phase::Connecting(Connecting {
                pending,
                send_window: window,
            }) => {
                let (fd, outcome) = match pending.complete(done) {
                    Ok(done) => done,
                    Err(cause) => {
                        let (pending, done) = cause.into_parts();
                        self.phase = Phase::Connecting(Connecting {
                            pending,
                            send_window: window,
                        });
                        return Err(done);
                    }
                };
                match outcome {
                    Ok(()) if self.authority.is_none() => Progress::Cancelled(fd),
                    Ok(()) => match now.try_add(window) {
                        Ok(until) => {
                            let sent = self.submit(fd, target, slots, driver);
                            Progress::Connected(until, sent)
                        }
                        Err(range) => Progress::Failed(Error::SendDeadlineOverflow(range), fd),
                    },
                    Err(errno) => Progress::Failed(Error::Connect(errno), fd),
                }
            }
            Phase::ConnectingCancelled(pending) => match pending.complete(done) {
                Ok((fd, Ok(()))) => Progress::Cancelled(fd),
                Ok((fd, Err(errno))) => Progress::Failed(Error::Connect(errno), fd),
                Err(cause) => {
                    let (pending, done) = cause.into_parts();
                    self.phase = Phase::ConnectingCancelled(pending);
                    return Err(done);
                }
            },
            phase => {
                self.phase = phase;
                return Err(done);
            }
        };
        Ok(progress)
    }

    pub(in crate::link::oneshot) fn retry<ID: Identity>(
        &mut self,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> phase::Retry<'d> {
        use phase::Retry;

        let current = mem::replace(&mut self.phase, Phase::Retired);
        if self.authority.is_none() {
            let (phase, progress) = current.cancel(driver.driver());
            self.phase = phase;
            return Retry::Cancel(progress);
        }
        let (phase, progress) = match current {
            Phase::ConnectRetry(retry) => {
                let (phase, progress) = retry.submit(&self.address, target, slots, driver);
                (phase, Retry::Connect(progress))
            }
            Phase::SendRetry(fd) => {
                return Retry::Send(self.submit(fd, target, slots, driver));
            }
            phase => (phase, Retry::Idle),
        };
        self.phase = phase;
        progress
    }

    pub(in crate::link::oneshot) fn send<ID: Identity>(
        &mut self,
        done: Completion<'d>,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<phase::SendProgress<'d>, Completion<'d>> {
        let phase = mem::replace(&mut self.phase, Phase::Retired);
        let tx = match phase {
            Phase::Sending(tx) if tx.flight.matches(done.token()) => tx,
            phase => {
                self.phase = phase;
                return Err(done);
            }
        };
        let Sending {
            fd,
            flight,
            bytes: bound,
        } = tx;
        drop(flight);
        let progress = match done.outcome() {
            Ok(bytes) => {
                if bytes.is_zero() || bytes > transfer::Len::from_nonzero(bound) {
                    return Ok(phase::SendProgress::Failed(
                        Error::SendAmount {
                            observed: bytes,
                            submitted: bound,
                        },
                        fd,
                    ));
                }
                self.offset += bytes.into_raw();
                if self.authority.is_none() {
                    phase::SendProgress::Cancelled(fd)
                } else {
                    self.submit(fd, target, slots, driver)
                }
            }
            Err(errno) => phase::SendProgress::Failed(Error::Send(errno), fd),
        };
        Ok(progress)
    }

    pub(in crate::link::oneshot) fn cancel(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<Cancelled<'d, A>> {
        let owner = self.authority.take()?;
        let current = mem::replace(&mut self.phase, Phase::Retired);
        let (phase, progress) = current.cancel(driver);
        self.phase = phase;
        Some((owner, progress))
    }

    fn submit<ID: Identity>(
        &mut self,
        fd: handles::Descriptor<'d>,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> phase::SendProgress<'d> {
        // SAFETY: only this cell's matching flight advances offset, by at most
        // its submitted prefix. StableOwner retains the same immutable backing.
        let tail = unsafe { self.payload.as_ref().get_unchecked(self.offset..) };
        let plain = Plain::proven(tail);
        let ready = match plain.select() {
            None => return phase::SendProgress::Complete(fd),
            Some(ready) => ready,
        };
        // SAFETY: Io binds this installed pool to the driver. Its resident Cell
        // retains the immutable StableOwner suffix and descriptor until this
        // exact flight completes or quiescence permits finalization.
        match unsafe { send::raw::Ready::submit(ready, &fd, target, slots, driver.reborrow()) } {
            Ok(Submitted { bytes, flight }) => {
                self.phase = Phase::Sending(Sending { fd, flight, bytes });
                phase::SendProgress::Submitted
            }
            Err(error) => match error.cause() {
                SubmitCause::Full => {
                    self.phase = Phase::SendRetry(fd);
                    phase::SendProgress::Retry(error)
                }
                SubmitCause::IdentityExhausted => {
                    phase::SendProgress::Failed(Error::Submit(error), fd)
                }
            },
        }
    }
}

type Cancelled<'d, A> = (A, Result<phase::Cancellation<'d>, cancel::Failure>);
