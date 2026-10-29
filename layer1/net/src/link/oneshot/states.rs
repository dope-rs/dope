use dope_core::{
    dope_sys::engine::admit::Cancelled,
    driver::{
        self,
        result::SubmitCause,
        retained::{
            self,
            flight::{self, cancel},
        },
        route::{self, Identity, kind},
    },
    io::{
        fd::handles,
        socket::{self, option, pending},
        transfer,
    },
};
use o3::base::time::Positive;

use crate::link::{
    self,
    oneshot::{Error, phase, phase::Phase},
};

pub(super) struct Creating<'d, State = pending::Live> {
    pub(super) socket: handles::Creation<'d, State>,
    pub(super) options: option::Set,
    pub(super) send_window: Positive,
}
pub(super) struct ConnectRetry<'d> {
    pub(super) fd: handles::Descriptor<'d>,
    pub(super) options: option::Set,
    pub(super) send_window: Positive,
}
pub(super) struct Connecting<'d, State = pending::Live> {
    pub(super) pending: pending::Connection<'d, State>,
    pub(super) send_window: Positive,
}
pub(super) struct Sending<'d> {
    pub(super) fd: handles::Descriptor<'d>,
    pub(super) flight: flight::Flight<'d, kind::Send>,
    pub(super) bytes: transfer::NonZero,
}

impl<'d> ConnectRetry<'d> {
    pub(super) fn submit<ID: Identity, A: socket::Prepared>(
        self,
        addr: &A,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> (Phase<'d>, phase::Dial<'d>) {
        let Self {
            fd,
            options: opts,
            send_window: window,
        } = self;
        match link::Connect::new(fd, addr, target).submit(slots, driver.reborrow(), opts) {
            Ok(pending) => (
                Phase::Connecting(Connecting {
                    pending,
                    send_window: window,
                }),
                phase::Dial::Pending,
            ),
            Err(reject) => {
                let (fd, cause) = reject.into_parts();
                match cause {
                    pending::Cause::Submit(error) => match error.cause() {
                        SubmitCause::Full => (
                            Phase::ConnectRetry(Self {
                                fd,
                                options: opts,
                                send_window: window,
                            }),
                            phase::Dial::Retry(error),
                        ),
                        SubmitCause::IdentityExhausted => (
                            Phase::Retired,
                            phase::Dial::Failed(Error::Submit(error), fd),
                        ),
                    },
                    pending::Cause::Option(error) => (
                        Phase::Retired,
                        phase::Dial::Failed(Error::Option(error), fd),
                    ),
                }
            }
        }
    }
}

impl<'d> Connecting<'d> {
    pub(super) fn cancel(
        self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Phase<'d>, Result<phase::Cancellation<'d>, cancel::Failure>) {
        let Self {
            pending,
            send_window: window,
        } = self;
        match pending.try_cancel(driver) {
            Ok(pending) => (
                Phase::ConnectingCancelled(pending),
                Ok(phase::Cancellation::Pending),
            ),
            Err((pending, error)) => {
                let phase = Phase::Connecting(Self {
                    pending,
                    send_window: window,
                });
                match error {
                    cancel::Error::Full(error) => (phase, Ok(phase::Cancellation::Retry(error))),
                    cancel::Error::Rejected(cause) => (phase, Err(cause)),
                }
            }
        }
    }
}

impl<'d> Sending<'d> {
    pub(super) fn cancel(
        mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Phase<'d>, Result<phase::Cancellation<'d>, cancel::Failure>) {
        let result = match self.flight.try_cancel(driver) {
            Ok(status) => match status {
                Cancelled::Submitted | Cancelled::Queued | Cancelled::Completed => {
                    Ok(phase::Cancellation::Pending)
                }
            },
            Err(cancel::Error::Full(error)) => Ok(phase::Cancellation::Retry(error)),
            Err(cancel::Error::Rejected(cause)) => Err(cause),
        };
        (Phase::Sending(self), result)
    }
}
