use std::mem;

use dope_core::{
    dope_sys::engine::admit::Cancelled,
    driver,
    driver::{
        result, retained,
        retained::{flight, flight::cancel},
        route,
        route::Identity,
        schedule::clock,
    },
    io::{
        event::net::socket::{Completion, Failure, Finished},
        fd::handles,
        socket,
        socket::{option, pending},
    },
};
use o3::{base::time::Positive, mem::Rejected};

use crate::link::oneshot::{
    Error,
    states::{ConnectRetry, Connecting, Creating, Sending},
};

pub(super) enum Retry<'d> {
    Cancel(Result<Cancellation<'d>, cancel::Failure>),
    Connect(Dial<'d>),
    Send(SendProgress<'d>),
    Idle,
}
pub(super) enum Socket<'d> {
    Pending,
    Cancelled(handles::Descriptor<'d>),
    Retry,
    Failed(Error, handles::Descriptor<'d>),
    Finished(Error),
    Stale,
}
pub(super) enum Dial<'d> {
    Pending,
    Retry(result::SubmitError),
    Failed(Error, handles::Descriptor<'d>),
}
pub(super) enum Progress<'d> {
    Connected(clock::Deadline<'d>, SendProgress<'d>),
    Failed(Error, handles::Descriptor<'d>),
    Cancelled(handles::Descriptor<'d>),
}
pub(super) enum SendProgress<'d> {
    Complete(handles::Descriptor<'d>),
    Submitted,
    Retry(result::SubmitError),
    Cancelled(handles::Descriptor<'d>),
    Failed(Error, handles::Descriptor<'d>),
}
pub(super) enum Cancellation<'d> {
    Idle(handles::Descriptor<'d>),
    Pending,
    Retry(result::SubmitError),
}

pub(super) enum Phase<'d> {
    Creating(Creating<'d>),
    CreatingCancelled(Creating<'d, pending::Cancelled>),
    ConnectRetry(ConnectRetry<'d>),
    Connecting(Connecting<'d>),
    ConnectingCancelled(pending::Connection<'d, pending::Cancelled>),
    SendRetry(handles::Descriptor<'d>),
    Sending(Sending<'d>),
    Retired,
}

impl<'d> Phase<'d> {
    pub(super) fn creating(
        socket: handles::Creation<'d>,
        opts: option::Set,
        window: Positive,
    ) -> Self {
        Self::Creating(Creating {
            socket,
            options: opts,
            send_window: window,
        })
    }

    pub(super) fn socket<ID: Identity, A: socket::Prepared>(
        self,
        done: Completion<'d>,
        addr: &A,
        target: route::Target<'d, ID>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
        stopped: bool,
    ) -> (Self, Socket<'d>) {
        let build = match self {
            Self::Creating(build) => build,
            Self::CreatingCancelled(build) => {
                let Creating {
                    socket,
                    options,
                    send_window: window,
                } = build;
                return match done.try_finish(socket) {
                    Ok(Finished::Created(fd)) => (Self::Retired, Socket::Cancelled(fd)),
                    Ok(Finished::Failed(Failure::Create(error))) => {
                        (Self::Retired, Socket::Finished(Error::Socket(error)))
                    }
                    Ok(Finished::Failed(Failure::Retired)) => {
                        (Self::Retired, Socket::Finished(Error::Abandoned))
                    }
                    Err(mismatch) => {
                        let socket = mismatch.reclaim();
                        (
                            Self::CreatingCancelled(Creating {
                                socket,
                                options,
                                send_window: window,
                            }),
                            Socket::Stale,
                        )
                    }
                };
            }
            state => {
                done.reclaim();
                return (state, Socket::Stale);
            }
        };
        let Creating {
            socket,
            options: opts,
            send_window: window,
        } = build;
        let fd = match done.try_finish(socket) {
            Ok(Finished::Created(fd)) if stopped => {
                return (Self::Retired, Socket::Cancelled(fd));
            }
            Ok(Finished::Created(fd)) => fd,
            Ok(Finished::Failed(Failure::Create(error))) => {
                return (Self::Retired, Socket::Finished(Error::Socket(error)));
            }
            Ok(Finished::Failed(Failure::Retired)) => {
                return (Self::Retired, Socket::Finished(Error::Abandoned));
            }
            Err(mismatch) => {
                let socket = mismatch.reclaim();
                return (Self::creating(socket, opts, window), Socket::Stale);
            }
        };
        let (state, dial) = ConnectRetry {
            fd,
            options: opts,
            send_window: window,
        }
        .submit(addr, target, slots, driver);
        let outcome = match dial {
            Dial::Pending => Socket::Pending,
            Dial::Retry(_) => Socket::Retry,
            Dial::Failed(error, fd) => Socket::Failed(error, fd),
        };
        (state, outcome)
    }

    pub(super) fn cancel(
        self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Self, Result<Cancellation<'d>, cancel::Failure>) {
        let (phase, progress) = match self {
            Self::Creating(build) => {
                let Creating {
                    socket,
                    options,
                    send_window: window,
                } = build;
                match socket.try_cancel(driver).map_err(Rejected::into_parts) {
                    Ok((
                        socket,
                        Cancelled::Submitted | Cancelled::Queued | Cancelled::Completed,
                    )) => (
                        Self::CreatingCancelled(Creating {
                            socket,
                            options,
                            send_window: window,
                        }),
                        Cancellation::Pending,
                    ),
                    Err((socket, cause)) => {
                        let restored = Self::Creating(Creating {
                            socket,
                            options,
                            send_window: window,
                        });
                        return match cause {
                            cancel::Error::Full(error) => {
                                (restored, Ok(Cancellation::Retry(error)))
                            }
                            cancel::Error::Rejected(cause) => (restored, Err(cause)),
                        };
                    }
                }
            }
            Self::Connecting(dial) => return dial.cancel(driver),
            Self::Sending(tx) => return tx.cancel(driver),
            Self::ConnectRetry(retry) => (Self::Retired, Cancellation::Idle(retry.fd)),
            Self::SendRetry(fd) => (Self::Retired, Cancellation::Idle(fd)),
            state => (state, Cancellation::Pending),
        };
        (phase, Ok(progress))
    }
}

const _: () = assert!(mem::size_of::<Phase<'static>>() <= 88);
