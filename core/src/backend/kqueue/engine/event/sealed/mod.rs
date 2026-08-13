mod dispatch;
mod poll;
mod queue;

use std::{os::fd, process};

pub(in crate::backend::kqueue) use dispatch::Dispatch;
pub(in crate::backend::kqueue) use poll::Poll;
pub(in crate::backend::kqueue) use queue::Queue;

use crate::{
    backend::{
        self,
        kqueue::{descriptor, engine::lifecycle},
    },
    driver::{self, flight, route},
    io::{self, event::open, fd::handles},
    platform,
};

enum KernelData {
    Public(flight::raw::Echo),
    Shutdown,
    Empty,
}

impl KernelData {
    fn decode(raw: u64) -> Self {
        if raw == u64::MAX {
            return KernelData::Shutdown;
        }
        match unsafe { flight::raw::Echo::from_kernel(raw) } {
            Some(key) => KernelData::Public(key),
            None => KernelData::Empty,
        }
    }
}

type Buffer = <backend::Kqueue as platform::Buffer>::Token;

pub(crate) enum CreateOutcome {
    Ready {
        slot: handles::FixedSlot,
        fd: descriptor::Handle,
    },
    Failed {
        slot: handles::FixedSlot,
        errno: i32,
    },
    Cancelled {
        slot: handles::FixedSlot,
    },
}

impl CreateOutcome {
    pub(super) const fn slot(&self) -> Option<handles::FixedSlot> {
        match self {
            Self::Ready { slot, .. } | Self::Failed { slot, .. } | Self::Cancelled { slot } => {
                Some(*slot)
            }
        }
    }
}

pub(crate) enum Completion {
    AcceptSuccess {
        ud: flight::raw::Echo,
        accepted: handles::Accepted,
        more: bool,
    },
    AcceptFailure {
        ud: flight::raw::Echo,
        errno: i32,
        more: bool,
    },
    RecvData {
        ud: flight::raw::Echo,
        len: u32,
        more: bool,
        buffer: Buffer,
    },
    RecvControl {
        ud: flight::raw::Echo,
        result: i32,
        more: bool,
    },
    Send {
        ud: flight::raw::Echo,
        result: i32,
    },
    Connect {
        ud: flight::raw::Echo,
        result: i32,
    },
    Create {
        ud: flight::raw::Echo,
        outcome: CreateOutcome,
    },
    Opened {
        ud: flight::raw::Echo,
        fd: fd::OwnedFd,
    },
    OpenFailed {
        ud: flight::raw::Echo,
        error: open::Error,
    },
    Read {
        ud: flight::raw::Echo,
        result: i32,
    },
    Write {
        ud: flight::raw::Echo,
        result: i32,
    },
    Stat {
        ud: flight::raw::Echo,
        result: i32,
    },
    Sync {
        ud: flight::raw::Echo,
        result: i32,
    },
    Shutdown,
}

impl Completion {
    pub(crate) fn into_completion(
        self,
        files: &mut lifecycle::Files,
        drain: &flight::Drain<'_, '_>,
    ) -> io::Completion {
        let driver = drain.driver();
        match self {
            Self::AcceptSuccess { ud, accepted, more } => {
                let token = completion(ud, more, drain);
                io::Completion::accepted(token, accepted, more)
            }
            Self::AcceptFailure { ud, errno, more } => {
                let token = completion(ud, more, drain);
                io::Completion::accept_failed(token, errno, more)
            }
            Self::RecvData {
                ud,
                len,
                more,
                buffer,
            } => {
                let token = completion(ud, more, drain);
                io::Completion::received(token, len, more, buffer)
            }
            Self::RecvControl { ud, result, more } => {
                let token = completion(ud, more, drain);
                io::Completion::operation(token, result, more)
            }
            Self::Send { ud, result } | Self::Connect { ud, result } => {
                let token = terminal(ud, drain);
                io::Completion::operation(token, result, false)
            }
            Self::Create { ud, outcome } => match outcome {
                CreateOutcome::Ready { slot, fd } => {
                    files.install_outbound(slot, fd);
                    io::Completion::socket_created(terminal(ud, drain), slot)
                }
                CreateOutcome::Failed { slot, errno } => {
                    socket_failed(driver, terminal(ud, drain), slot, errno)
                }
                CreateOutcome::Cancelled { slot } => {
                    socket_failed(driver, terminal(ud, drain), slot, libc::ECANCELED)
                }
            },
            Self::Opened { ud, fd } => {
                io::Completion::opened(terminal(ud, drain), open::Opened::new(fd))
            }
            Self::OpenFailed { ud, error } => {
                io::Completion::open_failed(terminal(ud, drain), error)
            }
            Self::Read { ud, result }
            | Self::Write { ud, result }
            | Self::Stat { ud, result }
            | Self::Sync { ud, result } => {
                io::Completion::operation(terminal(ud, drain), result, false)
            }
            Self::Shutdown => io::Completion::shutdown(),
        }
    }

    pub(super) fn token(&self) -> Option<flight::raw::Echo> {
        match self {
            Self::AcceptSuccess { ud, .. }
            | Self::AcceptFailure { ud, .. }
            | Self::RecvData { ud, .. }
            | Self::RecvControl { ud, .. }
            | Self::Send { ud, .. }
            | Self::Connect { ud, .. }
            | Self::Create { ud, .. }
            | Self::Opened { ud, .. }
            | Self::OpenFailed { ud, .. }
            | Self::Read { ud, .. }
            | Self::Write { ud, .. }
            | Self::Stat { ud, .. }
            | Self::Sync { ud, .. } => Some(*ud),
            Self::Shutdown => None,
        }
    }
}

fn completion(key: flight::raw::Echo, more: bool, drain: &flight::Drain<'_, '_>) -> route::Token {
    let Some(completion) = drain.complete(key) else {
        process::abort();
    };
    let Some(token) = completion.resolve(more) else {
        process::abort();
    };
    token
}

fn terminal(key: flight::raw::Echo, drain: &flight::Drain<'_, '_>) -> route::Token {
    completion(key, false, drain)
}

fn socket_failed(
    driver: driver::Reference<'_>,
    token: route::Token,
    slot: handles::FixedSlot,
    errno: i32,
) -> io::Completion {
    if let Some(slots) = driver.outbound().complete_outbound_create_failure(slot) {
        driver.maintenance().defer_outbound_slots(slots);
    }
    io::Completion::socket_failure(token, errno)
}
