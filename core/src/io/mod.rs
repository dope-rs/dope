pub mod datagram;
pub mod event;
pub mod fd;
pub mod fs;
pub mod recv;
pub mod socket;
pub mod transfer;

use std::{io, process};

use self::{
    event::{accept, creation, open, receiving},
    fd::handles,
};
use crate::{
    backend,
    driver::{
        self,
        route::{self, kind},
    },
    platform,
};

type Buffer = <backend::Backend as platform::Buffer>::Token;

#[must_use = "completion may own a resource that must be delivered or reclaimed"]
pub(crate) struct Completion(CompletionKind);

enum CompletionKind {
    Shutdown,
    AcceptSuccess {
        token: route::Token,
        accepted: handles::Accepted,
        more: bool,
    },
    AcceptFailure {
        token: route::Token,
        errno: i32,
        more: bool,
    },
    RecvData {
        token: route::Token,
        len: u32,
        more: bool,
        buffer: Buffer,
    },
    RecvExhausted {
        token: route::Token,
        more: bool,
    },
    Operation {
        token: route::Token,
        result: i32,
        more: bool,
    },
    Opened {
        token: route::Token,
        opened: open::Opened,
    },
    OpenFailed {
        token: route::Token,
        error: open::Error,
    },
    SocketCreated {
        token: route::Token,
        slot: handles::FixedSlot,
    },
    SocketFailed {
        token: route::Token,
        errno: i32,
    },
}

pub(crate) enum Reclaim<'d> {
    Accepted(handles::Accepted),
    Close(driver::Close<'d>),
    Slots(driver::RetiredSlots<'d>),
    Buffer(Buffer),
    None,
}

impl Completion {
    pub(crate) fn accepted(token: route::Token, accepted: handles::Accepted, more: bool) -> Self {
        Self(CompletionKind::AcceptSuccess {
            token,
            accepted,
            more,
        })
    }

    pub(crate) fn accept_failed(token: route::Token, errno: i32, more: bool) -> Self {
        Self(CompletionKind::AcceptFailure { token, errno, more })
    }

    pub(crate) fn shutdown() -> Self {
        Self(CompletionKind::Shutdown)
    }

    pub(crate) fn operation(token: route::Token, result: i32, more: bool) -> Self {
        Self(CompletionKind::Operation {
            token,
            result,
            more,
        })
    }

    pub(crate) fn opened(token: route::Token, opened: open::Opened) -> Self {
        Self(CompletionKind::Opened { token, opened })
    }

    pub(crate) fn open_failed(token: route::Token, error: open::Error) -> Self {
        Self(CompletionKind::OpenFailed { token, error })
    }

    pub(crate) fn socket_created(token: route::Token, slot: handles::FixedSlot) -> Self {
        Self(CompletionKind::SocketCreated { token, slot })
    }

    pub(crate) fn socket_failure(token: route::Token, errno: i32) -> Self {
        Self(CompletionKind::SocketFailed { token, errno })
    }

    pub(crate) fn received(token: route::Token, len: u32, more: bool, buffer: Buffer) -> Self {
        debug_assert_eq!(token.kind(), kind::RECV);
        Self(CompletionKind::RecvData {
            token,
            len,
            more,
            buffer,
        })
    }

    pub(crate) fn recv_exhausted(token: route::Token, more: bool) -> Self {
        debug_assert_eq!(token.kind(), kind::RECV);
        Self(CompletionKind::RecvExhausted { token, more })
    }

    pub(crate) fn into_reclaim<'d>(self, reference: driver::Reference<'d>) -> Reclaim<'d> {
        match self.0 {
            CompletionKind::AcceptSuccess { accepted, .. } => Reclaim::Accepted(accepted),
            CompletionKind::SocketCreated { slot, .. } => {
                match reference.outbound().complete_outbound_create_success(slot) {
                    driver::CreateSuccess::Deliver(key) => {
                        match reference.outbound().begin_outbound_close(key, slot) {
                            driver::CloseDisposition::Submit(close) => Reclaim::Close(close),
                            driver::CloseDisposition::NoSubmit(Some(slots)) => {
                                Reclaim::Slots(slots)
                            }
                            driver::CloseDisposition::NoSubmit(None) => Reclaim::None,
                        }
                    }
                    driver::CreateSuccess::Close(close) => Reclaim::Close(close),
                }
            }
            CompletionKind::RecvData { buffer, .. } => Reclaim::Buffer(buffer),
            CompletionKind::Opened { opened, .. } => {
                drop(opened);
                Reclaim::None
            }
            CompletionKind::Shutdown
            | CompletionKind::AcceptFailure { .. }
            | CompletionKind::RecvExhausted { .. }
            | CompletionKind::Operation { .. }
            | CompletionKind::OpenFailed { .. }
            | CompletionKind::SocketFailed { .. } => Reclaim::None,
        }
    }
}

pub enum RecvEvent<'d> {
    Data(recv::Lease<'d>),
    Eof,
    Cancelled,
    BufferExhausted,
    Starved,
    Failed(i32),
}

impl RecvEvent<'_> {
    fn from_errno(result: i32) -> Self {
        match -result {
            libc::ECANCELED => Self::Cancelled,
            libc::ENOBUFS | libc::EAGAIN | libc::EINTR => Self::Starved,
            errno => Self::Failed(errno),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SendEvent {
    Sent(u32),
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum ReadEvent {
    Read(u32),
    Eof,
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum WriteEvent {
    Written(u32),
    Failed(i32),
}

impl WriteEvent {
    fn from_result(result: i32) -> Self {
        if result >= 0 {
            Self::Written(result as u32)
        } else {
            Self::Failed(-result)
        }
    }
}

impl ReadEvent {
    fn from_result(result: i32) -> Self {
        match result {
            n if n > 0 => Self::Read(n as u32),
            0 => Self::Eof,
            n => Self::Failed(-n),
        }
    }
}

#[derive(Clone, Copy)]
pub enum StatEvent {
    Done,
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum Sync {
    Done,
    Failed(i32),
}

pub enum AcceptEvent<'d> {
    Accepted(handles::AcceptedSlot<'d>),
    Failed(i32),
}

pub enum SocketEvent<'d> {
    Created(handles::CreatedSlot<'d>),
    Failed(io::Error),
}

/// A completion emitted by the driver.
///
/// Its payload is inspectable through [`Event::kind`] or consumable through
/// [`Event::into_kind`], but safe code outside the driver cannot mint the
/// completion authority itself.
///
/// ```compile_fail
/// use dope_core::io::{Event, event};
///
/// let _ = Event(event::Kind::Shutdown);
/// ```
///
/// ```compile_fail
/// use dope_core::{driver::route, io};
///
/// let _ = io::event::send::Completion::new(route::SHUTDOWN, io::SendEvent::Sent(0));
/// ```
#[repr(transparent)]
pub struct Event<'d>(event::Kind<'d>);

const _: () =
    assert!(std::mem::size_of::<Event<'static>>() == std::mem::size_of::<event::Kind<'static>>());

impl<'d> Event<'d> {
    pub(crate) fn from_completion(
        completion: Completion,
        reference: driver::Reference<'d>,
        region: impl FnOnce(u32, &mut Buffer) -> recv::raw::Region,
    ) -> Self {
        use std::io::Error;

        match completion.0 {
            CompletionKind::Shutdown => Event(event::Kind::Shutdown),
            CompletionKind::AcceptSuccess {
                token,
                accepted,
                more,
            } => {
                let event = match accepted.bind(reference) {
                    Some(accepted) => AcceptEvent::Accepted(accepted),
                    None => AcceptEvent::Failed(libc::ECANCELED),
                };
                Event(event::Kind::Accept(
                    token,
                    accept::Completion::new(more, event),
                ))
            }
            CompletionKind::AcceptFailure { token, errno, more } => Event(event::Kind::Accept(
                token,
                accept::Completion::new(more, AcceptEvent::Failed(errno)),
            )),
            CompletionKind::RecvData {
                token,
                len,
                more,
                mut buffer,
            } => {
                use crate::io::recv::Lease;

                let region = region(len, &mut buffer);
                let lease = Lease::from_completion(reference, buffer, region);
                Event(event::Kind::Recv(receiving::Completion::new(
                    token,
                    more,
                    RecvEvent::Data(lease),
                )))
            }
            CompletionKind::RecvExhausted { token, more } => Event(event::Kind::Recv(
                receiving::Completion::new(token, more, RecvEvent::BufferExhausted),
            )),
            CompletionKind::Operation {
                token,
                result,
                more,
            } => Self::from_operation(token, result, more),
            CompletionKind::Opened { token, opened } => Event(event::Kind::Open(
                open::Completion::new(token, open::Outcome::Opened(opened)),
            )),
            CompletionKind::OpenFailed { token, error } => Event(event::Kind::Open(
                open::Completion::new(token, open::Outcome::Failed(error.get())),
            )),
            CompletionKind::SocketCreated { token, slot } => {
                Event(event::Kind::Socket(creation::Completion::new(
                    token,
                    match handles::Created::from_live(slot).bind(reference) {
                        Some(created) => SocketEvent::Created(created),
                        None => SocketEvent::Failed(Error::from_raw_os_error(libc::ECANCELED)),
                    },
                )))
            }
            CompletionKind::SocketFailed { token, errno } => {
                Event(event::Kind::Socket(creation::Completion::new(
                    token,
                    SocketEvent::Failed(Error::from_raw_os_error(errno)),
                )))
            }
        }
    }

    fn from_operation(token: route::Token, result: i32, more: bool) -> Self {
        use std::io::Error;

        match token.kind() {
            kind::RECV => {
                let event = if result == 0 {
                    RecvEvent::Eof
                } else {
                    RecvEvent::from_errno(result)
                };
                Self(event::Kind::Recv(receiving::Completion::new(
                    token, more, event,
                )))
            }
            kind::SEND => {
                use crate::io::event::send;

                Self(event::Kind::Send(send::Completion::new(
                    token,
                    if result >= 0 {
                        SendEvent::Sent(result as u32)
                    } else {
                        SendEvent::Failed(-result)
                    },
                )))
            }
            kind::READ => Self(event::Kind::Read(token, ReadEvent::from_result(result))),
            kind::WRITE => Self(event::Kind::Write(token, WriteEvent::from_result(result))),
            kind::STAT => Self(event::Kind::Stat(
                token,
                if result >= 0 {
                    StatEvent::Done
                } else {
                    StatEvent::Failed(-result)
                },
            )),
            kind::SYNC => Self(event::Kind::Sync(
                token,
                if result >= 0 {
                    Sync::Done
                } else {
                    Sync::Failed(-result)
                },
            )),
            kind::TUNING => {
                use crate::io::event::tuning;

                Self(event::Kind::Tuning(tuning::Completion::new(
                    token,
                    if result >= 0 {
                        tuning::Outcome::Applied
                    } else {
                        tuning::Outcome::Failed(Error::from_raw_os_error(-result))
                    },
                )))
            }
            kind::CONNECT => {
                use crate::io::event::connect;

                Self(event::Kind::Connect(connect::Completion::new(
                    token,
                    if result >= 0 {
                        connect::Outcome::Connected
                    } else {
                        connect::Outcome::Failed(Error::from_raw_os_error(-result))
                    },
                )))
            }
            _ => process::abort(),
        }
    }

    pub const fn is_shutdown(&self) -> bool {
        matches!(self.0, event::Kind::Shutdown)
    }

    pub const fn kind(&self) -> &event::Kind<'d> {
        &self.0
    }

    pub fn into_kind(self) -> event::Kind<'d> {
        self.0
    }

    pub const fn token(&self) -> Option<route::Token> {
        match &self.0 {
            event::Kind::Accept(token, ..)
            | event::Kind::Read(token, _)
            | event::Kind::Write(token, _)
            | event::Kind::Stat(token, _)
            | event::Kind::Sync(token, _) => Some(*token),
            event::Kind::Recv(completion) => Some(completion.token()),
            event::Kind::Send(completion) => Some(completion.token()),
            event::Kind::Socket(completion) => Some(completion.token()),
            event::Kind::Tuning(completion) => Some(completion.token()),
            event::Kind::Connect(completion) => Some(completion.token()),
            event::Kind::Open(completion) => Some(completion.token()),
            event::Kind::Shutdown => None,
        }
    }

    pub fn route(&self) -> u8 {
        match &self.0 {
            event::Kind::Accept(token, ..) => token.route(),
            event::Kind::Recv(completion) => completion.token().route(),
            event::Kind::Send(completion) => completion.token().route(),
            event::Kind::Socket(completion) => completion.token().route(),
            event::Kind::Tuning(completion) => completion.token().route(),
            event::Kind::Connect(completion) => completion.token().route(),
            event::Kind::Open(completion) => completion.token().route(),
            event::Kind::Read(t, _) => t.route(),
            event::Kind::Write(t, _) => t.route(),
            event::Kind::Stat(t, _) => t.route(),
            event::Kind::Sync(t, _) => t.route(),
            event::Kind::Shutdown => route::SHUTDOWN.route(),
        }
    }
}

impl<'d> From<receiving::Completion<'d>> for Event<'d> {
    fn from(completion: receiving::Completion<'d>) -> Self {
        Self(event::Kind::Recv(completion))
    }
}
