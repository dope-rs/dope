pub mod datagram;
pub mod fd;
pub(crate) mod ffi;
pub mod file;
pub mod pipe;
pub mod recv;
pub mod socket;

use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::Error;
use std::os::fd::{FromRawFd, OwnedFd};

use fd::AcceptedSlot;
use libc::{EAGAIN, ECANCELED, EINTR, ENOBUFS};
use recv::Lease;
use recv::completion::Completion;

use crate::driver::DriverRef;
use crate::driver::token::kind::{
    ACCEPT, CONNECT, OPEN, READ, RECV, RECV_DISCARD, SEND, SOCKET, STAT, SYNC, TIMER, WRITE,
};
use crate::driver::token::{SHUTDOWN, Token};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError;

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("invalid completion")
    }
}

impl error::Error for DecodeError {}

pub(crate) const BUFFER: u32 = 1 << 0;
pub(crate) const MORE: u32 = 1 << 1;
pub(crate) const BUFFER_SHIFT: u32 = 16;

#[derive(Clone, Copy)]
pub(crate) struct Cqe {
    token: Token,
    result: i32,
    flags: u32,
}

impl Cqe {
    pub(crate) const fn new(token: Token, result: i32, flags: u32) -> Self {
        Self {
            token,
            result,
            flags,
        }
    }

    pub(crate) fn kind(self) -> u8 {
        self.token.kind()
    }

    fn more(self) -> bool {
        self.flags & MORE != 0
    }

    fn bid_raw(self) -> u16 {
        (self.flags >> BUFFER_SHIFT) as u16
    }

    fn has_buffer(self) -> bool {
        self.flags & BUFFER != 0
    }
}

pub enum RecvEvent<'d> {
    Data(Lease<'d>),
    Discarded { len: u32 },
    Eof,
    Cancelled,
    Starved,
    Failed(i32),
}

impl RecvEvent<'_> {
    fn from_errno(result: i32) -> Self {
        match -result {
            ECANCELED => Self::Cancelled,
            ENOBUFS | EAGAIN | EINTR => Self::Starved,
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
pub enum WriteEvent {
    Wrote(u32),
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum SyncEvent {
    Synced,
    Failed(i32),
}

pub enum OpenEvent {
    Opened(OwnedFd),
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum ReadEvent {
    Read(u32),
    Eof,
    Failed(i32),
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
pub struct TimerEvent(i32);

impl TimerEvent {
    const fn from_result(result: i32) -> Self {
        Self(result)
    }

    pub const fn raw_result(self) -> i32 {
        self.0
    }

    pub const fn is_cancelled(self) -> bool {
        self.0 == -ECANCELED
    }
}

pub enum AcceptEvent<'d> {
    Accepted(AcceptedSlot<'d>),
    Failed(i32),
}

pub enum SocketEvent {
    Created,
    Failed(Error),
}

pub enum ConnectEvent {
    Connected,
    Failed(Error),
}

pub enum Event<'d> {
    Accept(Token, bool, AcceptEvent<'d>),
    Recv(Token, bool, RecvEvent<'d>),
    Send(Token, SendEvent),
    Timer(Token, TimerEvent),
    Socket(Token, SocketEvent),
    Connect(Token, ConnectEvent),
    Write(Token, WriteEvent),
    Sync(Token, SyncEvent),
    Open(Token, OpenEvent),
    Read(Token, ReadEvent),
    Stat(Token, StatEvent),
    Shutdown,
}

impl<'d> Event<'d> {
    pub(crate) fn from_cqe(
        cqe: Cqe,
        reference: DriverRef<'d>,
        received: impl FnOnce(u32, u16) -> Option<Completion>,
    ) -> Result<Self, DecodeError> {
        let result = cqe.result;
        let operation = cqe.kind();
        let mut received = cqe
            .has_buffer()
            .then(|| received(result.max(0) as u32, cqe.bid_raw()))
            .flatten()
            .map(|completion| Lease::from_completion(reference, completion));
        let token = cqe.token;
        let event = if token == SHUTDOWN {
            Event::Shutdown
        } else {
            match operation {
                ACCEPT => {
                    let event = if result >= 0 {
                        let slot = reference.fixed_fd_slot(result as u32).ok_or(DecodeError)?;
                        AcceptEvent::Accepted(AcceptedSlot::from_completion(slot, reference))
                    } else {
                        AcceptEvent::Failed(-result)
                    };
                    Event::Accept(token, cqe.more(), event)
                }
                RECV => {
                    let event = if result > 0 {
                        if !cqe.has_buffer() {
                            debug_assert!(false, "RECV data cqe without buffer flag");
                            return Err(DecodeError);
                        }
                        let lease = received.take().ok_or(DecodeError)?;
                        debug_assert_eq!(lease.as_slice().len(), result as usize);
                        RecvEvent::Data(lease)
                    } else if result == 0 {
                        RecvEvent::Eof
                    } else {
                        RecvEvent::from_errno(result)
                    };
                    Event::Recv(token, cqe.more(), event)
                }
                RECV_DISCARD => {
                    let event = if result > 0 {
                        RecvEvent::Discarded { len: result as u32 }
                    } else if result == 0 {
                        RecvEvent::Eof
                    } else {
                        RecvEvent::from_errno(result)
                    };
                    Event::Recv(token, cqe.more(), event)
                }
                SEND => Event::Send(
                    token,
                    if result >= 0 {
                        SendEvent::Sent(result as u32)
                    } else {
                        SendEvent::Failed(-result)
                    },
                ),
                WRITE => Event::Write(
                    token,
                    if result >= 0 {
                        WriteEvent::Wrote(result as u32)
                    } else {
                        WriteEvent::Failed(-result)
                    },
                ),
                SYNC => Event::Sync(
                    token,
                    if result >= 0 {
                        SyncEvent::Synced
                    } else {
                        SyncEvent::Failed(-result)
                    },
                ),
                OPEN => Event::Open(
                    token,
                    if result >= 0 {
                        // SAFETY: successful OPEN returns a fresh owned descriptor.
                        OpenEvent::Opened(unsafe { OwnedFd::from_raw_fd(result) })
                    } else {
                        OpenEvent::Failed(-result)
                    },
                ),
                READ => Event::Read(token, ReadEvent::from_result(result)),
                STAT => Event::Stat(
                    token,
                    if result >= 0 {
                        StatEvent::Done
                    } else {
                        StatEvent::Failed(-result)
                    },
                ),
                TIMER => Event::Timer(token, TimerEvent::from_result(result)),
                SOCKET => Event::Socket(
                    token,
                    if result >= 0 {
                        SocketEvent::Created
                    } else {
                        SocketEvent::Failed(Error::from_raw_os_error(-result))
                    },
                ),
                CONNECT => Event::Connect(
                    token,
                    if result >= 0 {
                        ConnectEvent::Connected
                    } else {
                        ConnectEvent::Failed(Error::from_raw_os_error(-result))
                    },
                ),
                _ => return Err(DecodeError),
            }
        };
        Ok(event)
    }

    pub const fn is_shutdown(&self) -> bool {
        matches!(self, Event::Shutdown)
    }

    pub const fn token(&self) -> Option<Token> {
        match self {
            Event::Accept(token, ..)
            | Event::Recv(token, ..)
            | Event::Send(token, _)
            | Event::Timer(token, _)
            | Event::Socket(token, _)
            | Event::Connect(token, _)
            | Event::Write(token, _)
            | Event::Sync(token, _)
            | Event::Open(token, _)
            | Event::Read(token, _)
            | Event::Stat(token, _) => Some(*token),
            Event::Shutdown => None,
        }
    }

    pub fn route(&self) -> u8 {
        match self {
            Event::Accept(t, ..) => t.route(),
            Event::Recv(t, ..) => t.route(),
            Event::Send(t, _) => t.route(),
            Event::Timer(t, _) => t.route(),
            Event::Socket(t, _) => t.route(),
            Event::Connect(t, _) => t.route(),
            Event::Write(t, _) => t.route(),
            Event::Sync(t, _) => t.route(),
            Event::Open(t, _) => t.route(),
            Event::Read(t, _) => t.route(),
            Event::Stat(t, _) => t.route(),
            Event::Shutdown => SHUTDOWN.route(),
        }
    }
}
