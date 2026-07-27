pub mod datagram;
pub mod fd;
pub(crate) mod ffi;
pub mod file;
pub mod pipe;
pub mod provided;
pub mod socket;

use std::error::Error as StdError;
use std::fmt;
use std::io::Error;
use std::os::fd::OwnedFd;

use crate::driver::DriverRef;
use crate::driver::token::kind::ACCEPT;
use crate::driver::token::kind::CONNECT;
use crate::driver::token::kind::OPEN;
use crate::driver::token::kind::READ;
use crate::driver::token::kind::RECV;
use crate::driver::token::kind::RECV_DISCARD;
use crate::driver::token::kind::SEND;
use crate::driver::token::kind::SOCKET;
use crate::driver::token::kind::STAT;
use crate::driver::token::kind::SYNC;
use crate::driver::token::kind::TIMER;
use crate::driver::token::kind::WRITE;
use crate::driver::token::{SHUTDOWN, Token};
use fd::{AcceptedSlot, FdSlot};
use ffi::Handle;
use libc::EAGAIN;
use libc::ECANCELED;
use libc::EINTR;
use libc::ENOBUFS;
use provided::ProvidedLease;
use provided::raw::completion::CompletedBuffer;
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError;

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("invalid completion")
    }
}

impl StdError for DecodeError {}

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
    Data(ProvidedLease<'d>),
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

pub enum AcceptEvent<'d> {
    Accepted(AcceptedSlot<'d>),
    Failed,
}

pub enum SocketEvent {
    Created,
    Failed(Error),
}

pub enum ConnectEvent {
    Connected,
    Failed(Error),
}

pub struct Event<'d> {
    kind: EventKind<'d>,
    result: i32,
    operation: u8,
}

pub enum EventKind<'d> {
    Accept(Token, bool, AcceptEvent<'d>),
    Recv(Token, bool, RecvEvent<'d>),
    Send(Token, SendEvent),
    Timer(Token),
    Socket(Token, SocketEvent),
    Connect(Token, ConnectEvent),
    Write(Token, WriteEvent),
    Sync(Token, SyncEvent),
    Open(Token, OpenEvent),
    Read(Token, ReadEvent),
    Stat(Token, StatEvent),
    Shutdown,
}

pub enum EventRef<'a, 'd> {
    Accept(Token, bool, &'a AcceptEvent<'d>),
    Recv(Token, bool, &'a RecvEvent<'d>),
    Send(Token, &'a SendEvent),
    Timer(Token),
    Socket(Token, &'a SocketEvent),
    Connect(Token, &'a ConnectEvent),
    Write(Token, &'a WriteEvent),
    Sync(Token, &'a SyncEvent),
    Open(Token, &'a OpenEvent),
    Read(Token, &'a ReadEvent),
    Stat(Token, &'a StatEvent),
    Shutdown,
}

impl<'d> Event<'d> {
    pub(crate) fn from_cqe(
        cqe: Cqe,
        reference: DriverRef<'d>,
        provided: impl FnOnce(u32, u16) -> Option<CompletedBuffer>,
    ) -> Result<Self, DecodeError> {
        let result = cqe.result;
        let operation = cqe.kind();
        let mut provided = cqe
            .has_buffer()
            .then(|| provided(result.max(0) as u32, cqe.bid_raw()))
            .flatten()
            .map(|completed| ProvidedLease::from_completion(reference, completed));
        let token = cqe.token;
        let kind = if token == SHUTDOWN {
            EventKind::Shutdown
        } else {
            match operation {
                ACCEPT => {
                    let event = if result >= 0 {
                        AcceptEvent::Accepted(AcceptedSlot::from_completion(
                            FdSlot::new(result as u32),
                            reference,
                        ))
                    } else {
                        AcceptEvent::Failed
                    };
                    EventKind::Accept(token, cqe.more(), event)
                }
                RECV => {
                    let event = if result > 0 {
                        if !cqe.has_buffer() {
                            debug_assert!(false, "RECV data cqe without buffer flag");
                            return Err(DecodeError);
                        }
                        let lease = provided.take().ok_or(DecodeError)?;
                        debug_assert_eq!(lease.as_slice().len(), result as usize);
                        RecvEvent::Data(lease)
                    } else if result == 0 {
                        RecvEvent::Eof
                    } else {
                        RecvEvent::from_errno(result)
                    };
                    EventKind::Recv(token, cqe.more(), event)
                }
                RECV_DISCARD => {
                    let event = if result > 0 {
                        RecvEvent::Discarded { len: result as u32 }
                    } else if result == 0 {
                        RecvEvent::Eof
                    } else {
                        RecvEvent::from_errno(result)
                    };
                    EventKind::Recv(token, cqe.more(), event)
                }
                SEND => EventKind::Send(
                    token,
                    if result >= 0 {
                        SendEvent::Sent(result as u32)
                    } else {
                        SendEvent::Failed(-result)
                    },
                ),
                WRITE => EventKind::Write(
                    token,
                    if result >= 0 {
                        WriteEvent::Wrote(result as u32)
                    } else {
                        WriteEvent::Failed(-result)
                    },
                ),
                SYNC => EventKind::Sync(
                    token,
                    if result >= 0 {
                        SyncEvent::Synced
                    } else {
                        SyncEvent::Failed(-result)
                    },
                ),
                OPEN => EventKind::Open(
                    token,
                    if result >= 0 {
                        OpenEvent::Opened(Handle::take(result).into_owned())
                    } else {
                        OpenEvent::Failed(-result)
                    },
                ),
                READ => EventKind::Read(token, ReadEvent::from_result(result)),
                STAT => EventKind::Stat(
                    token,
                    if result >= 0 {
                        StatEvent::Done
                    } else {
                        StatEvent::Failed(-result)
                    },
                ),
                TIMER => EventKind::Timer(token),
                SOCKET => EventKind::Socket(
                    token,
                    if result >= 0 {
                        SocketEvent::Created
                    } else {
                        SocketEvent::Failed(Error::from_raw_os_error(-result))
                    },
                ),
                CONNECT => EventKind::Connect(
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
        Ok(Self {
            kind,
            result,
            operation,
        })
    }

    pub fn into_kind(self) -> EventKind<'d> {
        self.kind
    }

    pub fn as_ref(&self) -> EventRef<'_, 'd> {
        match &self.kind {
            EventKind::Accept(t, more, e) => EventRef::Accept(*t, *more, e),
            EventKind::Recv(t, more, e) => EventRef::Recv(*t, *more, e),
            EventKind::Send(t, e) => EventRef::Send(*t, e),
            EventKind::Timer(t) => EventRef::Timer(*t),
            EventKind::Socket(t, e) => EventRef::Socket(*t, e),
            EventKind::Connect(t, e) => EventRef::Connect(*t, e),
            EventKind::Write(t, e) => EventRef::Write(*t, e),
            EventKind::Sync(t, e) => EventRef::Sync(*t, e),
            EventKind::Open(t, e) => EventRef::Open(*t, e),
            EventKind::Read(t, e) => EventRef::Read(*t, e),
            EventKind::Stat(t, e) => EventRef::Stat(*t, e),
            EventKind::Shutdown => EventRef::Shutdown,
        }
    }

    pub const fn result(&self) -> i32 {
        self.result
    }

    pub const fn operation(&self) -> u8 {
        self.operation
    }

    pub const fn is_shutdown(&self) -> bool {
        matches!(self.kind, EventKind::Shutdown)
    }

    pub const fn token(&self) -> Option<Token> {
        match &self.kind {
            EventKind::Accept(token, ..)
            | EventKind::Recv(token, ..)
            | EventKind::Send(token, _)
            | EventKind::Timer(token)
            | EventKind::Socket(token, _)
            | EventKind::Connect(token, _)
            | EventKind::Write(token, _)
            | EventKind::Sync(token, _)
            | EventKind::Open(token, _)
            | EventKind::Read(token, _)
            | EventKind::Stat(token, _) => Some(*token),
            EventKind::Shutdown => None,
        }
    }

    pub fn route(&self) -> u8 {
        match &self.kind {
            EventKind::Accept(t, ..) => t.route(),
            EventKind::Recv(t, ..) => t.route(),
            EventKind::Send(t, _) => t.route(),
            EventKind::Timer(t) => t.route(),
            EventKind::Socket(t, _) => t.route(),
            EventKind::Connect(t, _) => t.route(),
            EventKind::Write(t, _) => t.route(),
            EventKind::Sync(t, _) => t.route(),
            EventKind::Open(t, _) => t.route(),
            EventKind::Read(t, _) => t.route(),
            EventKind::Stat(t, _) => t.route(),
            EventKind::Shutdown => SHUTDOWN.route(),
        }
    }
}
