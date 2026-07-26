pub mod datagram;
pub mod fd;
pub(crate) mod ffi;
pub mod file;
pub mod pipe;
pub mod provided;
pub mod socket;

use std::error::Error as StdError;
use std::fmt;
use std::io::{self, Error};
use std::os::fd::OwnedFd;

use crate::driver::DriverRef;
use crate::driver::token;
use crate::driver::token::{SHUTDOWN, Token, kind};
use fd::{AcceptedSlot, FdSlot};
use ffi::Handle;
use provided::ProvidedLease;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid completion")
    }
}

impl StdError for DecodeError {}

pub(crate) const BUFFER: u32 = 1 << 0;
pub(crate) const MORE: u32 = 1 << 1;
pub(crate) const BUFFER_SHIFT: u32 = 16;

#[derive(Clone, Copy)]
pub(crate) struct Cqe {
    user_data: u64,
    result: i32,
    flags: u32,
}

impl Cqe {
    pub(crate) const fn new(user_data: u64, result: i32, flags: u32) -> Self {
        Self {
            user_data,
            result,
            flags,
        }
    }

    pub(crate) fn kind(self) -> u8 {
        (self.user_data >> token::KIND_SHIFT) as u8
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
    Failed(io::Error),
}

pub enum ConnectEvent {
    Connected,
    Failed(io::Error),
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

#[derive(Clone, Copy)]
enum DecodedRecv {
    Data { len: u32, bid: u16 },
    Discarded { len: u32 },
    Eof,
    Cancelled,
    Starved,
    Failed(i32),
}

impl DecodedRecv {
    fn from_errno(result: i32) -> Self {
        match -result {
            libc::ECANCELED => Self::Cancelled,
            libc::ENOBUFS | libc::EAGAIN | libc::EINTR => Self::Starved,
            errno => Self::Failed(errno),
        }
    }
}

#[derive(Clone, Copy)]
enum DecodedAccept {
    Accepted(FdSlot),
    Failed,
}

#[derive(Clone, Copy)]
enum DecodedEvent {
    Accept(Token, bool, DecodedAccept),
    Recv(Token, bool, DecodedRecv),
    Send(Token, SendEvent),
    Timer(Token),
    Socket(Token, i32),
    Connect(Token, i32),
    Write(Token, WriteEvent),
    Sync(Token, SyncEvent),
    Open(Token, i32),
    Read(Token, ReadEvent),
    Stat(Token, StatEvent),
    Shutdown,
}

impl DecodedEvent {
    fn decode(c: Cqe) -> Result<Self, DecodeError> {
        let token = Token::try_from_raw(c.user_data).ok_or(DecodeError)?;
        if token == SHUTDOWN {
            return Ok(Self::Shutdown);
        }
        match c.kind() {
            kind::ACCEPT => {
                let e = match c.result {
                    n if n >= 0 => DecodedAccept::Accepted(FdSlot::new(n as u32)),
                    _ => DecodedAccept::Failed,
                };
                Ok(Self::Accept(token, c.more(), e))
            }
            kind::RECV => {
                let e = match c.result {
                    n if n > 0 => {
                        if !c.has_buffer() {
                            debug_assert!(false, "RECV data cqe without buffer flag");
                            return Err(DecodeError);
                        }
                        DecodedRecv::Data {
                            len: n as u32,
                            bid: c.bid_raw(),
                        }
                    }
                    0 => DecodedRecv::Eof,
                    n => DecodedRecv::from_errno(n),
                };
                Ok(Self::Recv(token, c.more(), e))
            }
            kind::RECV_DISCARD => {
                let e = match c.result {
                    n if n > 0 => DecodedRecv::Discarded { len: n as u32 },
                    0 => DecodedRecv::Eof,
                    n => DecodedRecv::from_errno(n),
                };
                Ok(Self::Recv(token, c.more(), e))
            }
            kind::SEND => {
                let e = if c.result >= 0 {
                    SendEvent::Sent(c.result as u32)
                } else {
                    SendEvent::Failed(-c.result)
                };
                Ok(Self::Send(token, e))
            }
            kind::WRITE => {
                let e = if c.result >= 0 {
                    WriteEvent::Wrote(c.result as u32)
                } else {
                    WriteEvent::Failed(-c.result)
                };
                Ok(Self::Write(token, e))
            }
            kind::SYNC => {
                let e = if c.result >= 0 {
                    SyncEvent::Synced
                } else {
                    SyncEvent::Failed(-c.result)
                };
                Ok(Self::Sync(token, e))
            }
            kind::OPEN => Ok(Self::Open(token, c.result)),
            kind::READ => Ok(Self::Read(token, ReadEvent::from_result(c.result))),
            kind::STAT => {
                let e = if c.result >= 0 {
                    StatEvent::Done
                } else {
                    StatEvent::Failed(-c.result)
                };
                Ok(Self::Stat(token, e))
            }
            kind::TIMER => Ok(Self::Timer(token)),
            kind::SOCKET => Ok(Self::Socket(token, c.result)),
            kind::CONNECT => Ok(Self::Connect(token, c.result)),
            _ => Err(DecodeError),
        }
    }
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
        provided: impl FnOnce(u32, u16) -> ProvidedLease<'d>,
    ) -> Result<Self, DecodeError> {
        let result = cqe.result;
        let operation = cqe.kind();
        let mut provided = cqe
            .has_buffer()
            .then(|| provided(result.max(0) as u32, cqe.bid_raw()));
        let kind = match DecodedEvent::decode(cqe)? {
            DecodedEvent::Accept(token, more, event) => {
                let event = match event {
                    DecodedAccept::Accepted(slot) => {
                        AcceptEvent::Accepted(AcceptedSlot::from_completion(slot, reference))
                    }
                    DecodedAccept::Failed => AcceptEvent::Failed,
                };
                EventKind::Accept(token, more, event)
            }
            DecodedEvent::Recv(token, more, event) => {
                let event = match event {
                    DecodedRecv::Data { len, bid } => {
                        let lease = provided.take().ok_or(DecodeError)?;
                        debug_assert_eq!(lease.as_slice().len(), len as usize);
                        debug_assert_eq!(bid, cqe.bid_raw());
                        RecvEvent::Data(lease)
                    }
                    DecodedRecv::Discarded { len } => RecvEvent::Discarded { len },
                    DecodedRecv::Eof => RecvEvent::Eof,
                    DecodedRecv::Cancelled => RecvEvent::Cancelled,
                    DecodedRecv::Starved => RecvEvent::Starved,
                    DecodedRecv::Failed(errno) => RecvEvent::Failed(errno),
                };
                EventKind::Recv(token, more, event)
            }
            DecodedEvent::Send(token, event) => EventKind::Send(token, event),
            DecodedEvent::Timer(token) => EventKind::Timer(token),
            DecodedEvent::Socket(token, result) => EventKind::Socket(
                token,
                if result >= 0 {
                    SocketEvent::Created
                } else {
                    SocketEvent::Failed(Error::from_raw_os_error(-result))
                },
            ),
            DecodedEvent::Connect(token, result) => EventKind::Connect(
                token,
                if result >= 0 {
                    ConnectEvent::Connected
                } else {
                    ConnectEvent::Failed(Error::from_raw_os_error(-result))
                },
            ),
            DecodedEvent::Write(token, event) => EventKind::Write(token, event),
            DecodedEvent::Sync(token, event) => EventKind::Sync(token, event),
            DecodedEvent::Open(token, result) => EventKind::Open(
                token,
                if result >= 0 {
                    OpenEvent::Opened(Handle::take(result).into_owned())
                } else {
                    OpenEvent::Failed(-result)
                },
            ),
            DecodedEvent::Read(token, event) => EventKind::Read(token, event),
            DecodedEvent::Stat(token, event) => EventKind::Stat(token, event),
            DecodedEvent::Shutdown => EventKind::Shutdown,
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
