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

use crate::driver::token;
use crate::driver::token::{Token, kind};
use fd::FdSlot;

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
pub struct Cqe {
    pub user_data: u64,
    pub result: i32,
    pub flags: u32,
}

impl Cqe {
    pub const ZERO: Self = Self {
        user_data: 0,
        result: 0,
        flags: 0,
    };

    pub fn route(self) -> u8 {
        (self.user_data >> token::ROUTE_SHIFT) as u8
    }

    pub fn kind(self) -> u8 {
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

#[derive(Clone, Copy)]
pub enum RecvEvent {
    Data { len: u32, bid: u16 },
    Discarded { len: u32 },
    Eof,
    Cancelled,
    Starved,
    Failed(i32),
}

impl RecvEvent {
    fn from_errno(result: i32) -> Self {
        match -result {
            libc::ECANCELED => Self::Cancelled,
            libc::ENOBUFS => Self::Starved,
            libc::EAGAIN | libc::EINTR => Self::Starved,
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

#[derive(Clone, Copy)]
pub enum OpenEvent {
    Opened(i32),
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
pub enum SpliceEvent {
    Moved(u32),
    Eof,
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum StatEvent {
    Done,
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum AcceptEvent {
    Accepted(FdSlot),
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

pub struct Event(EventKind);

pub enum EventKind {
    Accept(Token, bool, AcceptEvent),
    Recv(Token, bool, RecvEvent),
    Send(Token, SendEvent),
    Timer(Token),
    Socket(Token, SocketEvent),
    Connect(Token, ConnectEvent),
    Write(Token, WriteEvent),
    Sync(Token, SyncEvent),
    Open(Token, OpenEvent),
    Read(Token, ReadEvent),
    ReadBlock(Token, ReadEvent),
    Splice(Token, SpliceEvent),
    Stat(Token, StatEvent),
}

impl Event {
    pub fn decode(c: Cqe) -> Result<Self, DecodeError> {
        let token = Token::try_from_raw(c.user_data).ok_or(DecodeError)?;
        match c.kind() {
            kind::ACCEPT => {
                let e = match c.result {
                    n if n >= 0 => AcceptEvent::Accepted(FdSlot::new(n as u32)),
                    _ => AcceptEvent::Failed,
                };
                Ok(Self(EventKind::Accept(token, c.more(), e)))
            }
            kind::RECV => {
                let e = match c.result {
                    n if n > 0 => {
                        if !c.has_buffer() {
                            debug_assert!(false, "RECV data cqe without buffer flag");
                            return Err(DecodeError);
                        }
                        RecvEvent::Data {
                            len: n as u32,
                            bid: c.bid_raw(),
                        }
                    }
                    0 => RecvEvent::Eof,
                    n => RecvEvent::from_errno(n),
                };
                Ok(Self(EventKind::Recv(token, c.more(), e)))
            }
            kind::RECV_DISCARD => {
                let e = match c.result {
                    n if n > 0 => RecvEvent::Discarded { len: n as u32 },
                    0 => RecvEvent::Eof,
                    n => RecvEvent::from_errno(n),
                };
                Ok(Self(EventKind::Recv(token, c.more(), e)))
            }
            kind::SEND => {
                let e = if c.result >= 0 {
                    SendEvent::Sent(c.result as u32)
                } else {
                    SendEvent::Failed(-c.result)
                };
                Ok(Self(EventKind::Send(token, e)))
            }
            kind::WRITE => {
                let e = if c.result >= 0 {
                    WriteEvent::Wrote(c.result as u32)
                } else {
                    WriteEvent::Failed(-c.result)
                };
                Ok(Self(EventKind::Write(token, e)))
            }
            kind::SYNC => {
                let e = if c.result >= 0 {
                    SyncEvent::Synced
                } else {
                    SyncEvent::Failed(-c.result)
                };
                Ok(Self(EventKind::Sync(token, e)))
            }
            kind::OPEN => {
                let e = if c.result >= 0 {
                    OpenEvent::Opened(c.result)
                } else {
                    OpenEvent::Failed(-c.result)
                };
                Ok(Self(EventKind::Open(token, e)))
            }
            kind::READ => Ok(Self(EventKind::Read(
                token,
                ReadEvent::from_result(c.result),
            ))),
            kind::READ_BLOCK => Ok(Self(EventKind::ReadBlock(
                token,
                ReadEvent::from_result(c.result),
            ))),
            kind::SPLICE => {
                let e = match c.result {
                    n if n > 0 => SpliceEvent::Moved(n as u32),
                    0 => SpliceEvent::Eof,
                    n => SpliceEvent::Failed(-n),
                };
                Ok(Self(EventKind::Splice(token, e)))
            }
            kind::STAT => {
                let e = if c.result >= 0 {
                    StatEvent::Done
                } else {
                    StatEvent::Failed(-c.result)
                };
                Ok(Self(EventKind::Stat(token, e)))
            }
            kind::TIMER => Ok(Self(EventKind::Timer(token))),
            kind::SOCKET => {
                let e = if c.result >= 0 {
                    SocketEvent::Created
                } else {
                    SocketEvent::Failed(Error::from_raw_os_error(-c.result))
                };
                Ok(Self(EventKind::Socket(token, e)))
            }
            kind::CONNECT => {
                let e = if c.result >= 0 {
                    ConnectEvent::Connected
                } else {
                    ConnectEvent::Failed(Error::from_raw_os_error(-c.result))
                };
                Ok(Self(EventKind::Connect(token, e)))
            }
            _ => Err(DecodeError),
        }
    }
}

impl TryFrom<Cqe> for EventKind {
    type Error = DecodeError;

    fn try_from(cqe: Cqe) -> Result<Self, DecodeError> {
        Event::decode(cqe).map(Event::into_kind)
    }
}

pub enum EventRef<'a> {
    Accept(Token, bool, &'a AcceptEvent),
    Recv(Token, bool, &'a RecvEvent),
    Send(Token, &'a SendEvent),
    Timer(Token),
    Socket(Token, &'a SocketEvent),
    Connect(Token, &'a ConnectEvent),
    Write(Token, &'a WriteEvent),
    Sync(Token, &'a SyncEvent),
    Open(Token, &'a OpenEvent),
    Read(Token, &'a ReadEvent),
    Splice(Token, &'a SpliceEvent),
    Stat(Token, &'a StatEvent),
}

impl Event {
    /// # Safety
    /// `cqe` was produced by the paired driver and has not been decoded before.
    pub unsafe fn from_cqe(cqe: Cqe) -> Result<Self, DecodeError> {
        Self::decode(cqe)
    }

    pub fn into_kind(self) -> EventKind {
        self.0
    }

    pub fn as_ref(&self) -> EventRef<'_> {
        match &self.0 {
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
            EventKind::ReadBlock(t, e) => EventRef::Read(*t, e),
            EventKind::Splice(t, e) => EventRef::Splice(*t, e),
            EventKind::Stat(t, e) => EventRef::Stat(*t, e),
        }
    }

    pub fn route(&self) -> u8 {
        match &self.0 {
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
            EventKind::ReadBlock(t, _) => t.route(),
            EventKind::Splice(t, _) => t.route(),
            EventKind::Stat(t, _) => t.route(),
        }
    }
}

#[cfg(test)]
mod tests;
