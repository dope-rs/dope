use std::io;

use crate::backend::socket::FdSlot;
use crate::backend::token;
use crate::backend::token::{Token, kind};

pub(super) const BUFFER: u32 = 1 << 0;
pub(super) const MORE: u32 = 1 << 1;
pub(super) const BUFFER_SHIFT: u32 = 16;

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

pub enum Event {
    Accept(Token, bool, AcceptEvent),
    Recv(Token, bool, RecvEvent),
    Send(Token, SendEvent),
    Timer(Token),
    Socket(Token, SocketEvent),
    Connect(Token, ConnectEvent),
    Write(Token, WriteEvent),
    Sync(Token, SyncEvent),
}

impl TryFrom<Cqe> for Event {
    type Error = ();

    fn try_from(c: Cqe) -> Result<Self, ()> {
        match c.kind() {
            kind::ACCEPT => {
                let e = match c.result {
                    n if n >= 0 => AcceptEvent::Accepted(FdSlot::new(n as u32)),
                    _ => AcceptEvent::Failed,
                };
                Ok(Self::Accept(Token::from_raw(c.user_data), c.more(), e))
            }
            kind::RECV => {
                let e = match c.result {
                    n if n > 0 => {
                        if !c.has_buffer() {
                            debug_assert!(false, "RECV data cqe without buffer flag");
                            return Err(());
                        }
                        RecvEvent::Data {
                            len: n as u32,
                            bid: c.bid_raw(),
                        }
                    }
                    0 => RecvEvent::Eof,
                    n => match -n {
                        libc::ECANCELED => RecvEvent::Cancelled,
                        libc::EAGAIN | libc::ENOBUFS | libc::EINTR => RecvEvent::Starved,
                        errno => RecvEvent::Failed(errno),
                    },
                };
                Ok(Self::Recv(Token::from_raw(c.user_data), c.more(), e))
            }
            kind::SEND => {
                let e = if c.result >= 0 {
                    SendEvent::Sent(c.result as u32)
                } else {
                    SendEvent::Failed(-c.result)
                };
                Ok(Self::Send(Token::from_raw(c.user_data), e))
            }
            kind::WRITE => {
                let e = if c.result >= 0 {
                    WriteEvent::Wrote(c.result as u32)
                } else {
                    WriteEvent::Failed(-c.result)
                };
                Ok(Self::Write(Token::from_raw(c.user_data), e))
            }
            kind::SYNC => {
                let e = if c.result >= 0 {
                    SyncEvent::Synced
                } else {
                    SyncEvent::Failed(-c.result)
                };
                Ok(Self::Sync(Token::from_raw(c.user_data), e))
            }
            kind::TIMER => Ok(Self::Timer(Token::from_raw(c.user_data))),
            kind::SOCKET => {
                let e = if c.result >= 0 {
                    SocketEvent::Created
                } else {
                    SocketEvent::Failed(io::Error::from_raw_os_error(-c.result))
                };
                Ok(Self::Socket(Token::from_raw(c.user_data), e))
            }
            kind::CONNECT => {
                let e = if c.result >= 0 {
                    ConnectEvent::Connected
                } else {
                    ConnectEvent::Failed(io::Error::from_raw_os_error(-c.result))
                };
                Ok(Self::Connect(Token::from_raw(c.user_data), e))
            }
            _ => Err(()),
        }
    }
}

impl Event {
    pub fn route(&self) -> u8 {
        match self {
            Self::Accept(t, ..) => t.route(),
            Self::Recv(t, ..) => t.route(),
            Self::Send(t, _) => t.route(),
            Self::Timer(t) => t.route(),
            Self::Socket(t, _) => t.route(),
            Self::Connect(t, _) => t.route(),
            Self::Write(t, _) => t.route(),
            Self::Sync(t, _) => t.route(),
        }
    }
}
