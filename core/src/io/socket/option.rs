use std::{io, time};

use crate::{
    backend,
    io::{fd::handles, socket::establishment},
};

pub const MAX_STREAM_OPTIONS: usize = 7;

#[derive(Clone, Copy)]
pub(crate) struct Common {
    level: libc::c_int,
    name: libc::c_int,
    value: libc::c_int,
}

impl Common {
    const fn new(level: libc::c_int, name: libc::c_int, value: libc::c_int) -> Self {
        Self { level, name, value }
    }

    pub(crate) const fn level(self) -> libc::c_int {
        self.level
    }

    pub(crate) const fn name(self) -> libc::c_int {
        self.name
    }

    pub(crate) const fn value(&self) -> &libc::c_int {
        &self.value
    }
}

/// Validated, allocation-free stream options owning every scalar retained by
/// an asynchronous kernel submission.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct StreamOptions {
    recv_buffer: libc::c_int,
    send_buffer: libc::c_int,
    keep_alive_idle: libc::c_int,
    keep_alive_interval: libc::c_int,
    keep_alive_retries: libc::c_int,
    present: u8,
    enabled: u8,
}

const _: () = assert!(std::mem::size_of::<StreamOptions>() == 24);

pub(crate) struct Iter {
    options: StreamOptions,
    remaining: u8,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// Result of submitting a strongly ordered socket-tuning plan.
#[doc(hidden)]
pub enum Tuning<'d> {
    /// All options were applied synchronously.
    Applied(handles::Descriptor<'d>),
    /// The descriptor and its exact completion/cancellation authority remain
    /// indivisible until the operation resolves.
    Pending(establishment::TuningPending<'d>),
}

impl StreamOptions {
    const NO_DELAY: u8 = 1 << 0;
    const KEEP_ALIVE: u8 = 1 << 1;
    const RECV_BUFFER: u8 = 1 << 2;
    const SEND_BUFFER: u8 = 1 << 3;
    const KEEP_ALIVE_IDLE: u8 = 1 << 4;
    const KEEP_ALIVE_INTERVAL: u8 = 1 << 5;
    const KEEP_ALIVE_RETRIES: u8 = 1 << 6;

    pub const EMPTY: Self = Self {
        recv_buffer: 0,
        send_buffer: 0,
        keep_alive_idle: 0,
        keep_alive_interval: 0,
        keep_alive_retries: 0,
        present: 0,
        enabled: 0,
    };

    pub(crate) const fn iter(self) -> Iter {
        Iter {
            options: self,
            remaining: self.present,
        }
    }

    pub const fn is_empty(self) -> bool {
        self.present == 0
    }

    fn insert(&mut self, option: Stream) -> io::Result<()> {
        use std::io::{Error, ErrorKind};

        match option {
            Stream::NoDelay(on) => {
                return self.set_enabled(Self::NO_DELAY, on);
            }
            Stream::KeepAlive(on) => {
                return self.set_enabled(Self::KEEP_ALIVE, on);
            }
            Stream::Buffer(size) => {
                let value = i32::try_from(size).map_err(|_| {
                    Error::new(ErrorKind::InvalidInput, "receive buffer size exceeds c_int")
                })?;
                self.claim(Self::RECV_BUFFER)?;
                self.recv_buffer = value;
            }
            Stream::SendBuffer(size) => {
                let value = i32::try_from(size).map_err(|_| {
                    Error::new(ErrorKind::InvalidInput, "send buffer size exceeds c_int")
                })?;
                self.claim(Self::SEND_BUFFER)?;
                self.send_buffer = value;
            }
            Stream::KeepAliveIdle(duration) => {
                let value = Stream::seconds_raw(duration).ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "keep-alive idle must fit positive c_int seconds",
                    )
                })?;
                self.claim(Self::KEEP_ALIVE_IDLE)?;
                self.keep_alive_idle = value;
            }
            Stream::KeepAliveInterval(duration) => {
                let value = Stream::seconds_raw(duration).ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "keep-alive interval must fit positive c_int seconds",
                    )
                })?;
                self.claim(Self::KEEP_ALIVE_INTERVAL)?;
                self.keep_alive_interval = value;
            }
            Stream::KeepAliveRetries(retries) => {
                let value = Stream::positive_raw(retries.into()).ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "keep-alive retries must fit a positive c_int",
                    )
                })?;
                self.claim(Self::KEEP_ALIVE_RETRIES)?;
                self.keep_alive_retries = value;
            }
        }
        Ok(())
    }

    fn set_enabled(&mut self, bit: u8, enabled: bool) -> io::Result<()> {
        self.claim(bit)?;
        if enabled {
            self.enabled |= bit;
        }
        Ok(())
    }

    fn claim(&mut self, bit: u8) -> io::Result<()> {
        if self.present & bit != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate stream socket option",
            ));
        }
        self.present |= bit;
        Ok(())
    }
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum Stream {
    KeepAlive(bool),
    Buffer(usize),
    SendBuffer(usize),
    NoDelay(bool),
    KeepAliveIdle(time::Duration),
    KeepAliveInterval(time::Duration),
    KeepAliveRetries(u32),
}

impl Stream {
    fn positive_raw(raw: u128) -> Option<libc::c_int> {
        if raw == 0 {
            None
        } else {
            i32::try_from(raw).ok()
        }
    }

    fn seconds_raw(duration: time::Duration) -> Option<libc::c_int> {
        let seconds = duration
            .as_secs()
            .checked_add(u64::from(duration.subsec_nanos() != 0))?;
        if seconds == 0 {
            None
        } else {
            i32::try_from(seconds).ok()
        }
    }
}

impl Iterator for Iter {
    type Item = Common;

    fn next(&mut self) -> Option<Self::Item> {
        let bit = self.remaining & self.remaining.wrapping_neg();
        self.remaining ^= bit;
        match bit {
            0 => None,
            StreamOptions::NO_DELAY => Some(Common::new(
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                i32::from(self.options.enabled & bit != 0),
            )),
            StreamOptions::KEEP_ALIVE => Some(Common::new(
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                i32::from(self.options.enabled & bit != 0),
            )),
            StreamOptions::RECV_BUFFER => Some(Common::new(
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                self.options.recv_buffer,
            )),
            StreamOptions::SEND_BUFFER => Some(Common::new(
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                self.options.send_buffer,
            )),
            StreamOptions::KEEP_ALIVE_IDLE => Some(Common::new(
                libc::IPPROTO_TCP,
                <backend::Backend as backend::Socket>::KEEP_ALIVE_IDLE,
                self.options.keep_alive_idle,
            )),
            StreamOptions::KEEP_ALIVE_INTERVAL => Some(Common::new(
                libc::IPPROTO_TCP,
                <backend::Backend as backend::Socket>::KEEP_ALIVE_INTERVAL,
                self.options.keep_alive_interval,
            )),
            StreamOptions::KEEP_ALIVE_RETRIES => Some(Common::new(
                libc::IPPROTO_TCP,
                <backend::Backend as backend::Socket>::KEEP_ALIVE_RETRIES,
                self.options.keep_alive_retries,
            )),
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining.count_ones() as usize;
        (len, Some(len))
    }
}

impl ExactSizeIterator for Iter {}

impl<const N: usize> TryFrom<[Option<Stream>; N]> for StreamOptions {
    type Error = io::Error;

    fn try_from(options: [Option<Stream>; N]) -> io::Result<Self> {
        if N > MAX_STREAM_OPTIONS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many stream socket options",
            ));
        }
        let mut resolved = StreamOptions::EMPTY;
        for option in options.into_iter().flatten() {
            resolved.insert(option)?;
        }
        Ok(resolved)
    }
}
