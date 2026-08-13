pub(crate) mod reactor;

pub mod affinity;
#[doc(hidden)]
pub mod wake;

use std::{convert, io, iter, num, os::fd, path, time};

use crate::{
    backend,
    driver::{self, flight, settings},
    io::{datagram, recv, socket::msg},
};

/// Operating-system entropy acquired by the selected backend.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Entropy([u64; 2]);

impl Entropy {
    pub fn acquire() -> io::Result<Self> {
        <backend::Backend as EntropySource>::acquire().map(Self)
    }

    pub const fn into_words(self) -> [u64; 2] {
        self.0
    }
}

#[repr(transparent)]
pub(crate) struct Timeout(libc::timespec);

impl TryFrom<time::Duration> for Timeout {
    type Error = io::Error;

    fn try_from(duration: time::Duration) -> Result<Self, Self::Error> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: timeout exceeds the platform timespec range",
            )
        };
        let seconds: libc::time_t = duration.as_secs().try_into().map_err(|_| invalid())?;
        let nanoseconds: libc::c_long = duration.subsec_nanos().into();
        Ok(Self(libc::timespec {
            tv_sec: seconds,
            tv_nsec: nanoseconds,
        }))
    }
}

impl Timeout {
    pub(crate) const fn seconds(&self) -> libc::time_t {
        self.0.tv_sec
    }

    pub(crate) const fn nanoseconds(&self) -> libc::c_long {
        self.0.tv_nsec
    }
}

const _: () = {
    assert!((libc::time_t::MAX as u128) <= i64::MAX as u128);
    assert!((libc::c_long::MAX as u128) <= i64::MAX as u128);
    assert!(std::mem::size_of::<Timeout>() == std::mem::size_of::<libc::timespec>());
};

pub(crate) trait Datagram {
    type Gso: GsoMode;

    fn project(buffer: &recv::Lease<'_>) -> datagram::Projection;
}

pub(crate) trait Buffer {
    type Token;

    fn release(&mut self, buffer: Self::Token);
}

pub(crate) trait Filesystem {
    fn open_directory(path: &path::Path) -> io::Result<fd::OwnedFd>;
}

pub(crate) trait Quiesce {
    fn all(&mut self, drain: flight::Drain<'_, '_>) -> io::Result<()>;
}

pub(crate) trait Available:
    Sized + Iterator<Item = u16> + DoubleEndedIterator + ExactSizeIterator + iter::FusedIterator
{
    fn current() -> io::Result<Self>;
}

pub(crate) trait Bound: Sized {
    fn bind(cpu: u16) -> io::Result<Self>;
    fn cpu(&self) -> u16;
}

pub(crate) trait Affinity {
    type Cpus: Available;
    type Binding: Bound;
}

pub(crate) trait Runtime: Sized {
    fn build(config: &settings::Config) -> io::Result<Self>;
    fn register_shutdown(&mut self, source: driver::Source<'_>) -> io::Result<()>;
}

pub(crate) trait GsoMode {
    type Capability;
    type Control;

    const LIMITS: Option<datagram::GsoLimits>;

    fn acquire() -> Option<Self::Capability>;
    fn limits(capability: &Self::Capability) -> datagram::GsoLimits;
    fn control(capability: Self::Capability, segment_size: num::NonZeroU16) -> Self::Control;
    fn release(control: Self::Control) -> (Self::Capability, num::NonZeroU16);
    fn attach<'a>(control: &'a mut Self::Control, message: &mut msg::Builder<'a>);
}

impl GsoMode for convert::Infallible {
    type Capability = convert::Infallible;
    type Control = convert::Infallible;

    const LIMITS: Option<datagram::GsoLimits> = None;

    fn acquire() -> Option<Self::Capability> {
        None
    }

    fn limits(capability: &Self::Capability) -> datagram::GsoLimits {
        match *capability {}
    }

    fn control(capability: Self::Capability, _: num::NonZeroU16) -> Self::Control {
        match capability {}
    }

    fn release(control: Self::Control) -> (Self::Capability, num::NonZeroU16) {
        match control {}
    }

    fn attach<'a>(control: &'a mut Self::Control, _: &mut msg::Builder<'a>) {
        match *control {}
    }
}

pub(crate) trait EntropySource {
    fn acquire() -> io::Result<[u64; 2]>;
}
