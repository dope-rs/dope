use std::{mem, num::NonZero, time};

use dope_sys::io::socket::option::{Kind, Value, tuning};

use crate::{
    Engine, driver,
    driver::{life::native::Apply, retained::flight, route, route::kind},
    io::{fd::handles, socket::pending},
};

mod kinds;

pub(super) use kinds::Mask;
pub(crate) use kinds::NonEmpty;

/// Maximum number of distinct options represented by a stream-option set.
pub(crate) const MAX_STREAM_OPTIONS: usize = 7;

/// An explicit binary socket-option command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Switch {
    Off,
    On,
}

/// A socket-buffer request representable by the kernel ABI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Buffer(Value);

/// A positive, whole-second keep-alive period representable by the kernel ABI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Period(NonZero<u32>);

/// A positive keep-alive retry count representable by the kernel ABI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Retries(NonZero<u32>);

impl Switch {
    pub(crate) const fn value(self) -> Value {
        match self {
            Self::Off => Value::ZERO,
            Self::On => Value::from_enabled(true),
        }
    }
}

impl Buffer {
    const fn value(self) -> Value {
        self.0
    }
}

impl Period {
    const fn value(self) -> Value {
        Value::new(self.0.get() as i32)
    }
}

impl Retries {
    const fn value(self) -> Value {
        Value::new(self.0.get() as i32)
    }
}

/// Validated, allocation-free stream options owning every scalar retained by
/// an asynchronous kernel submission.
#[derive(Clone, Copy)]
pub struct Set {
    recv_buffer: Value,
    send_buffer: Value,
    keep_alive_idle: Value,
    keep_alive_interval: Value,
    keep_alive_retries: Value,
    present: Mask,
    enabled: Mask,
}

/// Invalid allocation-free socket-option configuration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Error {
    Buffer {
        actual: usize,
        maximum: usize,
    },
    Period {
        actual: time::Duration,
        maximum: time::Duration,
    },
    Retries {
        actual: u32,
        maximum: u32,
    },
}

impl Default for Set {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// Result of submitting a strongly ordered socket-tuning plan.
pub enum Tuning<'d> {
    /// All options were applied synchronously.
    Applied(handles::Descriptor<'d>),
    /// The descriptor and its exact completion/cancellation authority remain
    /// indivisible until the operation resolves.
    Pending(pending::Tuning<'d>),
}

impl Set {
    /// Admits already-validated, structurally distinct stream options.
    pub fn new(
        recv: Option<Buffer>,
        send: Option<Buffer>,
        no_delay: Option<Switch>,
        alive: Option<Switch>,
        idle: Option<Period>,
        interval: Option<Period>,
        retries: Option<Retries>,
    ) -> Self {
        let mut set = Self::EMPTY;
        if let Some(value) = recv {
            set.set(Kind::RecvBuffer, value.value());
        }
        if let Some(value) = send {
            set.set(Kind::SendBuffer, value.value());
        }
        if let Some(value) = no_delay {
            set.set(Kind::NoDelay, value.value());
        }
        if let Some(value) = alive {
            set.set(Kind::KeepAlive, value.value());
        }
        if let Some(value) = idle {
            set.set(Kind::KeepAliveIdle, value.value());
        }
        if let Some(value) = interval {
            set.set(Kind::KeepAliveInterval, value.value());
        }
        if let Some(value) = retries {
            set.set(Kind::KeepAliveRetries, value.value());
        }
        set
    }

    pub const EMPTY: Self = Self {
        recv_buffer: Value::ZERO,
        send_buffer: Value::ZERO,
        keep_alive_idle: Value::ZERO,
        keep_alive_interval: Value::ZERO,
        keep_alive_retries: Value::ZERO,
        present: Mask::EMPTY,
        enabled: Mask::EMPTY,
    };

    pub(crate) fn non_empty(&self) -> Option<NonEmpty<'_>> {
        NonEmpty::new(self, self.present)
    }

    fn value(&self, kind: Kind) -> Value {
        match kind {
            Kind::NoDelay | Kind::KeepAlive => Value::from_enabled(self.enabled.contains(kind)),
            Kind::RecvBuffer => self.recv_buffer,
            Kind::SendBuffer => self.send_buffer,
            Kind::KeepAliveIdle => self.keep_alive_idle,
            Kind::KeepAliveInterval => self.keep_alive_interval,
            Kind::KeepAliveRetries => self.keep_alive_retries,
        }
    }

    fn set(&mut self, kind: Kind, value: Value) {
        self.present.insert(kind);
        match kind {
            Kind::NoDelay | Kind::KeepAlive if !value.is_zero() => self.enabled.insert(kind),
            Kind::NoDelay | Kind::KeepAlive => {}
            Kind::RecvBuffer => self.recv_buffer = value,
            Kind::SendBuffer => self.send_buffer = value,
            Kind::KeepAliveIdle => self.keep_alive_idle = value,
            Kind::KeepAliveInterval => self.keep_alive_interval = value,
            Kind::KeepAliveRetries => self.keep_alive_retries = value,
        }
    }
}

impl TryFrom<usize> for Buffer {
    type Error = Error;

    fn try_from(actual: usize) -> Result<Self, Self::Error> {
        let maximum = i32::MAX as usize;
        if actual <= maximum {
            Ok(Self(Value::new(actual as i32)))
        } else {
            Err(Error::Buffer { actual, maximum })
        }
    }
}

impl TryFrom<time::Duration> for Period {
    type Error = Error;

    fn try_from(actual: time::Duration) -> Result<Self, Self::Error> {
        let maximum = time::Duration::from_secs(i32::MAX as u64);
        if actual.is_zero() || actual > maximum {
            return Err(Error::Period { actual, maximum });
        }
        let seconds = actual.as_secs() + u64::from(actual.subsec_nanos() != 0);
        Ok(Self(NonZero::<u32>::MIN.saturating_add(seconds as u32 - 1)))
    }
}

impl TryFrom<u32> for Retries {
    type Error = Error;

    fn try_from(actual: u32) -> Result<Self, Self::Error> {
        let maximum = i32::MAX as u32;
        if actual == 0 || actual > maximum {
            return Err(Error::Retries { actual, maximum });
        }
        Ok(Self(NonZero::<u32>::MIN.saturating_add(actual - 1)))
    }
}

const _: () = {
    assert!(mem::size_of::<Switch>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Switch>() == mem::align_of::<bool>());
    assert!(mem::size_of::<Buffer>() == mem::size_of::<Value>());
    assert!(mem::size_of::<Period>() == mem::size_of::<Value>());
    assert!(mem::size_of::<Retries>() == mem::size_of::<Value>());
    assert!(mem::size_of::<Option<Period>>() == mem::size_of::<Period>());
    assert!(mem::size_of::<Option<Retries>>() == mem::size_of::<Retries>());
    assert!(mem::size_of::<Set>() == 24);
};

impl<'d> Tuning<'d> {
    pub fn try_new<Tag: route::Tag + route::Select<kind::Tuning>>(
        driver: &mut driver::Context<'_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        target: route::Target<'d, Tag>,
        fd: handles::Descriptor<'d>,
        opts: Set,
    ) -> Result<Self, pending::Rejection<'d>> {
        use tuning::device::{Owner as _, Source};
        let owner = Apply::new(driver.native(), slots, target, fd, opts);
        let request = <crate::Backend as Source>::Socket::new(owner);
        // SAFETY: the native borrow and affine descriptor share this driver's
        // slot table; the owner retains every option scalar until completion.
        unsafe { tuning::raw::Apply::<Engine, _, { MAX_STREAM_OPTIONS }>::try_apply(request) }
    }
}
