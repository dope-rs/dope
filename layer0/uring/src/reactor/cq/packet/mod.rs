use dope_sys::ring;
mod decoded;
pub(crate) use decoded::Decoded;
use dope_sys::{
    io::reactor::complete::{
        self, Flags as _, Outcome as _, Parts,
        flags::{Flow, Meta as _},
        packet::Invalid,
    },
    probe::Stopped,
    table::token::host,
};

use crate::reactor::cq::{Flags, Outcome};

pub struct Packet<T, B> {
    parts: Parts<T, Outcome, Flags>,
    buffer: Option<B>,
}

impl<T: Copy, B> ring::Packet for Packet<T, B> {
    type Native = crate::Uring;
}

impl<T: Copy, B> complete::Packet for Packet<T, B> {
    type Parts = Parts<T, Outcome, Flags>;
    type Buffer = B;

    fn new(parts: Self::Parts, buffer: Option<B>) -> Self {
        Self { parts, buffer }
    }

    fn try_new<E>(
        parts: Self::Parts,
        buffer: Result<Option<B>, E>,
    ) -> Result<Self, Stopped<Self, Invalid<E>>> {
        let buffer = buffer.map_err(|cause| Stopped {
            state: Self::new(parts, None),
            cause: Invalid::Buffer(cause),
        })?;
        let packet = Self::new(parts, buffer);
        if !parts.flags.is_valid() {
            return Err(Stopped {
                state: packet,
                cause: Invalid::Flags,
            });
        }
        Ok(packet)
    }

    fn parts(&self) -> Self::Parts {
        self.parts
    }

    fn buffer(&self) -> &Option<B> {
        &self.buffer
    }

    fn buffer_mut(&mut self) -> &mut Option<B> {
        &mut self.buffer
    }

    fn into_parts(self) -> (Self::Parts, Option<B>) {
        (self.parts, self.buffer)
    }

    fn is_plain(&self) -> bool {
        self.buffer.is_none() && self.parts.flags == Flags::new(0)
    }

    fn flow(&self) -> Flow {
        self.parts.flags.meta().flow()
    }

    fn try_retire(&self, echo: impl host::Echo) -> Result<(), &Self> {
        if self.is_plain() && self.parts.outcome.is_zero() {
            echo.complete();
            Ok(())
        } else {
            echo.retry();
            Err(self)
        }
    }
}
