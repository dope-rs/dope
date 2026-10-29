use dope_sys::{
    io::reactor::complete::{self, Decode, Value as _},
    kernel::Returned as _,
};
use o3::base::num::bounded;

use crate::{error::Returned, reactor::cq::Value};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Outcome(i32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Invalid(i32);

impl Outcome {
    pub(crate) const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub(crate) const fn from_error(code: dope_sys::Errno) -> Self {
        Self(-code.code().get())
    }
}

impl complete::Outcome for Outcome {
    type Value = Value;
    type Invalid = Invalid;
    type Returned = Returned;

    fn decode(self) -> Decode<Value, Invalid> {
        use dope_sys::kernel::raw::Code;

        let raw = self.0;
        if let Some(value) = bounded::U32::new(raw as u32) {
            return Decode::Value(Value(value));
        }
        if raw == i32::MIN {
            Decode::Invalid(Invalid(raw))
        } else {
            // SAFETY: this branch retained -i32::MAX..=-1 from the native CQE.
            Decode::Error(unsafe { Code::new(-raw) }.into())
        }
    }

    fn is_zero(self) -> bool {
        self.0 == 0
    }

    fn is_error(self) -> bool {
        self.0 < 0 && self.0 != i32::MIN
    }

    fn canceled(self) -> Result<(), Returned> {
        if self.0 == -libc::ENOENT || self.0 == -libc::ECANCELED || self.0 == -libc::EALREADY {
            Ok(())
        } else {
            Err(Returned::from(self))
        }
    }

    fn bytes(self) -> Result<Result<Value, dope_sys::Errno>, Returned> {
        match self.decode() {
            Decode::Value(value) => Ok(Ok(value)),
            Decode::Error(code) => Ok(Err(code)),
            Decode::Invalid(invalid) => Err(Returned::invalid(invalid)),
        }
    }

    fn unit(self) -> Result<Result<(), dope_sys::Errno>, Returned> {
        match self.decode() {
            Decode::Value(value) if value.is_zero() => Ok(Ok(())),
            Decode::Value(value) => Err(Returned::value(value)),
            Decode::Error(code) => Ok(Err(code)),
            Decode::Invalid(invalid) => Err(Returned::invalid(invalid)),
        }
    }

    fn shutdown(self) -> Result<(), Returned> {
        if self.0 >= 0
            && (self.0 & !(i32::from(libc::POLLHUP) | i32::from(libc::POLLRDHUP)))
                == i32::from(libc::POLLIN)
        {
            Ok(())
        } else {
            Err(Returned::from(self))
        }
    }

    fn stopped(self) -> Result<(), Returned> {
        if self.0 == -libc::ECANCELED {
            return Ok(());
        }
        self.shutdown()
    }

    fn aborted(self) -> Result<(), Returned> {
        if self.0 == -libc::ECANCELED {
            Ok(())
        } else {
            Err(Returned::from(self))
        }
    }
}

impl Invalid {
    pub(crate) const fn into_outcome(self) -> Outcome {
        Outcome(self.0)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Value>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Invalid>() == std::mem::size_of::<i32>());
    assert!(std::mem::size_of::<Outcome>() == std::mem::size_of::<i32>());
    assert!(std::mem::size_of::<Decode<Value, Invalid>>() == 2 * std::mem::size_of::<u32>());
};
