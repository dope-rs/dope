use std::mem;

use dope_sys::{
    io::reactor::complete::{
        self, Decode, Flags as _, Outcome as _, Record as _, Value as _,
        flags::{self, Meta as _},
    },
    probe::{self, Check, Failure},
};

use crate::{Observed, reactor::cq};

pub struct Outcome<T, P> {
    token: T,
    outcome: cq::Outcome,
    meta: cq::Meta,
    probe: P,
}

impl<T: complete::raw::Token, P: Copy> probe::Outcome for Outcome<T, P> {
    type Parts = complete::Parts<T, cq::Outcome, cq::Flags>;
    type Value = cq::Value;
    type Meta = cq::Meta;
    type Probe = P;
    type Observation = Observed<T>;

    fn new(parts: Self::Parts, probe: P) -> Result<Self, Failure<P, Observed<T>>> {
        let meta = match parts.flags.decode() {
            Ok(meta) => meta,
            Err(invalid) => {
                let observed = Observed::new(parts.token, parts.outcome, invalid.into());
                return Err(Failure::new(probe, Check::InvalidFlags(observed)));
            }
        };
        Ok(Self {
            token: parts.token,
            outcome: parts.outcome,
            meta,
            probe,
        })
    }

    fn success(&self) -> Result<cq::Value, Failure<P, Observed<T>>> {
        match self.outcome.decode() {
            Decode::Value(value) => Ok(value),
            Decode::Error(_) => Err(Failure::new(self.probe, Check::Rejected(self.observed()))),
            Decode::Invalid(_) => Err(self.unexpected()),
        }
    }

    fn zero(self) -> Result<(), Failure<P, Observed<T>>> {
        let result = self.success()?;
        if result.is_zero() {
            Ok(())
        } else {
            Err(self.unexpected())
        }
    }

    fn canceled(self) -> Result<(), Failure<P, Observed<T>>> {
        if self.outcome == cq::Outcome::from_error(dope_sys::Errno::CANCELED)
            && matches!(self.meta.flow(), flags::Flow::Last)
        {
            Ok(())
        } else {
            Err(self.unexpected())
        }
    }

    fn cancel(self) -> Result<(), Failure<P, Observed<T>>> {
        if self.outcome == cq::Outcome::from_error(dope_sys::Errno::CANCELED)
            && self.meta.buffer().is_none()
        {
            Ok(())
        } else {
            Err(self.nonterminal())
        }
    }

    fn probe(&self) -> P {
        self.probe
    }

    fn meta(&self) -> cq::Meta {
        self.meta
    }

    fn unexpected(&self) -> Failure<P, Observed<T>> {
        Failure::new(self.probe, Check::Unexpected(self.observed()))
    }

    fn nonterminal(&self) -> Failure<P, Observed<T>> {
        Failure::new(self.probe, Check::Nonterminal(self.observed()))
    }
}

impl<T: Copy, P: Copy> Outcome<T, P> {
    fn observed(&self) -> Observed<T> {
        Observed::new(self.token, self.outcome, self.meta.into())
    }
}

const _: () = {
    assert!(mem::size_of::<Outcome<u64, u8>>() == 3 * mem::size_of::<u64>());
    assert!(mem::align_of::<Outcome<u64, u8>>() == mem::align_of::<u64>());
};
