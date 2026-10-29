use std::{marker, num};

pub(crate) use close::Close;
use dope_sys::{
    io::reactor::complete,
    table::token::{self, host},
};
pub(crate) use frame::Frame;
use o3::base::num::bounded;
use token::{Close as _, Tuning as _, frame::Decode as _};
pub(crate) use tuning::Tuning;

mod close;
mod frame;
mod tuning;

const SHIFT: u32 = 56;
const SLOT: u64 = (1 << 24) - 1;
const TUNING: u8 = 13;
const PREP: u8 = 16;
const MIN: u8 = 251;
const MAX: u8 = 254;
const PROOF: u64 = 3_u64 << SHIFT;
const PAYLOAD: u64 = (1_u64 << SHIFT) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Encoded<C>(u64, marker::PhantomData<fn() -> C>);

// SAFETY: the transparent u64 admits every bit pattern, has no padding,
// and its zero-sized context marker does not change its eight-byte layout.
unsafe impl<C: host::Context> complete::raw::Token for Encoded<C> {}

impl<C: host::Context> token::Codec for Encoded<C> {
    type Context = C;
    type Close<'d> = Close<'d, C>;
    type Tuning = Tuning<C::Slot>;
    type Frame = Frame<C>;

    fn close(control: Self::Close<'_>) -> Self {
        Self(control.raw(), marker::PhantomData)
    }

    fn flight(key: C::Key) -> Self {
        Self(key.into(), marker::PhantomData)
    }

    fn prepare() -> Self {
        let raw: num::NonZero<u64> = C::control(PREP, C::ZERO).into();
        Self(raw.get(), marker::PhantomData)
    }

    fn probe(probe: C::Probe) -> Self {
        Self(probe.into(), marker::PhantomData)
    }

    fn cancel_probe(probe: C::Probe) -> Self {
        let probe: u64 = probe.into();
        Self((248_u64 << SHIFT) | probe, marker::PhantomData)
    }

    fn shutdown() -> Self {
        let raw: num::NonZero<u64> = C::SHUTDOWN.into();
        Self(raw.get(), marker::PhantomData)
    }

    fn tuning(control: Self::Tuning) -> Self {
        let raw: num::NonZero<u64> = C::control(TUNING, control.slot()).into();
        Self(raw.get(), marker::PhantomData)
    }

    fn cancel_key(key: C::Key) -> Self {
        Self((250_u64 << SHIFT) | key.into(), marker::PhantomData)
    }

    fn cancel_tune(control: Self::Tuning) -> Self {
        let slot: num::NonZero<u32> = control.slot().into();
        Self(
            (249_u64 << SHIFT) | u64::from(slot.get() - 1),
            marker::PhantomData,
        )
    }

    fn closed(self) -> Option<C::Returned> {
        Close::<C>::decode(self.0)
    }

    fn cancel(cancel: host::Cancel<C::Key, Self::Tuning>) -> Self {
        use host::Cancel;
        match cancel {
            Cancel::Flight(key) => Self::cancel_key(key),
            Cancel::Tuning(tuning) => Self::cancel_tune(tuning),
        }
    }

    fn bind<D>(
        self,
        drain: &D,
    ) -> host::Bound<D::Echo, D::Class, Self::Frame, D::Flight, C::Key, Self::Tuning>
    where
        D: host::Drain<Key = C::Key, Returned = C::Returned>,
    {
        use host::{Bound, Cancel, Closed};
        match self.decode() {
            token::Decoded::Close(returned) => match drain.retired(returned) {
                Closed::Ready(echo) => Bound::Close(echo),
                Closed::Class {
                    echo,
                    expected,
                    observed,
                } => Bound::CloseClass {
                    echo,
                    expected,
                    observed,
                },
                Closed::Missing => Bound::Invalid,
            },
            token::Decoded::Framework(control) => Bound::Framework(control),
            token::Decoded::Flight(key) => drain.bind(key).map_or(Bound::Invalid, Bound::Flight),
            token::Decoded::CancelFlight(key) => Bound::Cancel(Cancel::Flight(key)),
            token::Decoded::CancelTuning(control) => Bound::Cancel(Cancel::Tuning(control)),
            token::Decoded::Invalid => Bound::Invalid,
        }
    }

    fn decode(self) -> token::Decoded<C::Key, C::Returned, Self::Frame, Self::Tuning> {
        if let Some(returned) = self.closed() {
            return token::Decoded::Close(returned);
        }
        let raw = match Frame::from_raw(self.0) {
            Ok(control) => return token::Decoded::Framework(control),
            Err(raw) => raw,
        };
        if let Some(key) = C::read_key(raw) {
            return token::Decoded::Flight(key);
        }
        let payload = raw & PAYLOAD;
        if raw >> SHIFT == 250 {
            return match C::read_key(payload) {
                Some(key) => token::Decoded::CancelFlight(key),
                None => token::Decoded::Invalid,
            };
        }
        if raw >> SHIFT == 249 {
            if payload > SLOT {
                return token::Decoded::Invalid;
            }
            return token::Decoded::CancelTuning(Tuning::new(C::Slot::from(bounded::U32::clamp(
                payload as usize,
            ))));
        }
        token::Decoded::Invalid
    }
}

const _: () = assert!(std::mem::size_of::<Encoded<()>>() == std::mem::size_of::<u64>());
