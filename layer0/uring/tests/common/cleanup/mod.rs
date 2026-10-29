pub mod backend;
pub mod schedule;
pub mod unmap;

use std::num;

use dope_sys::{
    ring::{self, create},
    table::{
        reclaim,
        token::{self, host},
    },
};
use dope_uring::Uring;
use o3::base::num::bounded;

use crate::common::ring::try_create;

type Token = <Uring as token::Source>::Token<Host>;
type Ring = <Uring as ring::Source>::Ring<Token>;
type Setup = create::Error<
    <Ring as create::Create>::Setup,
    <Ring as create::Create>::Map,
    <Ring as create::Create>::Cleanup,
>;

pub fn try_ring() -> Result<Ring, Setup> {
    try_create::<Token, 2, 4>(false)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Host;

#[derive(Debug)]
pub struct Echo(pub u64);

impl AsRef<u64> for Echo {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
}

impl reclaim::Item for Echo {
    type Slot = [u8; 3];
    type Flight = u64;

    fn slot(&self) -> Self::Slot {
        [1, 0, 0]
    }

    fn submit(self) -> u64 {
        self.0
    }
}

impl host::Retire for Echo {
    fn echo(&self) -> u64 {
        self.0
    }
}

impl token::Context for Host {
    type Slot = Slot;
    type Control = Control;

    const ZERO: Self::Slot = Slot(num::NonZero::<u32>::MIN);
    const SHUTDOWN: Self::Control =
        Control(num::NonZero::<u64>::MIN.saturating_add(0xff07_0000_0000_0000 - 1));

    fn control(tag: u8, slot: Self::Slot) -> Self::Control {
        let raw = (u64::from(u8::MAX) << 56) | (u64::from(tag) << 48) | u64::from(slot.0.get() - 1);
        Control(num::NonZero::<u64>::MIN.saturating_add(raw - 1))
    }
}

impl host::Context for Host {
    type Key = num::NonZero<u64>;
    type Close<'d> = Echo;
    type Retire<'d> = Echo;
    type Returned = u64;
    type Probe = u8;

    fn read_key(value: u64) -> Option<Self::Key> {
        if value >> 56 != 0 {
            return None;
        }
        num::NonZero::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Control(num::NonZero<u64>);

impl From<Control> for num::NonZero<u64> {
    fn from(control: Control) -> Self {
        control.0
    }
}

impl TryFrom<u64> for Control {
    type Error = u64;
    fn try_from(raw: u64) -> Result<Self, u64> {
        if raw >> 56 != u64::from(u8::MAX) {
            return Err(raw);
        }
        Ok(Self(num::NonZero::<u64>::MIN.saturating_add(raw - 1)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Slot(num::NonZero<u32>);

impl From<bounded::U32<0, 0xff_ffff>> for Slot {
    fn from(index: bounded::U32<0, 0xff_ffff>) -> Self {
        Self(num::NonZero::<u32>::MIN.saturating_add(index.get()))
    }
}

impl From<Slot> for num::NonZero<u32> {
    fn from(slot: Slot) -> Self {
        slot.0
    }
}
