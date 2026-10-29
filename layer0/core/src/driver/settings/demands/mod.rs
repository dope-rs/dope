use o3::depot::capacity;

use crate::{
    driver::{route::table::conn, settings, settings::errors},
    io::transfer,
};

pub mod slots;

#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(C)]
pub struct Files {
    accepted: Accept,
    outbound: Outbound,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct Accept(u32);

#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct Outbound(u32);

impl Files {
    pub const ZERO: Self = Self::new(Accept::ZERO, Outbound::ZERO);

    pub const fn new(accepted: Accept, outbound: Outbound) -> Self {
        Self { accepted, outbound }
    }

    pub const fn accept(accepted: Accept) -> Self {
        Self::new(accepted, Outbound::ZERO)
    }

    pub const fn only_out(outbound: Outbound) -> Self {
        Self::new(Accept::ZERO, outbound)
    }

    pub const fn accepted(self) -> Accept {
        self.accepted
    }

    pub const fn outbound(self) -> Outbound {
        self.outbound
    }

    pub(in crate::driver::settings) fn merge(self, next: Self) -> Result<Self, errors::Claim> {
        let current = self.accepted.get();
        let added = next.accepted.get();
        let accepted = Accept(
            current
                .checked_add(added)
                .ok_or(errors::Claim::AcceptedDescriptorsOverflow { current, added })?,
        );
        let current = self.outbound.get();
        let added = next.outbound.get();
        let outbound = Outbound(
            current
                .checked_add(added)
                .ok_or(errors::Claim::OutboundDescriptorsOverflow { current, added })?,
        );
        Ok(Self::new(accepted, outbound))
    }
}

impl Accept {
    pub const ZERO: Self = Self(0);
    pub(in crate::driver::settings) const fn get(self) -> u32 {
        self.0
    }
}

impl From<slots::Slots> for Accept {
    fn from(slots: slots::Slots) -> Self {
        Self(slots.raw())
    }
}

impl From<conn::Capacity> for Accept {
    fn from(capacity: conn::Capacity) -> Self {
        Self(capacity.raw())
    }
}

impl Outbound {
    pub const ZERO: Self = Self(0);
    pub const fn fixed<const VALUE: u32>() -> Self {
        Self(VALUE)
    }
    pub(in crate::driver::settings) const fn get(self) -> u32 {
        self.0
    }

    pub const fn scale<const FACTOR: u32>(capacity: conn::Capacity) -> Self {
        Self(capacity.scale::<FACTOR>().raw())
    }
}

impl TryFrom<usize> for Accept {
    type Error = Overflow;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self(capacity::Count::try_from(value)?.raw()))
    }
}

impl TryFrom<usize> for Outbound {
    type Error = Overflow;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self(capacity::Count::try_from(value)?.raw()))
    }
}

/// Additive receive-pool demand. Entries compose by addition while buffer
/// length composes by maximum because every slot uses the largest framing.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct Receive {
    entries: Entries,
    buffer_len: transfer::Len,
}

/// Additive receive-slot demand representable by the claim accumulator.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct Entries(u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Overflow {
    actual: usize,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct Submit(u32);

/// Additive scheduler demand for ready and timer slots.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(C)]
pub struct Schedule {
    ready: slots::Slots,
    timers: slots::Slots,
}

impl Receive {
    pub const ZERO: Self = Self {
        entries: Entries::ZERO,
        buffer_len: transfer::Len::ZERO,
    };

    pub const fn new(entries: Entries, len: transfer::Len) -> Self {
        Self {
            entries,
            buffer_len: len,
        }
    }

    pub const fn entries(self) -> Entries {
        self.entries
    }

    pub const fn buffer_len(self) -> transfer::Len {
        self.buffer_len
    }

    pub(in crate::driver::settings) fn merge(self, next: Self) -> Result<Self, errors::Claim> {
        let entries = self.entries.0.checked_add(next.entries.0).ok_or(
            errors::Claim::ReceiveEntriesOverflow {
                current: self.entries.0,
                added: next.entries.0,
            },
        )?;
        Ok(Self::new(
            Entries(entries),
            self.buffer_len.max(next.buffer_len),
        ))
    }
}

impl From<settings::Receive> for Receive {
    fn from(layout: settings::Receive) -> Self {
        Self::new(
            Entries(u32::from(layout.entries())),
            transfer::Len::from_nonzero(layout.buffer_len()),
        )
    }
}

impl From<settings::Datagram> for Receive {
    fn from(layout: settings::Datagram) -> Self {
        Self::new(
            Entries(u32::from(layout.0.entries())),
            transfer::Len::from_nonzero(layout.0.buffer_len()),
        )
    }
}

impl Entries {
    pub const ZERO: Self = Self(0);

    pub(in crate::driver::settings) const fn get(self) -> u32 {
        self.0
    }
}

impl From<capacity::Count> for Entries {
    fn from(count: capacity::Count) -> Self {
        Self(count.raw())
    }
}

impl TryFrom<usize> for Entries {
    type Error = Overflow;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self(capacity::Count::try_from(value)?.raw()))
    }
}

impl From<capacity::Error> for Overflow {
    fn from(error: capacity::Error) -> Self {
        Self::new(error.requested())
    }
}

impl Overflow {
    pub(in crate::driver::settings) const fn new(actual: usize) -> Self {
        Self { actual }
    }
}

impl Default for Receive {
    fn default() -> Self {
        Self::ZERO
    }
}

impl Submit {
    pub(in crate::driver::settings) const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for Submit {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl Schedule {
    pub const ZERO: Self = Self::new(slots::Slots::ZERO, slots::Slots::ZERO);

    pub const fn new(ready: slots::Slots, timers: slots::Slots) -> Self {
        Self { ready, timers }
    }

    pub const fn fixed<const READY: u32, const TIMERS: u32>() -> Self {
        Self::new(
            slots::Slots::fixed::<READY>(),
            slots::Slots::fixed::<TIMERS>(),
        )
    }

    pub const fn ready(self) -> slots::Slots {
        self.ready
    }

    pub const fn timers(self) -> slots::Slots {
        self.timers
    }

    pub(in crate::driver::settings) fn merge(self, next: Self) -> Result<Self, errors::Claim> {
        let ready = self
            .ready
            .checked_add(next.ready)
            .map_err(errors::Claim::SchedulerReadyOverflow)?;
        let timers = self
            .timers
            .checked_add(next.timers)
            .map_err(errors::Claim::SchedulerTimersOverflow)?;
        Ok(Self::new(ready, timers))
    }
}

const _: () = {
    assert!(core::mem::size_of::<Files>() == 2 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Accept>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Outbound>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Receive>() == 2 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Entries>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Submit>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Schedule>() == 2 * core::mem::size_of::<u32>());
};
