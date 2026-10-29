use std::{num::NonZero, ops};

use o3::{
    buffer,
    buffer::{
        Complete as _,
        bytes::bounded::Value,
        hold, resident, storage,
        storage::{provided, range},
        write::{consume, consume::Consume},
    },
};
use provided::account;

use crate::io::{recv, transfer, transfer::Length as _};

#[repr(transparent)]
pub struct Shared<'d>(Value<storage::Shared<provided::Owner<'d>>>);

impl<'d> Shared<'d> {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn len(&self) -> transfer::Len {
        self.0.len()
    }

    pub fn try_get(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Result<Value<storage::Shared<Option<provided::Account<'d>>>>, account::Error> {
        self.0.try_get(range, budget)
    }
}

impl<'d> provided::Frozen<'d> for Shared<'d> {
    type Account = Value<storage::Shared<Option<provided::Account<'d>>>>;
    fn try_account(&self, budget: &resident::Budget<'d>) -> Result<Self::Account, account::Cause> {
        self.0.try_account(budget)
    }
}

impl buffer::Residency for Shared<'_> {
    fn resident(&self) -> usize {
        self.0.resident()
    }
}

impl AsRef<[u8]> for Shared<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl transfer::Length for Shared<'_> {
    fn len(&self) -> transfer::Len {
        self.0.len()
    }
}

// SAFETY: the wrapped provided owner retains the exact initialized byte range.
unsafe impl buffer::raw::Consume for Shared<'_> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        // SAFETY: the transparent wrapper exposes its entire inner range.
        <Value<storage::Shared<provided::Owner<'_>>> as buffer::raw::Consume>::consume(unsafe {
            prefix.map(|owner| &mut owner.0)
        });
    }
}

impl Consume for Shared<'_> {
    fn remaining(&self) -> usize {
        self.0.as_ref().len()
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.0.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.0.consume_max(max)
    }
}

impl<'d> buffer::Source for Shared<'d> {
    type View<'input>
        = Value<buffer::View<'input, storage::Shared<provided::Owner<'d>>>>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.0.as_ref().len()
    }
}

// SAFETY: The bounded provided view delegates selection and retention to its original owner.
unsafe impl<'d> hold::raw::Retain<'d> for Shared<'d> {
    fn charge<'a>(plan: &Self::View<'a>) -> hold::Charge
    where
        Self: 'a,
    {
        <Value<storage::Shared<provided::Owner<'d>>> as hold::raw::Retain<'d>>::charge(plan)
    }

    fn try_copy<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<resident::Shared<'d>, hold::Error>
    where
        Self: 'a,
    {
        <Value<storage::Shared<provided::Owner<'d>>> as hold::raw::Retain<'d>>::try_copy(
            plan, budget,
        )
    }

    fn try_retain<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<resident::Shared<'d>, hold::Error>
    where
        Self: 'a,
    {
        <Value<storage::Shared<provided::Owner<'d>>> as hold::raw::Retain<'d>>::try_retain(
            plan, budget,
        )
    }
}

impl<'d> buffer::Cursor for Shared<'d> {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        use buffer::Complete as _;

        self.view().try_get(range)
    }
}

impl<'d> buffer::Complete for Shared<'d> {
    fn view(&self) -> Self::View<'_> {
        self.0.view()
    }
}

const _: () = {
    assert!(size_of::<Shared<'static>>() == 3 * size_of::<usize>());
};

/// Received storage with its original driver and completion-length proof.
///
/// A temporary view returns a borrow of the receive owner.
#[repr(transparent)]
pub struct Lease<'d>(Value<provided::Unique<'d>>);

impl<'d> Lease<'d> {
    pub(crate) fn new(buffer: Value<provided::Unique<'d>>) -> Self {
        Self(buffer)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }

    /// Returns the exact remaining transfer-bounded length.
    pub fn len(&self) -> transfer::Len {
        self.0.len()
    }

    pub(in crate::io) fn into_inner(self) -> Value<provided::Unique<'d>> {
        self.0
    }

    pub fn into_shared(self) -> recv::Shared<'d> {
        self.into()
    }
}

impl AsRef<[u8]> for Lease<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl transfer::Length for Lease<'_> {
    fn len(&self) -> transfer::Len {
        self.0.len()
    }
}

// SAFETY: the exclusive provided owner retains the exact initialized range.
unsafe impl buffer::raw::Consume for Lease<'_> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        // SAFETY: projection preserves the same exclusive owner, range and end.
        Value::<provided::Unique>::consume(unsafe { prefix.map(|owner| &mut owner.0) });
    }
}

impl Consume for Lease<'_> {
    fn remaining(&self) -> usize {
        self.as_slice().len()
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.0.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.0.consume_max(max)
    }
}

const _: () = {
    assert!(size_of::<Lease<'static>>() == 3 * size_of::<usize>());
};

impl<'d> provided::Mutable<'d> for Lease<'d> {
    type Account = Value<storage::Shared<Option<provided::Account<'d>>>>;
    type Frozen = recv::Shared<'d>;
    type Prefix<'a>
        = Value<provided::Prefix<'a, 'd>>
    where
        Self: 'a;

    fn try_take(&mut self, amount: NonZero<usize>) -> Result<Self::Prefix<'_>, consume::Error> {
        self.0.try_take(amount)
    }
}

impl<'d> From<Lease<'d>> for Shared<'d> {
    fn from(lease: Lease<'d>) -> Self {
        let Lease(inner) = lease;
        Self(inner.into())
    }
}

impl AsMut<[u8]> for Lease<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl<'d> buffer::Source for Lease<'d> {
    type View<'input>
        = Value<&'input [u8]>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.0.as_ref().len()
    }
}

// SAFETY: The immutable lease view copies exactly its selected bytes into the resident domain.
unsafe impl<'d> hold::raw::Retain<'d> for Lease<'d> {
    fn try_copy<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<resident::Shared<'d>, hold::Error>
    where
        Self: 'a,
    {
        Ok(resident::Shared::try_copy(plan, budget)?)
    }
}

impl<'d> buffer::Cursor for Lease<'d> {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.0.view().try_get(range)
    }
}

impl<'d> buffer::Complete for Lease<'d> {
    fn view(&self) -> Self::View<'_> {
        self.0.view()
    }
}

impl<'d> ops::Deref for Lease<'d> {
    type Target = Value<provided::Unique<'d>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

mod raw;
