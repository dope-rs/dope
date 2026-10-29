use core::{hash, marker, num};

use o3::base::marker::ThreadBound;

use crate::driver::{
    route,
    route::{Epoch, indices, indices::SlotIndex, kind, table},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct Token {
    pub(super) epoch: num::NonZero<u64>,
    pub(super) thread: ThreadBound,
    pub(super) meta: u64,
}

impl hash::Hash for Token {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        hash::Hash::hash(&self.epoch.get(), state);
        hash::Hash::hash(&self.meta, state);
    }
}

impl Token {
    pub(in crate::driver) const fn from_flight(
        epoch: route::Epoch,
        payload: [u8; 6],
        kind: kind::Native,
        route: route::Route,
    ) -> Self {
        let (epoch, thread) = epoch.into_parts();
        let [a, b, c, d, e, f] = payload;
        Self {
            epoch,
            thread,
            meta: u64::from_le_bytes([a, b, c, d, e, f, kind as u8, route.raw()]),
        }
    }

    const fn pack(
        route: route::Route,
        kind: u8,
        slot: indices::SlotIndex,
        epoch: route::Epoch,
    ) -> Self {
        let (epoch, thread) = epoch.into_parts();
        Self {
            epoch,
            thread,
            meta: ((route.raw() as u64) << route::SHIFT)
                | ((kind as u64) << route::KIND_SHIFT)
                | slot.raw() as u64,
        }
    }

    pub const fn route(self) -> route::Route {
        route::Route::from_bits((self.meta >> route::SHIFT) as u8)
    }

    pub const fn kind(self) -> Result<kind::Kind, kind::Unknown> {
        let raw = self.kind_raw();
        match raw {
            0 => Ok(kind::Kind::Unspecified),
            1 => Ok(kind::Kind::Accept),
            2 => Ok(kind::Kind::Recv),
            3 => Ok(kind::Kind::Send),
            5 => Ok(kind::Kind::Socket),
            6 => Ok(kind::Kind::Connect),
            7 => Ok(kind::Kind::Shutdown),
            11 => Ok(kind::Kind::Open),
            19 => Ok(kind::Kind::OneShot),
            21 => Ok(kind::Kind::TaskQueue),
            25 => Ok(kind::Kind::Tuning),
            28 => Ok(kind::Kind::Write),
            29 => Ok(kind::Kind::Sync),
            unknown => Err(kind::Unknown::new(unknown)),
        }
    }

    pub(crate) const fn kind_raw(self) -> u8 {
        (self.meta >> route::KIND_SHIFT) as u8
    }

    pub const fn slot(self) -> indices::SlotIndex {
        SlotIndex::from_field(self.meta)
    }

    pub const fn epoch(self) -> route::Epoch {
        Epoch::new(self.epoch, self.thread)
    }

    pub(super) const fn encode<Tag, C>(parts: table::Parts<Tag>) -> Self
    where
        Tag: route::Select<C>,
        C: kind::Class,
    {
        Self::pack(Tag::ROUTE, C::CODE.raw(), parts.slot(), parts.epoch())
    }

    #[doc(hidden)]
    pub const fn from_parts<Tag: route::Tag>(parts: table::Parts<Tag>) -> Self {
        Self::pack(
            Tag::ROUTE,
            <Tag::Class as kind::Class>::CODE.raw(),
            parts.slot(),
            parts.epoch(),
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Brand<'d, Tag> {
    pub(super) driver: crate::driver::Invariant<'d>,
    tag: marker::PhantomData<*mut Tag>,
}

impl<'d, Tag> Brand<'d, Tag> {
    pub(crate) const fn new(driver: crate::driver::Invariant<'d>) -> Self {
        Self {
            driver,
            tag: marker::PhantomData,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct OutboundKey(u8);

impl OutboundKey {
    pub(crate) const fn raw(self) -> u32 {
        self.0 as u32
    }

    pub(crate) const fn from_raw(raw: u32) -> Option<Self> {
        if raw < route::FRAMEWORK as u32 {
            Some(Self(raw as u8))
        } else {
            None
        }
    }

    /// Type-branded outbound key for callers that carry an application route tag.
    pub(crate) const fn for_tag<Tag: route::Tag>() -> Self {
        const { assert!(Tag::ROUTE.raw() != route::FRAMEWORK) };
        Self(Tag::ROUTE.raw())
    }
}
