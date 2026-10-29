use std::{mem, pin};

use data::{compose, exec};
use dope_core::{
    dope_sys::io::socket::msg,
    driver::{
        self,
        route::{Identity, KeyTag, table},
        schedule::ready,
    },
    io::transfer,
};
use dope_net::link::{egress, egress::data, pool};
use egress::reserve;
use o3::{
    alloc,
    base::{cell::region, num::bounded},
    buffer::{resident, segments::scatter},
    depot::{arena::pool::recycle, capacity},
};
use reserve::{demand, exact};

use crate::network::listener;

pub(in crate::network::listener) mod access;
pub(in crate::network::listener) mod geometry;
pub(in crate::network::listener) mod release;

pub(in crate::network::listener) use release::Claim;

pub(in crate::network::listener) const WRITE_BUF_CAP: usize = 16 * 1024;

type DirectSource<'p, 's, 'd, ID, B> = release::Source<'p, 's, 'd, Flight<B>, ID>;
type HeaderSource<'p, 's, 'd, ID> = release::Source<'p, 's, 'd, Header, ID>;

/// Byte length proven to fit one listener header owner.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::network::listener) struct Len(transfer::Len);

/// A positive length bounded by its fixed header owner.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::network::listener) struct NonZero(transfer::NonZero);

impl NonZero {
    pub(in crate::network::listener) const fn transfer(self) -> transfer::NonZero {
        self.0
    }
}

impl Len {
    pub(in crate::network::listener) const ZERO: Self = Self(transfer::Len::ZERO);

    pub(in crate::network::listener) fn try_new(written: usize) -> Result<Self, bounded::Error> {
        Ok(Self::from(
            bounded::Usize::<WRITE_BUF_CAP>::ZERO.try_add(written)?,
        ))
    }

    pub(in crate::network::listener) const fn transfer(self) -> transfer::Len {
        self.0
    }

    pub(in crate::network::listener) const fn nonempty(self) -> Option<NonZero> {
        match self.0.nonzero() {
            Some(len) => Some(NonZero(len)),
            None => None,
        }
    }
}

impl From<bounded::Usize<WRITE_BUF_CAP>> for Len {
    fn from(written: bounded::Usize<WRITE_BUF_CAP>) -> Self {
        Self(transfer::Len::from_count(written))
    }
}

/// One address-stable listener response header buffer.
#[pin_project::pin_project(!Unpin)]
pub(in crate::network::listener) struct Header {
    bytes: [u8; WRITE_BUF_CAP],
}

impl Default for Header {
    fn default() -> Self {
        Self {
            bytes: [0; WRITE_BUF_CAP],
        }
    }
}

impl Header {
    pub(in crate::network::listener) fn as_slice(self: pin::Pin<&Self>) -> &[u8] {
        &self.get_ref().bytes
    }

    fn as_mut_slice(self: pin::Pin<&mut Self>) -> &mut [u8; WRITE_BUF_CAP] {
        self.project().bytes
    }
}

/// Pinned retention for one active direct send.
#[pin_project::pin_project(!Unpin)]
pub(in crate::network::listener) struct Flight<B> {
    #[pin]
    pub(super) header: Header,
    pub(super) source: Option<B>,
    pub(super) position: scatter::raw::Position,
    pub(super) iovecs: [msg::Iovec; listener::IOV_CAP],
    pub(super) message: msg::Header,
}

impl<B> Default for Flight<B> {
    fn default() -> Self {
        Self {
            header: Header::default(),
            source: None,
            position: scatter::raw::Position::new(),
            iovecs: [msg::Iovec::empty(); listener::IOV_CAP],
            message: msg::Header::new(),
        }
    }
}

pub(in crate::network::listener) struct Arena<'s, 'd, ID: Identity, B = resident::Shared<'d>> {
    direct: release::Gate<'s, 'd, Flight<B>, ID>,
    headers: release::Gate<'s, 'd, Header, ID>,
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Arena<'_, 'd, ID, B> {
    pub(in crate::network::listener) fn try_with<R>(
        shape: geometry::Geometry<B::Mode>,
        lanes: table::Capacity,
        driver: driver::Reference<'d>,
        build: impl for<'p, 's> FnOnce(
            &'p Arena<'s, 'd, ID, B>,
            demand::Setup<<B::Mode as exec::Mode>::Reserve<WRITE_BUF_CAP>>,
        ) -> R,
    ) -> Result<R, alloc::Error> {
        let (setup, direct, headers) = shape.into_parts();
        let (direct, d_lanes) = match direct {
            Some(count) => (count.get(), Some(lanes)),
            None => (0, None),
        };
        let (headers, h_lanes) = match headers {
            Some(count) => (count.get(), Some(lanes)),
            None => (0, None),
        };
        let direct = recycle::Store::try_new(capacity::Count::new(direct))?;
        let headers = recycle::Store::try_new(capacity::Count::new(headers))?;
        let arena = Arena {
            direct: release::Gate::try_new(&direct, driver, d_lanes)?,
            headers: release::Gate::try_new(&headers, driver, h_lanes)?,
        };
        Ok(build(&arena, setup))
    }
}

pub(in crate::network::listener) struct Retention<
    'p,
    's,
    'd,
    ID: Identity,
    B = resident::Shared<'d>,
> {
    arena: &'p Arena<'s, 'd, ID, B>,
    lane: pool::raw::Physical<'d, ID>,
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> Retention<'p, 's, 'd, ID, B> {
    pub(super) const fn new(
        arena: &'p Arena<'s, 'd, ID, B>,
        lane: pool::raw::Physical<'d, ID>,
    ) -> Self {
        Self { arena, lane }
    }

    pub(in crate::network::listener) fn reborrow(&self) -> Retention<'p, 's, 'd, ID, B> {
        Retention {
            arena: self.arena,
            lane: self.lane,
        }
    }

    pub(in crate::network::listener) fn sources(
        &self,
    ) -> (
        Option<DirectSource<'p, 's, 'd, ID, B>>,
        Option<HeaderSource<'p, 's, 'd, ID>>,
    ) {
        (
            release::Source::from(&self.arena.direct, self.lane),
            release::Source::from(&self.arena.headers, self.lane),
        )
    }

    pub(in crate::network::listener) fn take_direct(
        &self,
        source: DirectSource<'p, 's, 'd, ID, B>,
        target: ready::Target<'d>,
    ) -> Claim<DirectSlot<'p, 's, 'd, ID, B>> {
        if let Some(header) = release::Source::from(&self.arena.headers, self.lane) {
            header.cancel(target);
        }
        match source.claim(target) {
            Claim::Acquired(flight) => Claim::Acquired(DirectSlot(flight)),
            Claim::Waiting => Claim::Waiting,
        }
    }

    pub(in crate::network::listener) fn cancel_wait(&self, target: ready::Target<'d>) {
        let (direct, header) = self.sources();
        if let Some(direct) = direct {
            direct.cancel(target);
        }
        if let Some(header) = header {
            header.cancel(target);
        }
    }

    pub(in crate::network::listener) fn take_header(
        &self,
        source: HeaderSource<'p, 's, 'd, ID>,
        target: ready::Target<'d>,
    ) -> Claim<HeaderSlot<'p, 's, 'd, ID>> {
        if let Some(direct) = release::Source::from(&self.arena.direct, self.lane) {
            direct.cancel(target);
        }
        match source.claim(target) {
            Claim::Acquired(slot) => Claim::Acquired(HeaderSlot(slot)),
            Claim::Waiting => Claim::Waiting,
        }
    }

    pub(in crate::network::listener) fn cancel<'e>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        queue: &egress::Queue<
            '_,
            'e,
            'd,
            { listener::IOV_CAP },
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<WRITE_BUF_CAP>,
        >,
        target: ready::Target<'d>,
    ) {
        self.cancel_wait(target);
        queue.cancel_wait(token);
    }
}

pub(in crate::network::listener) struct HeaderSlot<'p, 's, 'd, ID: Identity>(
    release::Lease<'p, 's, 'd, Header, ID>,
);

pub(in crate::network::listener) struct DirectSlot<
    'p,
    's,
    'd,
    ID: Identity,
    B = resident::Shared<'d>,
>(release::Lease<'p, 's, 'd, Flight<B>, ID>);

pub(in crate::network::listener) enum Buffer<
    'a,
    'p,
    's,
    'e,
    'd,
    ID: Identity,
    B = resident::Shared<'d>,
> where
    B: data::raw::Payload<'d>,
{
    Direct(DirectSlot<'p, 's, 'd, ID, B>),
    Queued {
        slot: HeaderSlot<'p, 's, 'd, ID>,
        queue: egress::Queue<
            'a,
            'e,
            'd,
            { listener::IOV_CAP },
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<WRITE_BUF_CAP>,
        >,
        plan: reserve::Plan<
            'a,
            'e,
            'd,
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<WRITE_BUF_CAP>,
        >,
    },
}

pub(in crate::network::listener) enum Body<'a, 'p, 's, 'e, 'd, ID: Identity, S, B>
where
    B: data::raw::Payload<'d>,
{
    Direct {
        slot: DirectSlot<'p, 's, 'd, ID, B>,
        value: egress::NonEmpty<S>,
    },
    Queued {
        offer: exact::Offer<
            'a,
            'e,
            'd,
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<WRITE_BUF_CAP>,
            S,
        >,
    },
}

pub(in crate::network::listener) enum Bounded<
    'a,
    'p,
    's,
    'e,
    'd,
    ID: Identity,
    B = resident::Shared<'d>,
> where
    B: data::raw::Payload<'d>,
{
    Direct(DirectSlot<'p, 's, 'd, ID, B>),
    Queued {
        slot: HeaderSlot<'p, 's, 'd, ID>,
        queue: egress::Queue<
            'a,
            'e,
            'd,
            { listener::IOV_CAP },
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<WRITE_BUF_CAP>,
        >,
    },
}

impl<ID: Identity> AsMut<[u8; WRITE_BUF_CAP]> for HeaderSlot<'_, '_, '_, ID> {
    fn as_mut(&mut self) -> &mut [u8; WRITE_BUF_CAP] {
        self.0.get_mut().as_mut_slice()
    }
}

impl<'p, 's, 'd, ID: Identity> HeaderSlot<'p, 's, 'd, ID> {
    pub(in crate::network::listener) fn finish(
        self,
        len: Option<NonZero>,
    ) -> Option<listener::raw::Owned<'p, 's, 'd, ID>> {
        len.map(|len| listener::raw::Owned::new(self.0, len))
    }
}

impl<ID: Identity, B> AsMut<[u8; WRITE_BUF_CAP]> for DirectSlot<'_, '_, '_, ID, B> {
    fn as_mut(&mut self) -> &mut [u8; WRITE_BUF_CAP] {
        self.0.get_mut().project().header.as_mut_slice()
    }
}

const _: () = {
    assert!(mem::size_of::<Len>() == mem::size_of::<transfer::Len>());
    assert!(mem::size_of::<NonZero>() == mem::size_of::<Len>());
    assert!(mem::size_of::<Option<NonZero>>() == mem::size_of::<Len>());
    assert!(mem::size_of::<Header>() == WRITE_BUF_CAP);
    assert!(mem::align_of::<Header>() == mem::align_of::<u8>());
    assert!(mem::size_of::<Arena<'static, 'static, KeyTag<0>>>() == 12 * mem::size_of::<usize>());
    assert!(mem::align_of::<Arena<'static, 'static, KeyTag<0>>>() == mem::align_of::<usize>());
    assert!(
        mem::size_of::<Retention<'static, 'static, 'static, KeyTag<0>>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Claim<HeaderSlot<'static, 'static, 'static, KeyTag<0>>>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<listener::raw::Owned<'static, 'static, 'static, KeyTag<0>>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Bounded<'static, 'static, 'static, 'static, 'static, KeyTag<0>>>()
            < mem::size_of::<Buffer<'static, 'static, 'static, 'static, 'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Option<listener::raw::Owned<'static, 'static, 'static, KeyTag<0>>>>()
            == 3 * mem::size_of::<usize>()
    );
};
