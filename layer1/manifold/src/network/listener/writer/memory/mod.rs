use std::{cell::UnsafeCell, mem, pin};

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
    buffer::resident,
    depot::{arena::pool::recycle, capacity},
};
use reserve::demand;

use crate::network::listener::{self, writer};

pub(in crate::network::listener) mod access;
pub(in crate::network::listener) mod geometry;
pub(in crate::network::listener) mod raw;
pub(in crate::network::listener) mod release;

pub(in crate::network::listener) use release::Claim;

pub(in crate::network::listener) const WRITE_BUF_CAP: usize = 16 * 1024;

type DirectSource<'p, 's, 'd, ID, B> =
    release::Source<'p, 's, 'd, Flight<B, <B as data::raw::Payload<'d>>::Position>, ID>;
pub(in crate::network::listener) type Frame<'d, B> =
    Flight<B, <B as data::raw::Payload<'d>>::Position>;
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

impl From<bounded::U32<0, { WRITE_BUF_CAP as u32 }>> for Len {
    fn from(written: bounded::U32<0, { WRITE_BUF_CAP as u32 }>) -> Self {
        Self(transfer::Len::from(written.widen()))
    }
}

/// One address-stable listener response header buffer.
#[pin_project::pin_project(!Unpin)]
pub(in crate::network::listener) struct Header {
    bytes: [mem::MaybeUninit<u8>; WRITE_BUF_CAP],
}

impl Header {
    fn new() -> Self {
        Self {
            bytes: [mem::MaybeUninit::uninit(); WRITE_BUF_CAP],
        }
    }

    pub(in crate::network::listener) fn as_slice(self: pin::Pin<&Self>) -> &[mem::MaybeUninit<u8>] {
        &self.get_ref().bytes
    }

    fn as_mut_slice(self: pin::Pin<&mut Self>) -> &mut [mem::MaybeUninit<u8>; WRITE_BUF_CAP] {
        self.project().bytes
    }
}

/// Pinned retention for one active direct send.
#[pin_project::pin_project(!Unpin)]
pub(in crate::network::listener) struct Flight<B, P: data::position::State> {
    #[pin]
    pub(super) header: Header,
    pub(super) source: mem::MaybeUninit<B>,
    pub(super) position: P,
    pub(super) iovecs: [mem::MaybeUninit<msg::Iovec>; listener::IOV_CAP],
    pub(super) message: mem::MaybeUninit<msg::Header>,
}

impl<B, P: data::position::State> Flight<B, P> {
    fn new() -> Self {
        Self {
            header: Header::new(),
            source: mem::MaybeUninit::uninit(),
            position: P::default(),
            iovecs: [mem::MaybeUninit::uninit(); listener::IOV_CAP],
            message: mem::MaybeUninit::uninit(),
        }
    }
}

enum Supply<'s, 'd, ID: Identity, T> {
    Fixed(&'s [raw::Home<T>]),
    Shared(release::Gate<'s, 'd, T, ID>),
}

pub(in crate::network::listener) struct Arena<
    's,
    'd,
    ID: Identity,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
> {
    direct: Supply<'s, 'd, ID, Frame<'d, B>>,
    headers: release::Gate<'s, 'd, Header, ID>,
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Arena<'_, 'd, ID, B> {
    pub(in crate::network::listener) fn try_with<R>(
        shape: geometry::Geometry<B::Mode>,
        lanes: table::Capacity,
        driver: driver::Reference<'d>,
        build: impl for<'p, 's> FnOnce(
            &'p Arena<'s, 'd, ID, B>,
            demand::Setup<writer::Reserve<'d, B>>,
        ) -> R,
    ) -> Result<R, alloc::Error> {
        let (setup, direct, headers) = shape.into_parts();
        enum Store<T> {
            Fixed(alloc::array::Block<raw::Home<T>, capacity::Count>),
            Shared(recycle::Store<T>),
        }
        let store = match direct {
            Some(count) if count.get() == lanes.count().raw() => Store::Fixed(
                alloc::array::Block::try_new(lanes.count(), ())?
                    .repeat_with(|| raw::Home(UnsafeCell::new(Flight::new()))),
            ),
            count => Store::Shared(recycle::Store::try_from_fn(
                capacity::Count::new(count.map_or(0, |count| count.get())),
                |_| Flight::new(),
            )?),
        };
        let (headers, h_lanes) = match headers {
            Some(count) => (count.get(), Some(lanes)),
            None => (0, None),
        };
        let headers =
            recycle::Store::try_from_fn(capacity::Count::new(headers), |_| Header::new())?;
        let arena = Arena {
            direct: match &store {
                Store::Fixed(cells) => Supply::Fixed(cells),
                Store::Shared(store) => Supply::Shared(release::Gate::try_new(
                    store,
                    driver,
                    direct.map(|_| lanes),
                )?),
            },
            headers: release::Gate::try_new(&headers, driver, h_lanes)?,
        };
        Ok(build(&arena, setup))
    }

    /// # Safety
    /// The key is the original connection vacancy from this arena's installation.
    pub(in crate::network::listener) unsafe fn home(
        &self,
        key: pool::Key<'d, ID>,
    ) -> Option<&raw::Home<Frame<'d, B>>> {
        let Supply::Fixed(cells) = &self.direct else {
            return None;
        };
        // SAFETY: the fixed array and connection pool use the same immutable extent.
        Some(unsafe { cells.get_unchecked(key.lane().index().get() as usize) })
    }
}

pub(in crate::network::listener) struct Retention<
    'p,
    's,
    'd,
    ID: Identity,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
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

    pub(in crate::network::listener) fn shared(&self) -> Option<DirectSource<'p, 's, 'd, ID, B>> {
        match &self.arena.direct {
            Supply::Fixed(_) => None,
            Supply::Shared(gate) => release::Source::from(gate, self.lane),
        }
    }

    pub(in crate::network::listener) fn is_fixed(&self) -> bool {
        matches!(self.arena.direct, Supply::Fixed(_))
    }

    pub(in crate::network::listener) fn header(&self) -> Option<HeaderSource<'p, 's, 'd, ID>> {
        release::Source::from(&self.arena.headers, self.lane)
    }

    pub(in crate::network::listener) fn cancel_header(&self, target: ready::Target<'d>) {
        if let Some(header) = self.header() {
            header.cancel(target);
        }
    }

    pub(in crate::network::listener) fn claim_shared(
        &self,
        source: DirectSource<'p, 's, 'd, ID, B>,
        target: ready::Target<'d>,
    ) -> Claim<raw::DirectSlot<'p, 's, 'd, ID, B>> {
        self.cancel_header(target);
        match source.claim(target) {
            Claim::Acquired(lease) => Claim::Acquired(raw::DirectSlot::shared(lease)),
            Claim::Waiting => Claim::Waiting,
        }
    }

    pub(in crate::network::listener) fn cancel_wait(&self, target: ready::Target<'d>) {
        if let Some(direct) = self.shared() {
            direct.cancel(target);
        }
        self.cancel_header(target);
    }

    pub(in crate::network::listener) fn take_header(
        &self,
        source: HeaderSource<'p, 's, 'd, ID>,
        target: ready::Target<'d>,
    ) -> Claim<HeaderSlot<'p, 's, 'd, ID>> {
        if let Some(direct) = self.shared() {
            direct.cancel(target);
        }
        source.claim(target)
    }

    pub(in crate::network::listener) fn cancel<'e>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        queue: &egress::Queue<
            '_,
            'e,
            'd,
            { listener::IOV_CAP },
            writer::Payload<'p, 's, 'd, ID, B>,
            writer::Reserve<'d, B>,
        >,
        target: ready::Target<'d>,
    ) {
        self.cancel_wait(target);
        queue.cancel_wait(token);
    }
}

pub(in crate::network::listener) type HeaderSlot<'p, 's, 'd, ID> =
    release::Lease<'p, 's, 'd, Header, ID>;

pub(in crate::network::listener) type Plan<'a, 'p, 's, 'e, 'd, ID, B, G> =
    reserve::Plan<'a, 'e, 'd, writer::Payload<'p, 's, 'd, ID, B>, writer::Reserve<'d, B>, G>;

impl<ID: Identity> AsMut<[mem::MaybeUninit<u8>; WRITE_BUF_CAP]> for HeaderSlot<'_, '_, '_, ID> {
    fn as_mut(&mut self) -> &mut [mem::MaybeUninit<u8>; WRITE_BUF_CAP] {
        self.get_mut().as_mut_slice()
    }
}

impl<'p, 's, 'd, ID: Identity> HeaderSlot<'p, 's, 'd, ID> {
    pub(in crate::network::listener) fn finish(
        self,
        len: Option<NonZero>,
    ) -> Option<listener::raw::Owned<'p, 's, 'd, ID>> {
        len.map(|len| listener::raw::Owned::new(self, len))
    }
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> AsMut<[mem::MaybeUninit<u8>; WRITE_BUF_CAP]>
    for raw::DirectSlot<'_, '_, 'd, ID, B>
{
    fn as_mut(&mut self) -> &mut [mem::MaybeUninit<u8>; WRITE_BUF_CAP] {
        self.get_mut().project().header.as_mut_slice()
    }
}

const _: () = {
    assert!(
        mem::size_of::<Flight<resident::Shared<'_>, data::position::Saved>>()
            == mem::size_of::<Flight<resident::Shared<'_>, ()>>()
                + mem::size_of::<data::position::Saved>()
    );
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
        mem::size_of::<Option<listener::raw::Owned<'static, 'static, 'static, KeyTag<0>>>>()
            == 3 * mem::size_of::<usize>()
    );
};
