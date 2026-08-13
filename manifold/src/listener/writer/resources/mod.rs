use std::{marker, mem, num, pin, ptr};

use dope_core::io::socket::msg;
use dope_net::link::egress::{self, data};
use o3::collections::fixed::pinned::recycle;

use crate::listener;

mod sealed;

pub(in crate::listener) use sealed::Access;

pub(super) const WRITE_BUF_CAP: usize = 16 * 1024;

/// One invariant, address-stable listener response header buffer.
#[pin_project::pin_project(!Unpin)]
pub(in crate::listener) struct HeaderStorage<'d, const ID: u8> {
    bytes: [u8; WRITE_BUF_CAP],
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<const ID: u8> HeaderStorage<'_, ID> {
    pub(super) fn new() -> Self {
        Self {
            bytes: [0; WRITE_BUF_CAP],
            driver: marker::PhantomData,
        }
    }

    pub(in crate::listener) fn as_slice(self: pin::Pin<&Self>) -> &[u8] {
        &self.get_ref().bytes
    }

    fn as_mut_slice(self: pin::Pin<&mut Self>) -> &mut [u8] {
        self.project().bytes
    }
}

impl<const ID: u8> recycle::Recycle for HeaderStorage<'_, ID> {
    fn recycle(self: pin::Pin<&mut Self>) {}
}

/// Pinned retention for one active direct send.
#[pin_project::pin_project(!Unpin)]
pub(in crate::listener) struct Flight<'d, const ID: u8> {
    #[pin]
    pub(super) header: HeaderStorage<'d, ID>,
    pub(super) source: Option<data::Buffer<'d>>,
    pub(super) iovecs: [msg::Iovec; 2],
    pub(super) message: msg::Header,
}

impl<'d, const ID: u8> Flight<'d, ID> {
    pub(super) fn new() -> Self {
        Self {
            header: HeaderStorage::new(),
            source: None,
            iovecs: [msg::Iovec::empty(); 2],
            message: msg::Header::new(),
        }
    }

    pub(in crate::listener) fn header(self: pin::Pin<&Self>) -> &[u8] {
        self.project_ref().header.as_slice()
    }

    fn header_mut(self: pin::Pin<&mut Self>) -> &mut [u8] {
        self.project().header.as_mut_slice()
    }

    pub(in crate::listener) fn begin(self: pin::Pin<&mut Self>, source: Option<data::Buffer<'d>>) {
        let this = self.project();
        debug_assert!(this.source.is_none());
        *this.source = source;
    }

    fn clear(self: pin::Pin<&mut Self>) {
        *self.project().source = None;
    }
}

impl<const ID: u8> recycle::Recycle for Flight<'_, ID> {
    fn recycle(self: pin::Pin<&mut Self>) {
        self.clear();
    }
}

pub(super) struct Arena<'d, const ID: u8> {
    pub(super) direct: recycle::Pool<Flight<'d, ID>>,
    pub(super) headers: recycle::Pool<HeaderStorage<'d, ID>>,
}

struct DirectSource<'d, const ID: u8> {
    pool: ptr::NonNull<recycle::Pool<Flight<'d, ID>>>,
    owner: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

struct HeaderSource<'d, const ID: u8> {
    pool: ptr::NonNull<recycle::Pool<HeaderStorage<'d, ID>>>,
    owner: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy)]
pub(in crate::listener) struct Retention<'a, 'd, const ID: u8> {
    arena: &'a Arena<'d, ID>,
}

impl<'a, 'd, const ID: u8> Retention<'a, 'd, ID> {
    pub(super) const fn new(arena: &'a Arena<'d, ID>) -> Self {
        Self { arena }
    }

    pub(super) fn reborrow(&self) -> Retention<'_, 'd, ID> {
        Retention { arena: self.arena }
    }

    pub(in crate::listener) fn reserve(
        &self,
        queued: bool,
        queue: egress::Queue<'a, 'd, { listener::IOV_CAP }, Payload<'d, ID>>,
    ) -> Option<Buffer<'a, 'd, ID>> {
        if !queued
            && let Some(flight) = recycle::Pool::reserve_owned(DirectSource {
                pool: ptr::NonNull::from(&self.arena.direct),
                owner: marker::PhantomData,
            })
        {
            return Some(Buffer::Direct(DirectSlot(flight)));
        }
        Some(Buffer::Queued {
            slot: HeaderSlot(recycle::Pool::reserve_owned(HeaderSource {
                pool: ptr::NonNull::from(&self.arena.headers),
                owner: marker::PhantomData,
            })?),
            queue,
        })
    }
}

pub(in crate::listener) struct HeaderSlot<'d, const ID: u8>(
    recycle::Reservation<'d, HeaderStorage<'d, ID>>,
);

pub(in crate::listener) struct DirectSlot<'d, const ID: u8>(
    recycle::Reservation<'d, Flight<'d, ID>>,
);

#[repr(transparent)]
pub(in crate::listener) struct DirectLease<'d, const ID: u8>(recycle::Lease<'d, Flight<'d, ID>>);

impl<'d, const ID: u8> DirectLease<'d, ID> {
    pub(in crate::listener) fn begin(&mut self, source: Option<data::Buffer<'d>>) {
        self.0.get_mut().begin(source);
    }

    pub(in crate::listener) fn flight_mut(&mut self) -> pin::Pin<&mut Flight<'d, ID>> {
        self.0.get_mut()
    }
}

pub(in crate::listener) enum Buffer<'a, 'd, const ID: u8> {
    Direct(DirectSlot<'d, ID>),
    Queued {
        slot: HeaderSlot<'d, ID>,
        queue: egress::Queue<'a, 'd, { listener::IOV_CAP }, Payload<'d, ID>>,
    },
}

impl<const ID: u8> Buffer<'_, '_, ID> {
    pub(in crate::listener) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Direct(flight) => flight.get().header(),
            Self::Queued { slot, .. } => slot.get().as_slice(),
        }
    }

    pub(in crate::listener) fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Direct(flight) => flight.get_mut().header_mut(),
            Self::Queued { slot, .. } => slot.get_mut().as_mut_slice(),
        }
    }
}

pub struct Header<'d, const ID: u8> {
    pub(super) slot: recycle::Lease<'d, HeaderStorage<'d, ID>>,
    pub(super) len: num::NonZeroUsize,
}

impl<const ID: u8> AsRef<[u8]> for Header<'_, ID> {
    fn as_ref(&self) -> &[u8] {
        &self.slot.get().as_slice()[..self.len.get()]
    }
}

impl<'d, const ID: u8> Header<'d, ID> {
    pub(in crate::listener) fn new(
        slot: recycle::Lease<'d, HeaderStorage<'d, ID>>,
        len: num::NonZeroUsize,
    ) -> Self {
        Self { slot, len }
    }
}

pub enum Payload<'d, const ID: u8> {
    Header(Header<'d, ID>),
    Body(data::Buffer<'d>),
}

impl<const ID: u8> AsRef<[u8]> for Payload<'_, ID> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Header(header) => header.as_ref(),
            Self::Body(body) => body.as_ref(),
        }
    }
}

impl<'d, const ID: u8> HeaderSlot<'d, ID> {
    pub(in crate::listener) fn get(&self) -> pin::Pin<&HeaderStorage<'d, ID>> {
        self.0.get()
    }

    pub(in crate::listener) fn get_mut(&mut self) -> pin::Pin<&mut HeaderStorage<'d, ID>> {
        self.0.get_mut()
    }

    pub(in crate::listener) fn commit(self) -> recycle::Lease<'d, HeaderStorage<'d, ID>> {
        self.0.commit()
    }
}

impl<'d, const ID: u8> DirectSlot<'d, ID> {
    pub(in crate::listener) fn get(&self) -> pin::Pin<&Flight<'d, ID>> {
        self.0.get()
    }

    pub(in crate::listener) fn get_mut(&mut self) -> pin::Pin<&mut Flight<'d, ID>> {
        self.0.get_mut()
    }

    pub(in crate::listener) fn commit(self) -> DirectLease<'d, ID> {
        DirectLease(self.0.commit())
    }
}

const _: () = {
    assert!(mem::size_of::<HeaderStorage<'static, 0>>() == WRITE_BUF_CAP);
    assert!(mem::align_of::<HeaderStorage<'static, 0>>() == mem::align_of::<u8>());
    assert!(mem::size_of::<Arena<'static, 0>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Arena<'static, 0>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Retention<'static, 'static, 0>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Header<'static, 0>>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Payload<'static, 0>>()
            == mem::size_of::<data::Either<Header<'static, 0>, data::Buffer<'static>>>()
    );
    assert!(
        mem::align_of::<Payload<'static, 0>>()
            == mem::align_of::<data::Either<Header<'static, 0>, data::Buffer<'static>>>()
    );
};
