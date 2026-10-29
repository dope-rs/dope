use std::marker;

use dope_core::{
    driver::route::{Identity, table::conn},
    io::transfer,
};
use o3::buffer::hold::limits;

use crate::wire;

/// Resources retained for the full lifetime of one wire route.
#[repr(transparent)]
pub struct Owner<W: wire::Spec, ID: Identity> {
    storage: W::Storage<ID>,
}

impl<W: wire::Spec, ID: Identity> Owner<W, ID> {
    pub const fn new(storage: W::Storage<ID>) -> Self {
        Self { storage }
    }

    pub fn try_new(capacity: conn::Capacity) -> crate::Result<Self>
    where
        W::Bootstrap: Allocate<W>,
    {
        <W::Bootstrap as Allocate<W>>::try_storage::<ID>(capacity).map(Self::new)
    }

    pub fn bind<'d, P>(&'d self, plan: P) -> Result<Endpoint<'d, W, ID>, P::Error>
    where
        W: 'd,
        ID: 'd,
        P: Plan<W, ID>,
    {
        plan.bind(&self.storage).map(Endpoint::new)
    }
}

/// Converts an owning route plan into a binding branded by its storage owner.
pub trait Plan<W: wire::Spec, ID: Identity> {
    type Error;

    fn bind<'d>(self, storage: &'d W::Storage<ID>) -> Result<W::Binding<'d, ID>, Self::Error>
    where
        W: 'd,
        ID: 'd;
}

/// Storage-independent binding for a static wire runtime.
#[repr(transparent)]
pub struct Local<'w, W: wire::Spec + 'w, ID: Identity + 'w> {
    binding: W::Binding<'w, ID>,
}

impl<'w, W: wire::Spec + 'w, ID: Identity + 'w> Local<'w, W, ID> {
    pub const fn new(binding: W::Binding<'w, ID>) -> Self {
        Self { binding }
    }

    pub(crate) fn init<'d>(self, limits: Limits) -> W::Runtime<'w, 'd, ID>
    where
        W::Bootstrap: Init<W>,
        'd: 'w,
    {
        <W::Bootstrap as Init<W>>::runtime::<'w, 'd, ID>(limits, self.binding)
    }
}

/// A binding which retains the exact lifetime of its wire storage owner.
#[repr(transparent)]
pub struct Endpoint<'w, W: wire::Spec + 'w, ID: Identity + 'w> {
    binding: W::Binding<'w, ID>,
    owner: marker::PhantomData<&'w W::Storage<ID>>,
}

impl<'w, W: wire::Spec + 'w, ID: Identity + 'w> Endpoint<'w, W, ID> {
    fn new(binding: W::Binding<'w, ID>) -> Self {
        Self {
            binding,
            owner: marker::PhantomData,
        }
    }

    pub(crate) fn try_init<'d>(self, limits: Limits) -> crate::Result<W::Runtime<'w, 'd, ID>>
    where
        W::Bootstrap: Bootstrap<W>,
        'd: 'w,
    {
        <W::Bootstrap as Bootstrap<W>>::try_runtime::<'w, 'd, ID>(limits, self.binding)
    }
}

/// Allocates route storage before its driver domain exists.
pub trait Allocate<W: wire::Spec> {
    fn try_storage<ID: Identity>(capacity: conn::Capacity) -> crate::Result<W::Storage<ID>>;
}

/// Fallible runtime construction from an exact owner-bound wire binding.
pub trait Bootstrap<W: wire::Spec> {
    fn try_runtime<'w, 'd, ID: Identity + 'w>(
        limits: Limits,
        binding: W::Binding<'w, ID>,
    ) -> crate::Result<W::Runtime<'w, 'd, ID>>
    where
        W: 'w,
        'd: 'w;
}

/// Infallible runtime construction from an exact wire binding.
pub trait Init<W: wire::Spec> {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        limits: Limits,
        binding: W::Binding<'w, ID>,
    ) -> W::Runtime<'w, 'd, ID>
    where
        W: 'w,
        'd: 'w;
}

/// Bounds shared by connection, receive-retention, and receive-buffer owners.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    connections: conn::Capacity,
    receive: Receive,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Receive {
    retained: Retained,
    resident: Resident,
    len: transfer::NonZero,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Retained(usize);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Resident(usize);

impl Limits {
    pub(crate) const fn new(
        cap: conn::Capacity,
        retained: Retained,
        resident: Resident,
        len: transfer::NonZero,
    ) -> Self {
        Self {
            connections: cap,
            receive: Receive {
                retained,
                resident,
                len,
            },
        }
    }

    pub const fn fixed<
        const CONNECTIONS: usize,
        const RETAINED: usize,
        const RESIDENT: usize,
        const RECEIVE_LEN: u32,
    >() -> Self {
        Self {
            connections: conn::Capacity::fixed::<CONNECTIONS>(),
            receive: Receive {
                retained: Retained(RETAINED),
                resident: Resident(RESIDENT),
                len: transfer::NonZero::fixed::<RECEIVE_LEN>(),
            },
        }
    }

    pub const fn connections(self) -> conn::Capacity {
        self.connections
    }

    pub const fn retained(self) -> Retained {
        self.receive.retained
    }

    pub const fn resident(self) -> Resident {
        self.receive.resident
    }

    pub const fn receive_len(self) -> transfer::NonZero {
        self.receive.len
    }
}

impl Retained {
    pub const ZERO: Self = Self(0);

    /// Lowers this exact retained-byte limit at the allocation boundary.
    pub const fn into_raw(self) -> usize {
        self.0
    }
}

impl Resident {
    pub const ZERO: Self = Self(0);

    pub const fn from_limit(limit: limits::Bound) -> Self {
        Self(limit.bytes())
    }

    /// Lowers this exact resident-byte limit at the allocation boundary.
    pub const fn into_raw(self) -> usize {
        self.0
    }
}

impl From<usize> for Retained {
    fn from(count: usize) -> Self {
        Self(count)
    }
}

impl From<usize> for Resident {
    fn from(bytes: usize) -> Self {
        Self(bytes)
    }
}

const _: () = assert!(std::mem::size_of::<Limits>() == 4 * std::mem::size_of::<usize>());
