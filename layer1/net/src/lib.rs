//! Ownership and lifetime contracts.
//!
//! Reservation cannot widen the lifetime of the protocol owner it borrows.
//! Read access alone does not prove immutable payload geometry.

use dope_core::io::fd::outbound;
use o3::{
    alloc,
    buffer::pool::{GrowingError, LayoutError},
    depot::{heap, queue::pool},
};

pub mod link;
pub mod tcp;
pub mod wire;

use std::{net, result};

use dope_core::{
    driver::{
        self,
        retained::flight,
        route::table::{self, bounds},
    },
    io::{
        fd::handles,
        socket::{self, install::Error, option},
    },
};

use crate::link::egress::create;

/// Failure to construct retained network resources before a route is bound.

#[derive(Debug)]
pub enum Resource {
    Config(ConfigError),
    Descriptor(outbound::Error),
    Binding(BindingError),
    BufferLayout(BufferLayout),
    Pool(GrowingError),
    Allocation(alloc::Error),
    Queue(alloc::Error),
    Heap(heap::Error),
    Exhausted(alloc::Exhausted),
    Credit(alloc::Error),
    Egress(create::Error),
}

type Result<T> = result::Result<T, Resource>;

/// Construction input which violates a relationship not already encoded by
/// its component types.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConfigError {
    ReceiveLayout(pool::Error),
    ReceiveCapacity {
        entries: usize,
        lanes: usize,
    },
    RearmCapacity {
        entries: usize,
        lanes: usize,
    },
    PhysicalBelowLogical {
        logical: table::Capacity,
        physical: table::Capacity,
    },
    RouteSegment(bounds::SegmentError),
    FlightCapacity(flight::ScaleError),
}

/// Failure while binding a network resource to driver ownership.

#[derive(PartialEq, Eq, Debug)]
pub enum BindingError {
    AlreadyBound,
    Bootstrap(Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum BufferKind {
    Receive,
    Send,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BufferLayout {
    kind: BufferKind,
    cause: LayoutError,
}

impl BufferLayout {
    pub const fn new(kind: BufferKind, cause: LayoutError) -> Self {
        Self { kind, cause }
    }

    pub const fn kind(self) -> BufferKind {
        self.kind
    }

    pub const fn cause(self) -> LayoutError {
        self.cause
    }
}

pub trait Transport: Sized {
    type Addr;
    /// Retained kernel storage converted from an already validated address.
    type Prepared: socket::Prepared + for<'a> From<&'a Self::Addr>;
    type StreamConfig: Default + Copy;

    /// Resolves stream tuning into an owned, allocation-free kernel plan.
    fn options(config: Self::StreamConfig) -> option::Set;
}

pub trait Listen: Transport {
    type ListenerConfig: Default + Clone;
    type BoundListener;

    fn bind<'d>(
        driver: &mut driver::Context<'_, 'd>,
        addr: &Self::Addr,
        cap: socket::Backlog,
        config: &Self::ListenerConfig,
    ) -> result::Result<(handles::Descriptor<'d>, net::SocketAddr), BindingError>;

    fn adopt<'d>(
        driver: &mut driver::Context<'_, 'd>,
        bound: Self::BoundListener,
    ) -> result::Result<(handles::Descriptor<'d>, net::SocketAddr), BindingError>;
}
