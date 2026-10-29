use dope_core::driver::route::Identity;
use dope_net::link::egress;
use egress::{load::Value, reserve::error};

use crate::client::conn;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Plan {
    Lanes {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    Entries {
        actual: u32,
        minimum: u32,
    },
}

/// Failure to enqueue one owned payload on an exact connection generation.
#[must_use]
pub enum Enqueue<B> {
    Unknown(egress::Load<B>),
    Inactive(egress::Load<B>),
    Rejected(egress::Rejected<Value<B>>),
}

/// A proved payload retained with its rejected connection generation.
#[must_use]
pub enum Unsent<'d, B, ID: Identity> {
    Unknown {
        connection: conn::Id<'d, ID>,
        load: egress::NonEmpty<B>,
    },
    Inactive {
        connection: conn::Id<'d, ID>,
        load: egress::NonEmpty<B>,
    },
    Capacity {
        connection: conn::Id<'d, ID>,
        failure: error::Capacity<B>,
    },
}

/// Failure to bind one physical transport generation to a source lane.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Open {
    Unknown,
    Occupied,
    Exhausted,
}

/// Failure to order write-half shutdown for one exact connection generation.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Shutdown {
    Unknown,
    Inactive,
}

const _: () = {
    assert!(
        std::mem::size_of::<Enqueue<()>>() == std::mem::size_of::<egress::Rejected<Value<()>>>()
    );
    assert!(std::mem::size_of::<Open>() == 1);
    assert!(std::mem::size_of::<Result<(), Open>>() == 1);
    assert!(std::mem::size_of::<Shutdown>() == 1);
    assert!(std::mem::size_of::<Result<(), Shutdown>>() == 1);
};
