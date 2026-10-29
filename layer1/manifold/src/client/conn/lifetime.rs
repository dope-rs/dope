use std::mem;

use dope_core::driver::{
    route,
    route::{Identity, KeyTag},
    schedule::clock,
};

use crate::client::conn;

/// Exact transport generation and its connector-owned absolute retirement.
///
/// Driver brands cannot be widened.
/// ```compile_fail
/// use dope_core::driver::route::KeyTag;
/// use dope_manifold::client::conn;
///
/// fn widen<'d>(
///     lifetime: conn::Lifetime<'d, KeyTag<0>>,
/// ) -> conn::Lifetime<'static, KeyTag<0>> {
///     lifetime
/// }
/// ```
///
/// Connector routes cannot be crossed.
#[derive(PartialEq, Eq)]
#[must_use = "connection lifetime must be bound to its application generation"]
pub struct Lifetime<'d, ID: Identity> {
    connection: conn::Id<'d, ID>,
    expires_at: Option<clock::Deadline<'d>>,
}

impl<'d, ID: Identity> Lifetime<'d, ID> {
    pub(in crate::client) const fn new(
        id: conn::Id<'d, ID>,
        expiry: Option<clock::Deadline<'d>>,
    ) -> Self {
        Self {
            connection: id,
            expires_at: expiry,
        }
    }

    /// Returns the exact physical generation owning this lifetime.
    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.connection
    }

    /// Projects one exact connection identity into its route generation.
    pub const fn generation(id: conn::Id<'d, ID>) -> route::Epoch {
        id.0.epoch()
    }

    /// Returns the timer's absolute deadline, or `None` for a persistent connection.
    pub const fn expires_at(&self) -> Option<clock::Deadline<'d>> {
        self.expires_at
    }
}

const _: () = assert!(
    mem::size_of::<Lifetime<'static, KeyTag<0>>>()
        == mem::size_of::<(
            conn::Id<'static, KeyTag<0>>,
            Option<clock::Deadline<'static>>,
        )>()
);
