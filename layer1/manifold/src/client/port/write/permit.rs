use std::mem;

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::egress::{data, metadata};
use metadata::reserve;
use o3::base::cell::region;

use crate::client::{conn, port::state};

/// A source permit whose connection generation has been validated.
#[must_use = "a source permit must be committed or rolled back"]
pub struct Permit<'a, 'token, 'e, 'd, B, ID: Identity = KeyTag<0>> {
    entry: &'a state::Entry<'d, conn::Id<'d, ID>>,
    transaction: state::Transaction<conn::Id<'d, ID>>,
    source: reserve::Reserve<'a, 'token, 'e, 'd, B>,
}

impl<'a, 'token, 'e, 'd, B, ID: Identity> Permit<'a, 'token, 'e, 'd, B, ID> {
    pub(super) const fn new(
        entry: &'a state::Entry<'d, conn::Id<'d, ID>>,
        proof: state::Transaction<conn::Id<'d, ID>>,
        source: reserve::Reserve<'a, 'token, 'e, 'd, B>,
    ) -> Self {
        Self {
            entry,
            transaction: proof,
            source,
        }
    }
}

impl<'e, 'd, B: data::raw::Resident<'d>, ID: Identity> Permit<'_, '_, 'e, 'd, B, ID> {
    pub fn commit<R>(
        self,
        acquire: impl for<'a> FnOnce(&'a mut region::Token<region::Identity<'d>>) -> R,
    ) -> R {
        let Self {
            entry,
            transaction: proof,
            source,
        } = self;
        entry.publish(proof, || source.commit(acquire))
    }

    pub fn try_commit<R, E>(
        self,
        acquire: impl for<'a> FnOnce(&'a mut region::Token<region::Identity<'d>>) -> Result<R, E>,
    ) -> Result<R, reserve::Failure<B, E>> {
        let Self {
            entry,
            transaction: proof,
            source,
        } = self;
        entry.publish(proof, || source.try_commit(acquire))
    }

    pub fn try_with<R, E>(
        self,
        acquire: impl for<'a> FnOnce(&'a mut region::Token<region::Identity<'d>>, &B) -> Result<R, E>,
    ) -> Result<R, reserve::Failure<B, E>> {
        let Self {
            entry,
            transaction: proof,
            source,
        } = self;
        entry.publish(proof, || source.try_with(acquire))
    }
}

const _: () = assert!(
    mem::size_of::<Permit<'static, 'static, 'static, 'static, (), KeyTag<0>>>()
        == mem::size_of::<&state::Entry<'static, conn::Id<'static, KeyTag<0>>>>()
            + mem::size_of::<state::Transaction<conn::Id<'static, KeyTag<0>>>>()
            + mem::size_of::<reserve::Reserve<'static, 'static, 'static, 'static, ()>>()
);
