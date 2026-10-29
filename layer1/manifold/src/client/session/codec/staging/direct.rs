use std::num;

use dope_core::driver::{route::table::conn, settings::Domain, storage};
use o3::buffer::resident::{self, staging};

use crate::client::session::{
    codec::staging::{Empty, Policy, Seal},
    ingress,
};

/// No incremental storage. The codec must consume each direct input completely.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Direct;

impl Seal for Direct {
    const DOMAINS: Domain = Domain::ZERO;

    type Layout = ();
    type Pool<'d> = Self;
    type Buf<'d> = Empty<'d>;

    fn layout(
        _: Self,
        _: conn::Capacity,
        _: ingress::Buffers,
        _: ingress::Bytes,
    ) -> Result<(), staging::Error> {
        Ok(())
    }

    fn resident(_: ()) -> Option<storage::Resident> {
        None
    }

    fn warm(_: Self) -> Option<num::NonZero<u32>> {
        None
    }

    fn pool<'d>(_: &resident::Budget<'d>, _: (), _: bool) -> Result<Self, resident::AllocError> {
        Ok(Self)
    }
}

impl Policy for Direct {
    type Spec = Self;
    type Plan = Self;

    fn plan(spec: Self) -> Result<Self, staging::Error> {
        Ok(spec)
    }

    fn limit(_: Self::Plan) -> super::Limit {
        super::Limit(super::Bound::Direct)
    }
}

const _: () = assert!(size_of::<Direct>() == 0);
