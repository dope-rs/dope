use std::num;

use dope_core::driver::{route::table::conn, settings::Domain, storage};
use o3::buffer::resident::{self, staging};

use crate::client::session::{
    codec::staging::{Policy, Seal},
    ingress,
};

/// Precharged warm storage with bounded resident spill.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Buffered;

impl Seal for Buffered {
    const DOMAINS: Domain = Domain::ONE;

    type Layout = staging::Layout;
    type Pool<'d> = staging::Pool<'d>;
    type Buf<'d> = staging::Buf<'d>;

    fn layout(
        plan: staging::Plan,
        count: conn::Capacity,
        shared: ingress::Buffers,
        reserve: ingress::Bytes,
    ) -> Result<Self::Layout, staging::Error> {
        staging::Layout::new(
            count.entries().get(),
            plan,
            shared.into_raw(),
            reserve.into_raw(),
        )
    }

    fn resident(layout: Self::Layout) -> Option<storage::Resident> {
        Some(storage::Resident::bytes(layout.resident().get()))
    }

    fn warm(plan: staging::Plan) -> Option<num::NonZero<u32>> {
        Some(plan.warm())
    }

    fn pool<'d>(
        budget: &resident::Budget<'d>,
        layout: Self::Layout,
        track: bool,
    ) -> Result<Self::Pool<'d>, resident::AllocError> {
        if track {
            staging::Pool::try_account(budget, layout)
        } else {
            staging::Pool::try_new(budget, layout)
        }
    }
}

impl Policy for Buffered {
    type Spec = Result<staging::Plan, staging::Error>;
    type Plan = staging::Plan;

    fn plan(spec: Self::Spec) -> Result<Self::Plan, staging::Error> {
        spec
    }

    fn limit(plan: Self::Plan) -> super::Limit {
        super::Limit(super::Bound::Buffered(plan.ceiling()))
    }
}

const _: () = assert!(size_of::<Buffered>() == 0);
