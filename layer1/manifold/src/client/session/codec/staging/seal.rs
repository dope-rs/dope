use std::num;

use dope_core::driver::{route::table::conn, settings::Domain, storage};
use o3::{
    buffer::resident::{self, staging},
    mem::rc,
};

use crate::client::session::{
    codec::staging::{Buffer, Policy},
    ingress,
};

pub trait Seal {
    const DOMAINS: Domain;

    type Layout: Copy;
    type Pool<'d>;
    type Buf<'d>: Buffer<'d> + for<'a> TryFrom<&'a Self::Pool<'d>, Error = rc::Error>;

    fn layout(
        plan: <Self as Policy>::Plan,
        count: conn::Capacity,
        buffers: ingress::Buffers,
        reserve: ingress::Bytes,
    ) -> Result<Self::Layout, staging::Error>
    where
        Self: Policy;

    fn resident(layout: Self::Layout) -> Option<storage::Resident>;

    fn warm(plan: <Self as Policy>::Plan) -> Option<num::NonZero<u32>>
    where
        Self: Policy;

    fn pool<'d>(
        budget: &resident::Budget<'d>,
        layout: Self::Layout,
        track: bool,
    ) -> Result<Self::Pool<'d>, resident::AllocError>;
}
