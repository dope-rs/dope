use dope_core::driver::{self, route::table::conn};
use o3::buffer::resident;
use session::{
    codec::staging::{Policy, Seal},
    ingress,
};

use crate::{app::creation, client::session};

pub(super) struct Ingress<'d, P: Seal> {
    budget: resident::Budget<'d>,
    pool: <P as Seal>::Pool<'d>,
}

impl<'d, P: Policy> Ingress<'d, P> {
    pub(super) fn try_new<S>(
        limits: ingress::Plan<S>,
        cap: conn::Capacity,
        spec: P::Spec,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Self, creation::Error>
    where
        S: ingress::Source<'d>,
    {
        let plan = P::plan(spec).map_err(creation::Error::Ingress)?;
        let warm = <P as Seal>::warm(plan);
        let shared = warm.map_or(ingress::Buffers::EMPTY, |warm| limits.shared(warm));
        let reserve = limits.reserve();
        let layout = <P as Seal>::layout(plan, cap, shared, ingress::Bytes::EMPTY)
            .map_err(creation::Error::Ingress)?;
        let output = limits
            .acquire(reserve, driver)
            .map_err(creation::Error::Resident)?;
        let stage = match <P as Seal>::resident(layout) {
            Some(resident) => driver
                .storage
                .budget(resident)
                .map_err(creation::Error::Resident)?,
            None => output,
        };
        let pool = <P as Seal>::pool(&stage, layout, limits.is_accounted())
            .map_err(creation::Error::IngressStorage)?;
        Ok(Self {
            budget: output,
            pool,
        })
    }

    pub(super) const fn budget(&self) -> &resident::Budget<'d> {
        &self.budget
    }

    pub(super) const fn pool(&self) -> &<P as Seal>::Pool<'d> {
        &self.pool
    }
}
