use std::marker;

use dope_core::driver::{
    route::{self, Identity, table::conn},
    settings::{self, claim, demands, errors},
};

use crate::{
    client::side,
    network::client::topology::plan::{Input, Plan, Side, Spec, Standard},
};

/// Startup-only physical resource layout for one connector route.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Layout<ID: Identity, const MAX: usize, M = Standard, A = super::Local> {
    capacities: side::Capacities,
    types: crate::Family<(M, A, ID)>,
}

impl<ID: Identity, const MAX: usize, M, A: Input> Layout<ID, MAX, M, A> {
    fn capacity(count: usize) -> Result<conn::Capacity, errors::Claim> {
        if count == 0 || count > MAX {
            return Err(errors::Claim::DescriptorConnectionsOutOfRange {
                actual: count,
                minimum: 1,
                maximum: MAX,
            });
        }
        Ok(conn::Capacity::try_from(count)?)
    }

    fn claim_with(&self, files: demands::Outbound) -> Result<claim::Claim, errors::Claim> {
        Ok(claim::Claim::new(settings::Routes::singleton::<ID>())
            .files(demands::Files::only_out(files))
            .domains(A::DOMAINS.map_err(errors::Claim::ResidentDomainsOverflow)?)
            .schedule(demands::Schedule::fixed::<1, 3>()))
    }

    pub const fn connections(&self) -> conn::Capacity {
        self.capacities.primary()
    }

    pub(crate) const fn capacities(&self) -> side::Capacities {
        self.capacities
    }

    pub const fn into_plan(self, spec: Spec) -> Plan<ID, MAX, M, A> {
        Plan {
            layout: self,
            state: spec,
        }
    }
}

impl<ID: Identity, const MAX: usize, A: Input> Layout<ID, MAX, Standard, A> {
    pub fn new(count: usize) -> Result<Self, errors::Claim> {
        Ok(Self {
            capacities: side::Capacities::standard(Self::capacity(count)?),
            types: marker::PhantomData,
        })
    }

    pub fn claim(&self) -> Result<claim::Claim, errors::Claim> {
        self.claim_with(demands::Outbound::scale::<1>(self.connections()))
    }
}

impl<ID: Identity, const MAX: usize, A: Input> Layout<ID, MAX, Side, A> {
    pub(super) fn try_new(count: usize) -> Result<Self, errors::Claim> {
        let primary = Self::capacity(count)?;
        Ok(Self {
            capacities: side::Capacity::try_new(primary)?.into_inner(),
            types: marker::PhantomData,
        })
    }

    pub fn from_side(capacity: side::Capacity) -> Self {
        const { assert!(MAX >= route::SLOT_MASK as usize) };
        Self {
            capacities: capacity.into_inner(),
            types: marker::PhantomData,
        }
    }

    fn claim(&self) -> Result<claim::Claim, errors::Claim> {
        self.claim_with(demands::Outbound::scale::<2>(self.connections()))
    }
}

impl<ID: Identity, const MAX: usize, A: Input> settings::Resource for Layout<ID, MAX, Standard, A> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.claim()?)
    }
}

impl<ID: Identity, const MAX: usize, A: Input> settings::Resource for Layout<ID, MAX, Side, A> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.claim()?)
    }
}
