use dope::runtime::civil;
use lookup::{create, refresh};

use crate::{Target, lookup, storage::literal};

/// Driver-branded state for IP-literal discovery.
pub struct State<'d> {
    inner: literal::Plan,
    clock: civil::Anchor<'d>,
}

impl<'d> State<'d> {
    pub(crate) fn new(inner: literal::Plan, clock: civil::Anchor<'d>) -> Self {
        Self { inner, clock }
    }

    pub fn discovery<P: refresh::Policy, const LANES: usize, const ADDRESSES: usize>(
        &self,
        target: Target,
    ) -> Result<lookup::Discovery<'_, '_, 'd, P, LANES, ADDRESSES>, create::Error> {
        let (host, port) = target.into_parts();
        let crate::Host::Address(address) = host else {
            return Err(create::Error::LiteralName(Target { host, port }));
        };
        Ok(lookup::Discovery::literal(
            address,
            port,
            &self.inner.refresh,
            self.clock,
        ))
    }
}
