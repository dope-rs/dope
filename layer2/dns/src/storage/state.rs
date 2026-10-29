use std::time;

use dope::{core::driver::schedule::clock, runtime::civil};
use lookup::{create, refresh};
use o3::base::cell::region;
use storage::{
    peer,
    queries::{self, index},
};

use crate::{Target, lookup, storage};

/// Driver-owned DNS state built from a [`storage::Plan`].
pub struct State<'s, 'd, const LANES: usize, const ADDRESSES: usize> {
    pub(crate) queries: queries::Queries<'s, 'd, LANES, ADDRESSES>,
    pub(in crate::storage) clock: civil::Anchor<'d>,
}

impl<'s, 'd, const LANES: usize, const ADDRESSES: usize> State<'s, 'd, LANES, ADDRESSES> {
    pub(crate) fn peer(&self, index: index::Server) -> peer::Peer<'_, 's, 'd, LANES, ADDRESSES> {
        peer::Peer {
            index,
            owner: &self.queries,
        }
    }

    pub(crate) fn apply<'state, R>(
        &'state self,
        now: clock::Moment<'d>,
        apply: impl FnOnce(&'state queries::Queries<'s, 'd, LANES, ADDRESSES>, time::Instant) -> R,
    ) -> R {
        now.at(&self.queries).apply(apply)
    }

    pub(crate) fn discovery<P: refresh::Policy>(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        target: Target,
    ) -> Result<lookup::Discovery<'_, 's, 'd, P, LANES, ADDRESSES>, create::Error> {
        let (host, port) = target.into_parts();
        match host {
            crate::Host::Address(address) => Ok(lookup::Discovery::<P, LANES, ADDRESSES>::literal(
                address,
                port,
                &self.queries.config.refresh,
                self.clock,
            )),
            crate::Host::Name(name) => {
                let Some(lane) = self.queries.claim(token) else {
                    return Err(create::Error::LaneCapacity {
                        target: Target {
                            host: crate::Host::Name(name),
                            port,
                        },
                        capacity: LANES,
                    });
                };
                Ok(lookup::Discovery::<P, LANES, ADDRESSES>::query(
                    name,
                    port,
                    lane,
                    &self.queries.config.refresh,
                    self.clock,
                ))
            }
        }
    }
}
