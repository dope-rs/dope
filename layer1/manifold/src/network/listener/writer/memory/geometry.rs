use std::num;

use data::exec;
use dope_core::{driver::route::table::conn, io::transfer};
use dope_net::link::egress::{self, data, reserve};
use reserve::demand;

use crate::network::listener::{config, writer::memory};

type Setup<M> = demand::Setup<<M as exec::Mode>::Reserve<{ memory::WRITE_BUF_CAP }>>;

pub(in crate::network::listener) struct Geometry<M: exec::Mode> {
    setup: Setup<M>,
    direct: Option<num::NonZero<u32>>,
    headers: Option<num::NonZero<u32>>,
}

impl<M: exec::Mode> Geometry<M> {
    pub(super) fn into_parts(
        self,
    ) -> (
        Setup<M>,
        Option<num::NonZero<u32>>,
        Option<num::NonZero<u32>>,
    ) {
        (self.setup, self.direct, self.headers)
    }

    pub(in crate::network::listener) fn resolve(
        queue: egress::Config,
        policy: config::Flights,
        count: conn::Capacity,
    ) -> Result<Self, crate::ConfigError> {
        let provided =
            <M::Reserve<{ memory::WRITE_BUF_CAP }> as demand::Mode>::provision(queue, count);
        let direct = match policy {
            config::Flights::Minimum => match &provided {
                demand::Provision::Queued { .. } => None,
                demand::Provision::Direct(_) => M::DIRECT_MINIMUM,
            },
            config::Flights::Exact(flights) => {
                let maximum = count.entries().get();
                if flights > maximum {
                    return Err(crate::ConfigError::ListenerWriteFlights {
                        actual: flights,
                        maximum,
                    });
                }
                num::NonZero::<u32>::new(flights as u32)
            }
            config::Flights::PerConnection => Some(count.count()),
        };
        match provided {
            demand::Provision::Queued { setup, headers } => Ok(Self {
                setup,
                direct,
                headers: Some(headers),
            }),
            demand::Provision::Direct(setup) => {
                if direct.is_none() {
                    match M::EXECUTION {
                        data::Execution::Fallible => {}
                        data::Execution::Bounded(body) => {
                            return Err(crate::ConfigError::ListenerBoundedEgress { body });
                        }
                        data::Execution::Unbounded => {
                            return Err(crate::ConfigError::ListenerUnboundedEgress);
                        }
                    }
                }
                Ok(Self {
                    setup,
                    direct,
                    headers: match M::EXECUTION {
                        data::Execution::Fallible => queue
                            .chunks(transfer::NonZero::fixed_size::<{ memory::WRITE_BUF_CAP }>()),
                        data::Execution::Bounded(_) | data::Execution::Unbounded => None,
                    },
                })
            }
        }
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Geometry<exec::Fallible>>()
            == std::mem::size_of::<demand::Setup<demand::Fallible>>() + std::mem::size_of::<u64>()
    );
};
