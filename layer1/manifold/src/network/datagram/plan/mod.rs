use std::{marker, mem, num};

use dope_core::driver::{
    self,
    retained::flight,
    route::{self, Identity, kind::Recv, table},
    settings::{self, claim, demands, errors},
};
use o3::buffer;

use crate::{
    app::creation,
    network::datagram::{self, packet, send, socket::sender},
};

pub(in crate::network::datagram) mod bounds;

pub(in crate::network::datagram) enum Invalid {
    Zero,
    Route(table::Invalid),
}

#[derive(Clone, Copy)]
pub(super) enum Retention {
    Bytes(usize),
    Slots(num::NonZero<usize>),
}

impl Retention {
    fn resolve(self, receive: settings::Receive) -> Result<usize, crate::ConfigError> {
        match self {
            Self::Bytes(bytes) => Ok(bytes),
            Self::Slots(slots) => {
                let bytes = receive.buffer_len().widen();
                slots
                    .checked_mul(bytes)
                    .map(num::NonZero::get)
                    .ok_or(crate::ConfigError::DatagramReceiveCapacity { slots, bytes })
            }
        }
    }

    const fn is_empty(self) -> bool {
        matches!(self, Self::Bytes(0))
    }
}

/// Validated bounds for one datagram endpoint.
#[derive(Clone, Copy)]
pub struct Config {
    pending_sends: bounds::Pending,
    retained_send_bytes: usize,
    in_flight_sends: bounds::Flights,
    retained_receive: Retention,
}

impl Config {
    /// Validates progress-capable pending and in-flight bounds.
    pub fn new(pending: usize, bytes: usize, flight: usize) -> Result<Self, crate::ConfigError> {
        let pending = bounds::Pending::try_from(pending).map_err(|error| match error {
            Invalid::Zero => crate::ConfigError::DatagramPendingZero,
            Invalid::Route(cause) => crate::ConfigError::DatagramPendingCapacity(cause),
        })?;
        let flight = bounds::Flights::try_from(flight).map_err(|error| match error {
            Invalid::Zero => crate::ConfigError::DatagramFlightZero,
            Invalid::Route(cause) => crate::ConfigError::DatagramFlightCapacity(cause),
        })?;
        Ok(Self {
            pending_sends: pending,
            retained_send_bytes: bytes,
            in_flight_sends: flight,
            retained_receive: Retention::Bytes(0),
        })
    }

    /// Bounds driver receive storage retained beyond its dispatch turn.
    pub fn retain_recv(mut self, bytes: usize) -> Self {
        self.retained_receive = Retention::Bytes(bytes);
        self
    }

    /// Bounds retained packets using the final shared driver's physical slot size.
    pub fn retain_slots(mut self, slots: num::NonZero<usize>) -> Self {
        self.retained_receive = Retention::Slots(slots);
        self
    }

    const fn claim(&self, claim: claim::Claim) -> claim::Claim {
        claim
            .files(demands::Files::only_out(demands::Outbound::fixed::<1>()))
            .domains(self.domains())
            .schedule(demands::Schedule::fixed::<1, 1>())
    }

    pub(super) const fn domains(&self) -> settings::Domain {
        match (
            self.retained_send_bytes == 0,
            self.retained_receive.is_empty(),
        ) {
            (true, true) => settings::Domain::ZERO,
            (false, false) => settings::Domain::fixed::<2>(),
            (true, false) | (false, true) => settings::Domain::ONE,
        }
    }
}

/// Single-use startup authority for one datagram route.
#[must_use = "a datagram plan must be declared and consumed by one endpoint"]
pub struct Plan<ID: Identity, M: send::Mode = send::Plain> {
    config: Config,
    route: crate::Family<(ID, M)>,
}

impl<ID: Identity, M: send::Mode> Plan<ID, M> {
    pub const fn new(config: Config) -> Self {
        Self {
            config,
            route: marker::PhantomData,
        }
    }

    /// Exact base claim for callers that extend this route's resources.
    pub const fn claim(&self) -> claim::Claim {
        let claim = claim::Claim::new(settings::Routes::singleton::<ID>());
        self.config.claim(claim)
    }

    pub fn prepare<'d, B: buffer::Residency + buffer::raw::Source>(
        self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<datagram::Prepared<'d, ID, B, M>, creation::Error> {
        self.prepare_for(flight::Capacity::fixed::<1>(), driver)
    }

    pub(in crate::network::datagram) fn prepare_for<
        'd,
        B: buffer::Residency + buffer::raw::Source,
    >(
        self,
        receive: flight::Capacity,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<datagram::Prepared<'d, ID, B, M>, creation::Error> {
        let config = self.config;
        let retained = config
            .retained_receive
            .resolve(driver.receive())
            .map_err(creation::Error::Config)?;
        let budget = datagram::Prepared::<ID, B, M>::budget(config.retained_send_bytes, driver)?;
        let retain =
            packet::Retainer::new(datagram::Prepared::<ID, B, M>::budget(retained, driver)?);
        let sender = sender::Sender::try_new(
            config.pending_sends,
            budget,
            config.in_flight_sends,
            driver.receive().buffer_len().widen(),
            driver.driver_ref().targets(),
        )
        .map_err(creation::Error::Allocation)?;
        let send = flight::Need::new(flight::Capacity::table(config.in_flight_sends.into_table()));
        let recv = flight::Need::<<ID as route::Materialize<Recv>>::With>::new(receive);
        Ok(datagram::Prepared {
            sender,
            flights: recv.pair(send),
            retention: retain,
        })
    }
}

impl<ID: Identity, M: send::Mode> settings::Resource for Plan<ID, M> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.claim())
    }
}

const _: () = {
    assert!(mem::size_of::<Plan<route::KeyTag<0>>>() == mem::size_of::<Config>());
    assert!(mem::size_of::<Plan<route::KeyTag<0>, send::Sourced>>() == mem::size_of::<Config>());
    assert!(!mem::needs_drop::<Plan<route::KeyTag<0>>>());
};
