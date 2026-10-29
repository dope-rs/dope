use std::{marker, num};

use data::compose;
use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::{app, ready},
};
use dope_net::link::egress::{
    self, data,
    metadata::{self, arena},
};
use o3::{alloc, base::cell::region};

use crate::client::{app::request, attempt, close, conn, port::write::Reserve};

pub mod batch;
mod branches;
pub mod error;
mod receiver;
mod retire;
mod sender;
mod state;
pub mod write;

pub use branches::Branches;
use receiver::Core;
pub use sender::Sender;

/// A connector-side request port bound to one exact driver route.
///
/// A connection from another route cannot enter this port.
pub struct Port<'e, 'd, B, ID: Identity = KeyTag<0>> {
    core: Core<'e, 'd, B, ID>,
}

/// Construction-time retained staging policy for one payload type.
pub struct Plan<B> {
    lanes: metadata::LaneCount,
    retained: egress::Config,
    payload: marker::PhantomData<fn(B) -> B>,
}

impl<B> Clone for Plan<B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<B> Copy for Plan<B> {}

impl<B> Plan<B> {
    pub const fn new(lanes: usize, retained: egress::Config) -> Result<Self, error::Plan> {
        let Some(lanes) = num::NonZero::<usize>::new(lanes) else {
            return Err(error::Plan::Lanes {
                actual: 0,
                minimum: 1,
                maximum: metadata::Count::MAXIMUM,
            });
        };
        let lanes = match metadata::LaneCount::try_new(lanes) {
            Ok(lanes) => lanes,
            Err(cause) => {
                return Err(error::Plan::Lanes {
                    actual: cause.actual().get(),
                    minimum: 1,
                    maximum: metadata::Count::MAXIMUM,
                });
            }
        };
        if retained.entries().is_empty() {
            return Err(error::Plan::Entries {
                actual: 0,
                minimum: 1,
            });
        }
        Ok(Self {
            lanes,
            retained,
            payload: marker::PhantomData,
        })
    }

    /// Builds a runtime-lane plan from already proven shared capacity.
    pub fn from_shared(
        lanes: num::NonZero<usize>,
        entries: num::NonZero<u32>,
        bytes: u32,
    ) -> Result<Self, metadata::Count> {
        let lanes = metadata::LaneCount::try_new(lanes)?;
        Ok(Self {
            lanes,
            retained: egress::Config::shared(egress::Credit::new(
                egress::Entries::new(entries.get()),
                egress::Resident::new(bytes),
            )),
            payload: marker::PhantomData,
        })
    }

    /// Builds a shared retained arena with one entry per compile-time lane.
    pub fn shared<const LANES: usize>(bytes: u32) -> Self {
        let lanes = metadata::LaneCount::fixed::<LANES>();
        Self {
            lanes,
            retained: egress::Config::shared(egress::Credit::new(
                egress::Entries::new(LANES as u32),
                egress::Resident::new(bytes),
            )),
            payload: marker::PhantomData,
        }
    }

    pub const fn capacity(&self) -> metadata::LaneCount {
        self.lanes
    }

    pub const fn retained(&self) -> egress::Config {
        self.retained
    }
}

impl<'e, 'd, B: data::raw::Payload<'d>, ID: Identity> Port<'e, 'd, B, ID> {
    pub fn try_new(
        store: &'e mut arena::Store<'d>,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            core: Core::try_new(store, token)?,
        })
    }

    pub fn capacity(&self) -> metadata::LaneCount {
        self.core.capacity()
    }

    /// Exact retained-credit policy encoded by the live arena.
    pub fn config(&self) -> egress::Config {
        self.core.config()
    }

    pub fn is_active(&self, conn: conn::Id<'d, ID>) -> bool {
        self.core.entry(conn).is_some()
    }

    /// Reports payloads or write shutdown still awaiting final-link admission.
    /// Native completion is reported separately by the application's send callback.
    /// An inactive connection generation has no pending state.
    pub fn pending(&self, conn: conn::Id<'d, ID>) -> Option<egress::Pending> {
        let receiver = self.core.entry(conn)?;
        let slot = receiver.slot;
        Some(
            if !slot.queue().is_empty() || matches!(slot.state().signal(), state::Signal::Shutdown)
            {
                egress::Pending::Present
            } else {
                egress::Pending::Absent
            },
        )
    }

    pub fn with_sender<R>(
        &self,
        conn: conn::Id<'d, ID>,
        f: impl for<'a> FnOnce(Sender<'a, 'e, 'd, B, ID>) -> R,
    ) -> Option<R> {
        let receiver = self.core.entry(conn)?;
        let (slot, proof) = (receiver.slot, receiver.txn);
        Some(f(Sender::new(slot, proof)))
    }

    /// Applies port admission to the engine-owned activation.
    pub fn try_activate(&self, active: &conn::Activation<'_, 'd, ID>) -> Result<(), error::Open> {
        self.core.try_activate(active.connection(), active.ready())
    }

    pub fn retire<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        work: app::Work<'turn, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
        cancel: impl FnOnce(B),
    ) -> egress::Cleared {
        self.core.retire(conn, work, region, cancel)
    }

    pub fn try_enqueue(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        value: B,
    ) -> Result<(), error::Enqueue<B>> {
        self.core.try_admit(region, conn, egress::Load::new(value))
    }

    pub fn try_reserve<'a, 'token>(
        &'a self,
        region: &'token mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        value: B,
    ) -> Reserve<'a, 'token, 'e, 'd, B, ID>
    where
        B: data::raw::Resident<'d>,
    {
        self.core.try_reserve(region, conn, value)
    }

    pub fn close(&self, conn: conn::Id<'d, ID>, reason: close::Reason) {
        self.core.close(conn, reason);
    }

    /// Orders write-half shutdown after all payloads already admitted for this
    /// exact connection generation.
    pub fn finish_write(&self, conn: conn::Id<'d, ID>) -> Result<(), error::Shutdown> {
        self.core.finish_write(conn)
    }

    pub fn drain<'p>(&self, requests: &mut request::Requests<'_, '_, 'p, 'd, B, ID>) {
        self.core.drain(requests.connection(), requests);
    }

    pub fn try_drain<'p, E>(
        &self,
        requests: &mut request::Requests<'_, '_, 'p, 'd, B, ID>,
        begin: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        let Some(receiver) = self.core.entry(requests.connection()) else {
            return Ok(());
        };
        let (slot, proof) = (receiver.slot, receiver.txn);
        let close = receiver.take_close();
        let result = if close.is_none() {
            receiver.try_drain(requests, begin)
        } else {
            Ok(())
        };
        Core::finish_drain(slot, proof, close, requests);
        result
    }

    /// Drains this generation and retains its admission proof when capacity moved.
    pub fn drain_ready<'p, 'a>(
        &'a self,
        requests: &mut request::Requests<'_, '_, 'p, 'd, B, ID>,
    ) -> Option<Sender<'a, 'e, 'd, B, ID>> {
        let receiver = self.core.entry(requests.connection())?;
        let (slot, proof) = (receiver.slot, receiver.txn);
        let close = receiver.take_close();
        let progress = if close.is_none() {
            receiver.drain(requests)
        } else {
            receiver::Progress::Idle
        };
        Core::finish_drain(slot, proof, close, requests);
        match progress {
            receiver::Progress::Idle => None,
            receiver::Progress::Moved => Some(Sender::new(slot, proof)),
        }
    }
}

impl<'e, 'd, B: data::raw::Payload<'d>, K: Copy, ID: Identity>
    Port<'e, 'd, compose::Tagged<B, K>, ID>
{
    /// Validates allocation-free source metadata and strips it before the
    /// payload enters the final link queue. The payload owner is moved once;
    /// no bytes are copied and final egress retains the original `B` layout.
    pub fn drain_tagged<'p>(
        &self,
        requests: &mut request::Requests<'_, '_, 'p, 'd, B, ID>,
        validate: impl FnMut(&mut region::Token<region::Identity<'d>>, K) -> bool,
        reject: impl FnMut(compose::Tagged<B, K>),
    ) {
        self.core.drain_map::<B, K, ID>(
            requests.connection(),
            requests,
            |value| *value.tag(),
            validate,
            reject,
        );
    }
}

const _: () = {
    assert!(std::mem::size_of::<Result<(), error::Open>>() == std::mem::size_of::<error::Open>());
    assert!(
        std::mem::size_of::<conn::Activation<'_, 'static, KeyTag<0>>>()
            == std::mem::size_of::<attempt::Id<'static, KeyTag<0>>>()
                + std::mem::size_of::<conn::Id<'static, KeyTag<0>>>()
                + std::mem::size_of::<ready::Target<'static>>()
    );
};
