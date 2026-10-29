use dope_core::driver::{
    route::Identity,
    schedule::{app, ready},
};
use dope_net::link::egress::{
    self, data,
    metadata::{self, arena, front},
    reserve::error::Capacity,
};
use o3::{alloc, base::cell::region};

use crate::{
    app::dispatch::{identity, identity::Identity as _},
    client::{
        app::request,
        close, conn,
        port::{Reserve, error, retire, state, write::Hold},
    },
};

mod head;
mod pause;

pub(super) enum Progress {
    Idle,
    Moved,
}

pub(super) struct Receiver<'a, 'e, 'd, B, I: identity::Identity> {
    pub(super) slot: arena::Slot<'a, 'e, 'd, B, state::Entry<'d, I>>,
    pub(super) txn: state::Transaction<I>,
}

impl<'a, 'e, 'd, S: data::raw::Payload<'d>, I: identity::Identity> Receiver<'a, 'e, 'd, S, I> {
    fn reject<'p, B: data::raw::Payload<'d>, ID: Identity>(
        requests: &mut request::Requests<'_, '_, 'p, 'd, B, ID>,
        cause: request::Capacity,
    ) {
        let cause = match cause {
            request::Capacity::Waiting => return,
            request::Capacity::Rejected(cause) => cause,
        };
        requests.close(close::Directive::cut_retry(close::Reason::Egress(cause)));
    }

    fn advance<'p, ID: Identity>(
        pause: &pause::Pause<'_, 'e, 'd, S, I>,
        requests: &mut request::Requests<'_, '_, 'p, 'd, S, ID>,
    ) -> Progress
    where
        for<'queue, 'token> front::Front<'queue, 'token, 'e, 'd, S>: front::Transfer<'d, S>,
    {
        let Some(head) = pause.front() else {
            return Progress::Idle;
        };
        let request::Front::Ready(work) = requests.admit_front() else {
            return Progress::Idle;
        };
        match work.move_with(|token, queue, target| head.transfer_to(token, queue, target)) {
            Some(Ok(())) => Progress::Moved,
            Some(Err(cause)) => {
                Self::reject(requests, cause);
                Progress::Idle
            }
            None => Progress::Idle,
        }
    }

    pub(super) fn drain<'p, ID: Identity>(
        &self,
        requests: &mut request::Requests<'_, '_, 'p, 'd, S, ID>,
    ) -> Progress
    where
        for<'queue, 'token> front::Front<'queue, 'token, 'e, 'd, S>: front::Transfer<'d, S>,
    {
        let Some(pause) = pause::Pause::begin(self.slot, self.txn) else {
            return Progress::Idle;
        };
        let mut progress = Progress::Idle;
        while let Progress::Moved = Self::advance(&pause, requests) {
            progress = Progress::Moved;
            if !pause.is_current() {
                break;
            }
        }
        progress
    }

    pub(super) fn try_drain<'p, E, ID: Identity>(
        &self,
        requests: &mut request::Requests<'_, '_, 'p, 'd, S, ID>,
        mut begin: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E>
    where
        for<'queue, 'token> front::Front<'queue, 'token, 'e, 'd, S>: front::Transfer<'d, S>,
    {
        let Some(pause) = pause::Pause::begin(self.slot, self.txn) else {
            return Ok(());
        };
        while let Progress::Moved = Self::advance(&pause, requests) {
            begin()?;
            if !pause.is_current() {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn drain_map<'p, B, K, ID: Identity>(
        &self,
        requests: &mut request::Requests<'_, '_, 'p, 'd, B, ID>,
        mut classify: impl FnMut(&S) -> K,
        mut validate: impl FnMut(&mut region::Token<region::Identity<'d>>, K) -> bool,
        mut reject: impl FnMut(S),
    ) where
        B: data::raw::Payload<'d>,
        K: Copy,
        for<'queue, 'token> front::Front<'queue, 'token, 'e, 'd, S>: front::Transfer<'d, B>,
    {
        let Some(pause) = pause::Pause::begin(self.slot, self.txn) else {
            return;
        };
        loop {
            let Some(head) = pause.head(requests.region(), &mut classify) else {
                return;
            };
            let mut work = match requests.admit_front() {
                request::Front::Ready(work) => work,
                request::Front::Exhausted => {
                    return;
                }
            };
            let valid = work.with_region(|region| validate(region, head.kind()));
            if valid {
                let moved =
                    work.move_with(|token, queue, target| head.transfer_to(token, queue, target));
                match moved {
                    Some(Ok(())) => {}
                    Some(Err(cause)) => {
                        Self::reject::<B, ID>(requests, cause);
                        return;
                    }
                    None => return,
                }
            } else {
                let rejected =
                    work.with_region(|token| head.take(token).map(front::Front::into_inner));
                let Some(rejected) = rejected else {
                    return;
                };
                reject(rejected);
            }
            if !pause.is_current() {
                return;
            }
        }
    }

    pub(super) fn take_close(&self) -> Option<close::Reason> {
        self.slot.state().take_close(self.txn)
    }
}

pub(super) struct Core<'e, 'd, B, ID: Identity> {
    pub(super) arena: arena::Arena<'e, 'd, B, state::Entry<'d, conn::Id<'d, ID>>>,
}

impl<'e, 'd, B: data::raw::Payload<'d>, ID: Identity> Core<'e, 'd, B, ID> {
    pub(super) fn try_new(
        store: &'e mut arena::Store<'d>,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        use arena::Arena;
        Ok(Self {
            arena: Arena::try_new(token, store)?,
        })
    }

    pub(super) fn capacity(&self) -> metadata::LaneCount {
        self.arena.len()
    }

    pub(super) fn config(&self) -> egress::Config {
        self.arena.config()
    }

    pub(super) fn entry(
        &self,
        conn: conn::Id<'d, ID>,
    ) -> Option<Receiver<'_, 'e, 'd, B, conn::Id<'d, ID>>> {
        let slot = self.arena.get(metadata::Lane::from_slot(conn.slot()))?;
        Some(Receiver {
            slot,
            txn: slot.state().transaction(conn)?,
        })
    }

    pub(super) fn try_activate(
        &self,
        conn: conn::Id<'d, ID>,
        ready: ready::Target<'d>,
    ) -> Result<(), error::Open> {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return Err(error::Open::Unknown);
        };
        slot.state().try_activate(conn, ready)
    }

    pub(super) fn retire<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        work: app::Work<'turn, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
        cancel: impl FnOnce(B),
    ) -> egress::Cleared {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return egress::Cleared::Done;
        };
        let retire = match retire::Retire::begin(slot, conn, region) {
            Ok(Some(retire)) => retire,
            Ok(None) => return egress::Cleared::Done,
            Err(state::Busy) => return egress::Cleared::Retry,
        };
        retire.clear(work, cancel)
    }

    /// Owns one exact payload before protocol state is touched. Validation is
    /// a separate consuming type transition.
    pub(super) fn try_reserve<'a, 'token>(
        &'a self,
        region: &'token mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        value: B,
    ) -> Reserve<'a, 'token, 'e, 'd, B, ID>
    where
        B: data::raw::Resident<'d>,
    {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return Reserve::Unknown(value);
        };
        let entry = slot.state();
        let Some(proof) = entry.writable(conn) else {
            return Reserve::Inactive(value);
        };
        let load = match egress::Load::new(value).try_nonempty() {
            Ok(load) => load,
            Err(rejected) => {
                return match egress::Empty::try_from(rejected) {
                    Ok(empty) => Reserve::Empty(empty),
                    Err(rejected) => Reserve::Invalid(rejected),
                };
            }
        };
        let source = match slot.queue().try_reserve(region, load) {
            metadata::Reservation::Ready(source) => source,
            metadata::Reservation::Rejected(error) => return Reserve::Rejected(error),
        };
        Reserve::Item(Hold::new(entry, proof, source))
    }

    pub(super) fn try_admit(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        load: egress::Load<B>,
    ) -> Result<(), error::Enqueue<B>> {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return Err(error::Enqueue::Unknown(load));
        };
        let entry = slot.state();
        let Some(proof) = entry.writable(conn) else {
            return Err(error::Enqueue::Inactive(load));
        };
        match slot.queue().admit(region, load) {
            metadata::Admission::Committed => {}
            metadata::Admission::Rejected(rejected) => {
                return Err(error::Enqueue::Rejected(rejected));
            }
        }
        entry.mark_ready(proof);
        Ok(())
    }

    pub(super) fn try_project<S: data::raw::Payload<'d>>(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        load: egress::NonEmpty<S>,
        project: impl FnOnce(
            metadata::Queue<'_, 'e, 'd, B>,
            &mut region::Token<region::Identity<'d>>,
            egress::NonEmpty<S>,
        ) -> Result<(), Capacity<S>>,
    ) -> Result<(), error::Unsent<'d, S, ID>> {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return Err(error::Unsent::Unknown {
                connection: conn,
                load,
            });
        };
        let entry = slot.state();
        let Some(proof) = entry.writable(conn) else {
            return Err(error::Unsent::Inactive {
                connection: conn,
                load,
            });
        };
        project(slot.queue(), region, load).map_err(|failure| error::Unsent::Capacity {
            connection: conn,
            failure,
        })?;
        entry.mark_ready(proof);
        Ok(())
    }

    pub(super) fn close(&self, conn: conn::Id<'d, ID>, reason: close::Reason) {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return;
        };
        slot.state().close(conn, reason);
    }

    pub(super) fn finish_write(&self, conn: conn::Id<'d, ID>) -> Result<(), error::Shutdown> {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return Err(error::Shutdown::Unknown);
        };
        slot.state().shutdown(conn)
    }

    pub(super) fn finish_drain<'p, O: data::raw::Payload<'d>, ROUTE: Identity>(
        slot: arena::Slot<'_, 'e, 'd, B, state::Entry<'d, conn::Id<'d, ID>>>,
        proof: state::Transaction<conn::Id<'d, ID>>,
        close: Option<close::Reason>,
        requests: &mut request::Requests<'_, '_, 'p, 'd, O, ROUTE>,
    ) {
        if let Some(reason) = close {
            requests.close(close::Directive::reconnect(reason));
            return;
        }
        if !slot.queue().is_empty() {
            return;
        }
        match slot.state().take_signal(proof) {
            state::Signal::Shutdown => requests.finish_write(),
            state::Signal::Clear => {}
        }
    }

    pub(super) fn drain_map<'p, O, K, ROUTE: Identity>(
        &self,
        conn: conn::Id<'d, ID>,
        requests: &mut request::Requests<'_, '_, 'p, 'd, O, ROUTE>,
        classify: impl FnMut(&B) -> K,
        validate: impl FnMut(&mut region::Token<region::Identity<'d>>, K) -> bool,
        reject: impl FnMut(B),
    ) where
        O: data::raw::Payload<'d>,
        K: Copy,
        for<'queue, 'token> front::Front<'queue, 'token, 'e, 'd, B>: front::Transfer<'d, O>,
    {
        let Some(receiver) = self.entry(conn) else {
            return;
        };
        let (slot, proof) = (receiver.slot, receiver.txn);
        let close = receiver.take_close();
        if close.is_none() {
            receiver.drain_map::<O, K, ROUTE>(requests, classify, validate, reject);
        }
        Self::finish_drain(slot, proof, close, requests);
    }

    pub(super) fn drain<'p, ROUTE: Identity>(
        &self,
        conn: conn::Id<'d, ID>,
        requests: &mut request::Requests<'_, '_, 'p, 'd, B, ROUTE>,
    ) {
        let Some(receiver) = self.entry(conn) else {
            return;
        };
        let (slot, proof) = (receiver.slot, receiver.txn);
        let close = receiver.take_close();
        if close.is_none() {
            receiver.drain(requests);
        }
        Self::finish_drain(slot, proof, close, requests);
    }
}
