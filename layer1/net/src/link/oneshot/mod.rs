use std::time;

use dope_core::{
    driver,
    driver::{
        life::finish,
        result,
        retained::flight,
        route::{Identity, indices, table, table::bounds},
        schedule::clock,
    },
    io::{fd::outbound, socket, socket::option},
};
use entry::{Access, Entry};
pub use error::Error;
use o3::{
    base::time::Positive,
    depot,
    depot::{
        arena::slab::{external::errors, lookup},
        heap::keyed,
        queue::indexed::{exact, fifo},
    },
};
pub use request::Request;

use crate::link::egress::data;

mod error;
mod request;

/// The caller's original reason for stopping one send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cancellation {
    Cancelled,
    ConnectTimeout { late_by: time::Duration },
    SendTimeout { late_by: time::Duration },
}

pub enum Progress<A> {
    Idle,
    Pending,
    Complete {
        authority: A,
        result: Result<(), Error>,
    },
}

#[must_use = "the exact unsent request must be retried or settled"]
pub enum Unsent<R> {
    Busy {
        request: R,
        cause: Busy,
    },
    Rejected {
        request: R,
        cause: result::SubmitError,
    },
    Outside {
        request: R,
        cause: bounds::Excluded,
    },
    Exhausted(R, errors::Exhausted),
}

pub enum Busy {
    Slot,
    Descriptor,
    Submit(result::SubmitError),
}

#[must_use = "the send outcome must be settled"]
pub enum Dispatch<R> {
    Sent,
    Unsent(Unsent<R>),
}

pub struct Parts<'d, A, P, B> {
    pub address: P,
    pub options: option::Set,
    pub authority: A,
    pub payload: B,
    pub connect_deadline: clock::Deadline<'d>,
    pub send_window: Positive,
}

mod entry;
mod phase;
pub mod raw;
mod routes;
mod states;

pub struct Pool<'d, ID: Identity, A: 'd, Address: socket::Prepared, B: 'd>
where
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    routes: routes::Routes<'d, ID>,
    flights: flight::Slots<'d, ID>,
    slots: table::CellSlab<raw::Cell<'d, A, Address, B>, ID>,
    retries: fifo::List<routes::Local<'d, ID>, indices::SlotIndex, exact::Policy>,
    deadlines: keyed::Min<routes::Index, routes::Due<'d>>,
}

impl<'d, ID: Identity, A: 'd, Address, B: 'd> Pool<'d, ID, A, Address, B>
where
    Address: socket::Prepared,
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    pub fn prepare(
        base: indices::Edge,
        cap: table::Capacity,
        driver: &mut driver::Context<'_, 'd>,
    ) -> crate::Result<Self> {
        let routes = routes::Routes::try_new(base, cap, driver)?;
        let slots = table::CellSlab::try_new(cap).map_err(crate::Resource::Allocation)?;
        let retries = fifo::List::try_count(cap).map_err(crate::Resource::Allocation)?;
        let times = keyed::Min::try_new(cap.count()).map_err(crate::Resource::Heap)?;
        let flights = driver
            .storage
            .reborrow()
            .try_flights(flight::Need::<ID>::new(flight::Capacity::table(cap)))
            .map_err(crate::Resource::Allocation)?
            .slots();
        Ok(Self {
            routes,
            flights,
            slots,
            retries,
            deadlines: times,
        })
    }
    pub fn try_send(
        &mut self,
        fds: &outbound::Reserved<'d, ID>,
        local: indices::SlotIndex,
        req: Request<'d, A, Address, B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Dispatch<Request<'d, A, Address, B>> {
        let access = match self.try_access(local) {
            Ok(access) => access,
            Err(cause) => {
                return Dispatch::Unsent(Unsent::Outside {
                    request: req,
                    cause,
                });
            }
        };
        access.send(fds, req, driver)
    }

    pub fn try_cancel(
        &mut self,
        local: indices::SlotIndex,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Progress<A>, bounds::Excluded> {
        let access = self.try_access(local)?;
        Ok(Self::cancel_at(access, driver))
    }

    /// Cancels the next position in this pool's own fixed segment.
    /// The cursor is an iterator offset; arbitrary or exhausted offsets end the scan.
    pub fn cancel_next(
        &mut self,
        cursor: &mut u32,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<Progress<A>> {
        let access = self.next_access(cursor)?;
        Some(Self::cancel_at(access, driver))
    }

    /// Cancels the retained owner, if the exclusive scan can access one.
    fn cancel_at(
        access: Access<'_, 'd, ID, A, Address, B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress<A> {
        match Entry::try_at(access) {
            Ok(entry) => entry.cancel(Cancellation::Cancelled, driver),
            Err(
                lookup::Error::Busy { .. }
                | lookup::Error::Free { .. }
                | lookup::Error::Reserved { .. }
                | lookup::Error::Index { .. }
                | lookup::Error::Retired { .. },
            ) => Progress::Idle,
        }
    }

    fn try_access(
        &mut self,
        local: indices::SlotIndex,
    ) -> Result<Access<'_, 'd, ID, A, Address, B>, bounds::Excluded> {
        let index = self.routes.try_index(local)?;
        // SAFETY: this admission pairs the selected index with its original Pool.
        Ok(Access::new(unsafe {
            depot::raw::Proof::new((self, index))
        }))
    }

    fn next_access(&mut self, cursor: &mut u32) -> Option<Access<'_, 'd, ID, A, Address, B>> {
        let index = self.routes.indices().nth(*cursor as usize)?;
        *cursor += 1;
        // SAFETY: this Pool's own segment iterator produces the admitted position.
        Some(Access::new(unsafe {
            depot::raw::Proof::new((self, index))
        }))
    }

    pub fn earliest(&self) -> Option<clock::Deadline<'d>> {
        Some(self.deadlines.peek()?.1.at())
    }

    pub fn expire_one(
        &mut self,
        now: clock::Moment<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress<A> {
        let Some(entry) = self.deadlines.first_entry() else {
            return Progress::Idle;
        };
        if entry.priority().at() > now {
            return Progress::Idle;
        }
        let index = entry.key();
        let due = entry.remove();
        // SAFETY: this exact Pool's heap retained the admitted physical position.
        let access = Access::new(unsafe { depot::raw::Proof::new((self, index)) });
        match Entry::try_new(access, due.epoch()) {
            Ok(entry) => entry.expire(now, due, driver),
            Err(errors::Lookup::Physical { .. } | errors::Lookup::Generation { .. }) => {
                Progress::Idle
            }
        }
    }

    pub fn has_retries(&self) -> bool {
        !self.retries.is_empty()
    }

    pub fn has_io(&self) -> bool {
        !self.slots.is_empty()
    }

    /// Releases retained owners after this driver's kernel operations quiesce.
    pub fn finish(&mut self, _: &mut finish::Finish<'_, 'd>, mut release: impl FnMut(A)) {
        let mut cursor = 0;
        while let Some(access) = self.next_access(&mut cursor) {
            if let Some(owner) = access.finish() {
                release(owner);
            }
        }
    }
}
