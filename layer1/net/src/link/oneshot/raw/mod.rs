mod cell;

pub(in crate::link::oneshot) use cell::Cell;
use dope_core::{
    driver::{result, retained, route::Identity},
    io::{
        event::net::{connect, send, socket::Completion},
        socket,
    },
};
use o3::depot::{self, arena::slab::external::errors};

use crate::link::{
    egress::data,
    oneshot::{
        Pool, Progress,
        entry::{Access, Entry},
    },
};

/// Retained operations on one exact installed pool and its driver turn.
pub struct Io<'a, 'turn, 'owner, 'd: 'owner, ID: Identity, A: 'd, P: socket::Prepared, B: 'd>
where
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    pool: &'a mut Pool<'d, ID, A, P, B>,
    driver: &'a mut retained::Context<'turn, 'owner, 'd>,
}

impl<'a, 'turn, 'owner, 'd: 'owner, ID: Identity, A: 'd, P: socket::Prepared, B: 'd>
    Io<'a, 'turn, 'owner, 'd, ID, A, P, B>
where
    B: data::raw::Payload<'d> + data::raw::StableOwner<'d>,
{
    /// # Safety
    /// This pool belongs to `driver`'s installed owner; retain its fixed backing,
    /// addresses and payloads until completion or quiescence, beyond this access.
    pub unsafe fn new(
        pool: &'a mut Pool<'d, ID, A, P, B>,
        driver: &'a mut retained::Context<'turn, 'owner, 'd>,
    ) -> Self {
        Self { pool, driver }
    }

    pub fn socket(&mut self, done: Completion<'d>) -> Progress<A> {
        let local = match self.pool.routes.parse(done.token()) {
            Ok(local) => local,
            Err(_) => {
                done.reclaim();
                return Progress::Idle;
            }
        };
        // SAFETY: this Pool just admitted the completion's own physical segment.
        let access =
            Access::new(unsafe { depot::raw::Proof::new((&mut *self.pool, local.index())) });
        match Entry::try_new(access, local.parts().epoch()) {
            Ok(entry) => entry.socket(done, self.driver),
            Err(errors::Lookup::Physical { .. } | errors::Lookup::Generation { .. }) => {
                done.reclaim();
                Progress::Idle
            }
        }
    }

    pub fn connect(
        &mut self,
        done: connect::Completion<'d>,
    ) -> Result<Progress<A>, connect::Completion<'d>> {
        let local = match self.pool.routes.parse(done.token()) {
            Ok(local) => local,
            Err(_) => {
                return Err(done);
            }
        };
        // SAFETY: this Pool just admitted the completion's own physical segment.
        let access =
            Access::new(unsafe { depot::raw::Proof::new((&mut *self.pool, local.index())) });
        match Entry::try_new(access, local.parts().epoch()) {
            Ok(entry) => entry.connect(done, self.driver),
            Err(errors::Lookup::Physical { .. } | errors::Lookup::Generation { .. }) => Err(done),
        }
    }

    pub fn send(
        &mut self,
        done: send::Completion<'d>,
    ) -> Result<Progress<A>, send::Completion<'d>> {
        let local = match self.pool.routes.parse(done.token()) {
            Ok(local) => local,
            Err(_) => {
                return Err(done);
            }
        };
        // SAFETY: this Pool just admitted the completion's own physical segment.
        let access =
            Access::new(unsafe { depot::raw::Proof::new((&mut *self.pool, local.index())) });
        match Entry::try_new(access, local.parts().epoch()) {
            Ok(entry) => entry.send(done, self.driver),
            Err(errors::Lookup::Physical { .. } | errors::Lookup::Generation { .. }) => Err(done),
        }
    }

    /// Retries one operation; a full submission queue retains it for a later turn.
    pub fn retry_one(&mut self) -> Result<Progress<A>, result::SubmitError> {
        let Some(local) = self.pool.retries.pop_front() else {
            return Ok(Progress::Idle);
        };
        // SAFETY: this exact Pool's retry queue retained the admitted position.
        let access =
            Access::new(unsafe { depot::raw::Proof::new((&mut *self.pool, local.index())) });
        match Entry::try_new(access, local.parts().epoch()) {
            Ok(entry) => entry.retry(self.driver),
            Err(errors::Lookup::Physical { .. } | errors::Lookup::Generation { .. }) => {
                Ok(Progress::Idle)
            }
        }
    }
}
