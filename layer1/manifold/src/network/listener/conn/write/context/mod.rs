use std::{
    convert::{self, Infallible},
    mem,
};

use compose::Either;
use conn::write;
use dope_core::driver::{
    self,
    route::{Identity, KeyTag},
};
use dope_net::{link::egress, wire};
use egress::{
    data::{self, Bound, compose},
    reserve::exact,
};
use o3::{
    base::cell::region,
    buffer::{resident, resident::bound},
};
use write::{
    body::{Admit, Failure},
    error::Error,
};

use crate::network::listener::{conn, writer};

mod modes;
mod send;

use send::{body, capacity};

/// Affine access to one callback-scoped listener write transaction.
pub struct Context<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B>
where
    B: data::raw::Payload<'d>,
{
    connection: &'turn mut conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>,
}

impl<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B>
    Context<'e, 'turn, 'a, 'p, 's, 'w, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    pub(in crate::network::listener) const fn new(
        scope: &'turn mut conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>,
    ) -> Self {
        Self { connection: scope }
    }

    /// Acquires the turn once, then carries its lease through native reservation and commit.
    fn try_send<R, E, P, const CANCEL: bool>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        prepare: impl FnOnce(
            &egress::Queue<
                'a,
                'e,
                'd,
                { crate::network::listener::IOV_CAP },
                writer::Payload<'p, 's, 'd, ID, B>,
                writer::Reserve<'d, B>,
            >,
        ) -> Option<P>,
        commit: impl FnOnce(
            send::Header<'turn, 'a, 'p, 's, 'w, 'e, 'd, ID, W, C, B, P>,
            &mut driver::Context<'_, 'd>,
        ) -> Result<write::Outcome<R>, Error<B, R, E>>,
    ) -> Result<write::Outcome<R>, Error<B, R, E>> {
        let Some(lease) = self.connection.work.lease() else {
            self.connection.slot.io().wake_target().wake();
            return Ok(write::Outcome::Deferred(write::Deferred::Turn));
        };
        let slot = &mut self.connection.slot;
        let conn::Output::Open {
            slots,
            permit,
            held,
            queue,
        } = &mut self.connection.output
        else {
            return Ok(write::Outcome::Closed);
        };
        let buffer = match capacity::header::<_, _, _, _, _, CANCEL>(
            held,
            queue,
            slot,
            driver.region_token(),
            prepare,
        ) {
            Ok(buffer) => buffer,
            Err(capacity::Unavailable::Closed) => return Ok(write::Outcome::Closed),
            Err(capacity::Unavailable::Deferred(cause)) => {
                slot.ensure_send(driver.driver_ref().clock().now());
                return Ok(write::Outcome::Deferred(cause));
            }
            Err(capacity::Unavailable::Rejected(cause)) => {
                return Err(Error::Capacity(cause));
            }
        };
        commit(
            send::Header {
                slot,
                flights: slots,
                pending: *permit,
                buffer,
                lease,
            },
            driver,
        )
    }

    pub fn try_bounded<R, F>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<B, R, Infallible>>
    where
        F: for<'b, 'id> FnOnce(
            &mut C,
            write::Prefix<'b, 'id>,
        )
            -> write::Commit<region::Identity<'id>, egress::load::Value<B>, R>,
        B: 'd,
    {
        self.try_send::<_, _, _, true>(
            driver,
            |_| Some(()),
            |send, driver| send.execute(driver, commit),
        )
    }

    /// Reserves the exact prefix owner and queue capacity before calling the application.
    /// The uninhabited body type prevents the callback from adding unreserved egress.
    pub fn try_prefix<R, F>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<B, R, Infallible>>
    where
        F: for<'b, 'id> FnOnce(
            &mut C,
            write::Prefix<'b, 'id>,
        ) -> write::Commit<
            region::Identity<'id>,
            egress::load::Value<Infallible>,
            R,
        >,
        B: 'd,
    {
        self.try_send::<_, _, _, false>(
            driver,
            |_| Some(()),
            |send, driver| send.try_prefix(driver, commit),
        )
    }

    pub fn admit(self, load: egress::NonEmpty<B>, driver: &mut driver::Context<'_, 'd>) -> Admit<B>
    where
        B: 'd,
    {
        body::send(
            load,
            self.connection,
            driver,
            exact::Prepared::right,
            convert::identity,
        )
    }

    pub fn pending(&self) -> egress::Pending {
        self.connection.output.pending(&self.connection.slot)
    }

    pub fn reject(self, cause: Failure) {
        self.connection.slot.state.fail(conn::Failure::Write(cause));
        self.connection.slot.close_after();
    }
}

impl<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, L, R>
    Context<'e, 'turn, 'a, 'p, 's, 'w, 'd, ID, W, C, Either<L, R>>
where
    W: wire::Spec + 'w + 'd,
    L: data::raw::Payload<'d>,
    R: data::raw::Payload<'d>,
{
    pub fn admit_left(
        self,
        load: egress::NonEmpty<L>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Admit<L>
    where
        L: 'd,
        R: 'd,
    {
        body::send(
            load,
            self.connection,
            driver,
            exact::Prepared::right_left,
            egress::NonEmpty::into_left::<R>,
        )
    }

    pub fn admit_right(
        self,
        load: egress::NonEmpty<R>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Admit<R>
    where
        L: 'd,
        R: 'd,
    {
        body::send(
            load,
            self.connection,
            driver,
            exact::Prepared::right_right,
            egress::NonEmpty::into_right::<L>,
        )
    }
}

impl<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B, G>
    Context<'e, 'turn, 'a, 'p, 's, 'w, 'd, ID, W, C, Bound<B, G>>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
    G: bound::Bound,
{
    pub fn try_commit<R, F, E>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<Bound<B, G>, R, E>>
    where
        F: for<'b, 'id> FnOnce(
            &mut C,
            write::Prefix<'b, 'id>,
        ) -> Result<
            write::Commit<region::Identity<'id>, egress::load::Value<Bound<B, G>>, R>,
            E,
        >,
    {
        self.try_send::<_, _, _, false>(driver, egress::Queue::prove, |send, driver| {
            send.try_header(driver, commit)
        })
    }
}

impl<'e, 'turn, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, L, R, G>
    Context<'e, 'turn, 'a, 'p, 's, 'w, 'd, ID, W, C, Bound<Either<L, R>, G>>
where
    W: wire::Spec + 'w + 'd,
    L: data::raw::Payload<'d> + 'd,
    R: data::raw::Payload<'d> + 'd,
    G: bound::Bound + 'd,
{
    /// Retains the exact bounded source until admission commits its sum variant.
    pub fn admit_right(
        self,
        load: egress::NonEmpty<Bound<R, G>>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Admit<Bound<R, G>> {
        body::send(
            load,
            self.connection,
            driver,
            exact::Prepared::bound_right,
            egress::NonEmpty::bound_right::<L>,
        )
    }
}

const _: () = assert!(
    mem::size_of::<
        Context<
            'static,
            'static,
            'static,
            'static,
            'static,
            'static,
            'static,
            KeyTag<0>,
            wire::Identity,
            (),
            resident::Shared<'static>,
        >,
    >() == mem::size_of::<usize>()
);
