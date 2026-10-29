use std::{
    convert::{self, Infallible},
    mem,
};

use compose::Either;
use conn::write;
use dope_core::driver::{
    retained,
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

use crate::network::listener::conn;

mod modes;
mod send;

use send::{body, build, capacity};

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
    fn try_send<R, E, S>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        reserve: impl FnOnce(
            capacity::Capacity<'e, 'turn, 'a, 'p, 's, 'w, 'd, ID, W, C, B>,
            &mut region::Token<region::Identity<'d>>,
        ) -> Result<S, capacity::Unavailable>,
        commit: impl FnOnce(
            S,
            &mut retained::Context<'_, '_, 'd>,
        ) -> Result<write::Outcome<R>, Error<B, R, E>>,
    ) -> Result<write::Outcome<R>, Error<B, R, E>> {
        let Some(lease) = self.connection.work.lease() else {
            self.connection.slot.io().wake_target().wake();
            return Ok(write::Outcome::Deferred(write::Deferred::Turn));
        };
        let capacity =
            capacity::Capacity::new(self.connection, driver.driver_ref().clock().now(), lease);
        let send = match reserve(capacity, driver.region_token()) {
            Ok(send) => send,
            Err(capacity::Unavailable::Deferred(cause)) => {
                return Ok(write::Outcome::Deferred(cause));
            }
            Err(capacity::Unavailable::Closed) => return Ok(write::Outcome::Closed),
            Err(capacity::Unavailable::Rejected(cause)) => {
                return Err(Error::Capacity(cause));
            }
        };
        commit(send, driver)
    }

    pub fn try_bounded<R, F>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<B, R, Infallible>>
    where
        F: FnOnce(&mut C, &mut [u8]) -> write::Draft<B, R>,
        B: 'd,
    {
        self.try_send(driver, capacity::Capacity::try_bounded, |send, driver| {
            send.execute(driver, commit)
        })
    }

    /// Reserves the exact prefix owner and queue capacity before calling the application.
    /// The uninhabited body type prevents the callback from adding unreserved egress.
    pub fn try_prefix<R, F>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<B, R, Infallible>>
    where
        F: FnOnce(&mut C, &mut [u8]) -> write::Draft<Infallible, R>,
        B: 'd,
    {
        self.try_send(driver, capacity::Capacity::try_bounded, |send, driver| {
            send.try_prefix(driver, commit)
        })
    }

    pub fn admit(
        self,
        load: egress::NonEmpty<B>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Admit<B>
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
        driver: &mut retained::Context<'_, '_, 'd>,
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
        driver: &mut retained::Context<'_, '_, 'd>,
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
    /// Reserves the final prefix and body bound before building a checked draft.
    pub fn try_draft<R, F, E>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<Bound<B, G>, R, E>>
    where
        F: FnOnce(&mut C, &mut [u8]) -> Result<write::Draft<Bound<B, G>, R>, E>,
    {
        self.try_send(driver, capacity::Capacity::take, |send, driver| {
            send.try_draft(driver, commit)
        })
    }

    pub fn try_commit<R, F, E>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        commit: F,
    ) -> Result<write::Outcome<R>, Error<Bound<B, G>, R, E>>
    where
        F: for<'b, 'id> FnOnce(
            &mut C,
            write::Prefix<'b, 'id>,
        ) -> Result<
            write::Commit<region::Proof<region::Identity<'id>>, Bound<B, G>, R>,
            E,
        >,
    {
        self.try_send(driver, capacity::Capacity::take, |send, driver| {
            send.try_header(driver, build::Scoped(commit))
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
        driver: &mut retained::Context<'_, '_, 'd>,
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
