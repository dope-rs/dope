use std::{
    convert,
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
    reserve::{self, exact},
};
use o3::{
    buffer::{resident, resident::bound},
    mem::slot,
};
use write::{
    body::{Admit, Failure},
    error::Admission,
    output::Output,
};

use crate::network::listener::{conn, writer};

mod send;

use send::body;

/// Affine access to one listener output admission.
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

    /// Admits a header without granting body publication authority.
    pub fn try_prefix<'out, 'io>(
        self,
        driver: &'turn mut driver::Context<'io, 'd>,
        output: &'out mut mem::MaybeUninit<
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, reserve::Prefix<{ writer::memory::WRITE_BUF_CAP }>>,
        >,
    ) -> Result<
        slot::Occupied<'out, Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, reserve::Prefix<{ writer::memory::WRITE_BUF_CAP }>>>,
        Admission,
    > {
        use writer::memory;

        Output::prepare(
            self.connection,
            driver,
            output,
            |_| Some(((), ())),
            |queue, ()| {
                queue.prefix::<{ memory::WRITE_BUF_CAP }>()
            },
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
        self.connection.pending()
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
    pub fn try_open<'out, 'io>(
        self,
        driver: &'turn mut driver::Context<'io, 'd>,
        output: &'out mut mem::MaybeUninit<
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, Bound<B, G>, reserve::Bounds, G>,
        >,
    ) -> Result<
        slot::Occupied<
            'out,
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, Bound<B, G>, reserve::Bounds, G>,
        >,
        Admission,
    > {
        Output::prepare(
            self.connection,
            driver,
            output,
            egress::Queue::prove,
            |_, plan| Ok(plan),
        )
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
