use std::{convert, mem::MaybeUninit};

use dope_core::driver::{self, route::Identity};
use dope_net::{
    link::egress::{
        self,
        data::{self, Bound, compose::Either},
        reserve::{self, error::Full, exact},
    },
    wire,
};
use o3::{buffer::resident::bound, mem::slot};

use crate::network::listener::{conn, writer::memory::WRITE_BUF_CAP};

pub mod body;
mod direct;
pub mod error;
mod output;

pub use output::Output;

pub const HEADER_CAPACITY: usize = WRITE_BUF_CAP;

/// Retryable cause whose exact wake authority is already registered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Deferred {
    Turn,
    Capacity,
    OrderedDrain,
}

/// Permanent final-link rejection of a valid fallible write plan.
#[derive(Debug)]
pub enum Reject {
    Geometry(Full),
    Header,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum End {
    Open,
    Shutdown,
    Close,
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B>
    conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
{
    /// Admits a header without granting body publication authority.
    pub fn try_prefix<'turn, 'out, 'io>(
        &'turn mut self,
        driver: &'turn mut driver::Context<'io, 'd>,
        output: &'out mut MaybeUninit<
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, reserve::Prefix<HEADER_CAPACITY>>,
        >,
    ) -> Result<
        slot::Occupied<
            'out,
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, reserve::Prefix<HEADER_CAPACITY>>,
        >,
        error::Admission,
    > {
        Output::prepare(
            self,
            driver,
            output,
            |_| Some(((), ())),
            |queue, ()| queue.prefix::<HEADER_CAPACITY>(),
        )
    }

    pub fn admit(
        &mut self,
        load: egress::NonEmpty<B>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> body::Admit<B>
    where
        B: 'd,
    {
        body::send(load, self, driver, exact::Prepared::right, convert::identity)
    }

    pub fn reject_write(&mut self, cause: body::Failure) {
        self.slot.state.fail(conn::Failure::Write(cause));
        self.slot.close_after();
    }
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, L, R>
    conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, Either<L, R>>
where
    W: wire::Spec + 'w + 'd,
    L: data::raw::Payload<'d> + 'd,
    R: data::raw::Payload<'d> + 'd,
{
    pub fn admit_left(
        &mut self,
        load: egress::NonEmpty<L>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> body::Admit<L> {
        body::send(
            load,
            self,
            driver,
            exact::Prepared::right_left,
            egress::NonEmpty::into_left::<R>,
        )
    }

    pub fn admit_right(
        &mut self,
        load: egress::NonEmpty<R>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> body::Admit<R> {
        body::send(
            load,
            self,
            driver,
            exact::Prepared::right_right,
            egress::NonEmpty::into_right::<L>,
        )
    }
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, B, G>
    conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, Bound<B, G>>
where
    W: wire::Spec + 'w + 'd,
    B: data::raw::Payload<'d>,
    G: bound::Bound,
{
    pub fn try_output<'turn, 'out, 'io>(
        &'turn mut self,
        driver: &'turn mut driver::Context<'io, 'd>,
        output: &'out mut MaybeUninit<
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, Bound<B, G>, reserve::Bounds, G>,
        >,
    ) -> Result<
        slot::Occupied<
            'out,
            Output<'turn, 'a, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, Bound<B, G>, reserve::Bounds, G>,
        >,
        error::Admission,
    > {
        Output::prepare(self, driver, output, egress::Queue::prove, |_, plan| Ok(plan))
    }
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W, C, L, R, G>
    conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, Bound<Either<L, R>, G>>
where
    W: wire::Spec + 'w + 'd,
    L: data::raw::Payload<'d> + 'd,
    R: data::raw::Payload<'d> + 'd,
    G: bound::Bound + 'd,
{
    /// Retains the exact bounded source until admission commits its sum variant.
    pub fn admit_right(
        &mut self,
        load: egress::NonEmpty<Bound<R, G>>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> body::Admit<Bound<R, G>> {
        body::send(
            load,
            self,
            driver,
            exact::Prepared::bound_right,
            egress::NonEmpty::bound_right::<L>,
        )
    }
}
