use std::{convert, error, fmt, io};

use o3::cell::region;

use crate::driver::{self, flight, lifecycle::routing, route, schedule::timer};

mod impossible;
mod never;
pub(super) mod ownership;
pub(super) mod retirements;

pub(super) use impossible::Impossible;
pub use never::Never;

/// Restricted construction context for driver-domain storage.
///
/// This exposes stable domain identities and route reservation, but
/// deliberately cannot submit operations. A factory therefore cannot publish
/// a pointer into its output before the runtime has pinned that output.
///
/// ```compile_fail
/// use dope_core::driver::{
///     ops::Submit,
///     storage::Context,
///     route::{KeyTag, Operation},
/// };
///
/// fn submit_during_build<'d>(
///     context: &mut Context<'_, 'd>,
///     target: Operation<'d, KeyTag<1>>,
/// ) {
///     let _ = Submit::cancel(context, target);
/// }
/// ```
pub struct Context<'a, 'd> {
    context: driver::Context<'a, 'd>,
}

impl<'a, 'd> Context<'a, 'd> {
    pub(super) fn new(context: driver::Context<'a, 'd>) -> Self {
        Self { context }
    }

    pub fn driver(&self) -> driver::Reference<'d> {
        self.context.driver_ref()
    }

    pub fn region(&self) -> &region::Token<'d> {
        self.context.region_token_ref()
    }

    pub fn timer(&self) -> &'d timer::Timer<'d> {
        self.context.timer()
    }

    #[doc(hidden)]
    pub fn flight_slots<Tag: route::Tag>(
        &mut self,
        capacity: usize,
    ) -> io::Result<flight::Slots<'d, Tag>> {
        self.context.flight_slots(capacity)
    }

    pub fn reserve_route<const ID: u8>(&mut self) -> io::Result<routing::Reserved<'d, ID>> {
        use crate::driver::route::FRAMEWORK;

        if ID == FRAMEWORK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: reserved route",
            ));
        }
        routing::Reserved::reserve_allowed(&mut self.context)
    }
}

/// Constructs driver-domain storage through the non-submitting [`Context`].
pub trait Factory: 'static {
    type Output<'d>: 'd;
    type Error: error::Error + Send + Sync + 'static;

    fn build<'d>(self, context: &mut Context<'_, 'd>) -> Result<Self::Output<'d>, Self::Error>;
}

pub struct Value<T>(T);

impl<T> Value<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: 'static> Factory for Value<T> {
    type Output<'d> = T;
    type Error = convert::Infallible;

    fn build<'d>(self, _context: &mut Context<'_, 'd>) -> Result<Self::Output<'d>, Self::Error> {
        Ok(self.0)
    }
}

impl Factory for () {
    type Output<'d> = ();
    type Error = convert::Infallible;

    fn build<'d>(self, _context: &mut Context<'_, 'd>) -> Result<Self::Output<'d>, Self::Error> {
        Ok(())
    }
}

impl<F: Factory> Factory for Option<F> {
    type Output<'d> = Option<F::Output<'d>>;
    type Error = F::Error;

    fn build<'d>(self, context: &mut Context<'_, 'd>) -> Result<Self::Output<'d>, Self::Error> {
        self.map(|factory| factory.build(context)).transpose()
    }
}

#[derive(Debug)]
pub enum PairError<A, B> {
    First(A),
    Second(B),
}

impl<A: fmt::Display, B: fmt::Display> fmt::Display for PairError<A, B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::First(error) => error.fmt(formatter),
            Self::Second(error) => error.fmt(formatter),
        }
    }
}

impl<A, B> error::Error for PairError<A, B>
where
    A: error::Error + 'static,
    B: error::Error + 'static,
{
}

impl<A: Factory, B: Factory> Factory for (A, B) {
    type Output<'d> = (A::Output<'d>, B::Output<'d>);
    type Error = PairError<A::Error, B::Error>;

    fn build<'d>(self, context: &mut Context<'_, 'd>) -> Result<Self::Output<'d>, Self::Error> {
        let first = Factory::build(self.0, context).map_err(PairError::First)?;
        let second = Factory::build(self.1, context).map_err(PairError::Second)?;
        Ok((first, second))
    }
}
