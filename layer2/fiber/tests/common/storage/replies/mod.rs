use dope_fiber::reply::Batch;

pub mod builds;
pub mod life;
pub mod position;
pub mod retire;
pub mod support;

use dope_fiber::{
    reply,
    reply::{recv, recv::outcome::Terminal, retain},
};
use support::{Counted, Error};

pub struct RetainMany;
pub struct TakeBatch;
pub struct TakeOne;

impl<'s, 'arena, 'credits: 'arena, 'region: 'credits, 'a>
    recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>> for TakeOne
{
    type Output = Result<retain::Lease<'arena, 'credits, 'region, Counted<'a>>, Error>;
    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        slot.try_lease().map_err(Error::Retain).transpose()
    }
}

impl<'s, 'arena, 'credits: 'arena, 'region: 'credits, 'a>
    recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>> for RetainMany
{
    type Output = Result<
        (
            retain::Retained<'arena, 'credits, 'region, Counted<'a>>,
            retain::Lease<'arena, 'credits, 'region, Counted<'a>>,
            retain::Retained<'arena, 'credits, 'region, Counted<'a>>,
        ),
        Error,
    >;
    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        let first = {
            let done = match slot.terminal()? {
                Terminal::Complete(done) => done,
                Terminal::Failed(fault) => {
                    return Some(Err(Error::Failed(fault.into_error().failure)));
                }
            };
            match done.try_retain() {
                Ok(Some(value)) => value,
                Ok(None) => return None,
                Err(error) => return Some(Err(Error::Retain(error))),
            }
        };
        let mut next = match slot.terminal()? {
            Terminal::Complete(done) => done,
            Terminal::Failed(fault) => return Some(Err(Error::Failed(fault.into_error().failure))),
        };
        let leased = match next.try_lease() {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(error) => return Some(Err(Error::Retain(error))),
        };
        let second = match next.try_retain() {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(error) => return Some(Err(Error::Retain(error))),
        };
        Some(Ok((first, leased, second)))
    }
}

impl<'s, 'arena: 's, 'credits: 'arena, 'region: 'credits, 'a: 'arena>
    recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>> for TakeBatch
{
    type Output = Result<reply::Batch<'s, 'arena, 'credits, 'region, Counted<'a>>, Error>;
    type Context = &'s reply::Batches<'arena, 'credits, 'region, Counted<'a>>;

    fn extract(
        batches: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        Some(match slot.terminal()? {
            Terminal::Complete(done) => Batch::try_new(done, batches).map_err(Error::Retain),
            Terminal::Failed(fault) => Err(Error::Failed(fault.into_error().failure)),
        })
    }
}
