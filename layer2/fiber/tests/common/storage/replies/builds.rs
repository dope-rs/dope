use std::convert;

use dope_fiber::reply::{
    self, recv,
    recv::outcome::Terminal,
    retain, usage,
    usage::{Bytes, Usage, Values},
};

use crate::common::storage::replies::support::{Counted, Drops, Error};

pub const SCALAR: [u8; 32] = [7; 32];

pub struct Scalar<'a> {
    pub(crate) bytes: [u8; 32],
    drops: &'a Drops,
}

pub struct ScalarBuild<'a, 'b> {
    pub(crate) bytes: &'b [u8; 32],
    pub(crate) drops: &'a Drops,
}

pub struct TakeScalar;

impl usage::Footprint for Scalar<'_> {
    type Amount = Bytes;

    fn footprint(&self) -> Bytes {
        Bytes::new(self.bytes.len())
    }
}

impl Drop for Scalar<'_> {
    fn drop(&mut self) {
        self.drops.record();
    }
}

impl<'a> usage::Build<Scalar<'a>> for ScalarBuild<'a, '_> {
    type Error = convert::Infallible;

    fn usage(&self) -> Bytes {
        Bytes::new(self.bytes.len())
    }

    fn try_build(self) -> Result<Scalar<'a>, (Self, Self::Error)> {
        Ok(Scalar {
            bytes: *self.bytes,
            drops: self.drops,
        })
    }
}

impl<'s, 'arena, 'credits: 'arena, 'region: 'credits, 'a: 'arena>
    recv::Extract<'s, 'arena, 'credits, 'region, Scalar<'a>> for TakeScalar
{
    type Output = Result<retain::Node<'arena, 'credits, 'region, Scalar<'a>>, Error>;
    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Scalar<'a>>,
    ) -> Option<Self::Output> {
        match slot.terminal()? {
            Terminal::Complete(mut done) => done.try_node().map_err(Error::Retain).transpose(),
            Terminal::Failed(_) => Some(Err(Error::Terminal)),
        }
    }
}

pub struct Probe<'a, 'b> {
    pub(crate) built: &'a mut bool,
    pub(crate) drops: &'b Drops,
}

pub struct Fail;

pub struct Reject;

impl<'a> usage::Build<Counted<'a>> for Probe<'_, 'a> {
    type Error = convert::Infallible;

    fn usage(&self) -> usage::Usage {
        Usage::new(Bytes::ZERO, Values::new(1))
    }

    fn try_build(self) -> Result<Counted<'a>, (Self, Self::Error)> {
        *self.built = true;
        Ok(Counted::data(self.drops))
    }
}

impl<'a> usage::Build<Counted<'a>> for Fail {
    type Error = Reject;

    fn usage(&self) -> usage::Usage {
        Usage::new(Bytes::ZERO, Values::new(1))
    }

    fn try_build(self) -> Result<Counted<'a>, (Self, Self::Error)> {
        Err((self, Reject))
    }
}
