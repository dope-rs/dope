use std::convert;

use dope_fiber::reply::{
    self, recv,
    recv::outcome::Terminal,
    retain, usage,
    usage::{Bytes, Usage, Values},
};

use crate::common::storage::replies::support::{Counted, Drops, Error};

pub const SCALAR: [u8; 32] = [7; 32];

pub struct Span<'a>(pub &'a [u8]);

pub struct Rows<'a> {
    pub span: Span<'a>,
    pub values: std::num::NonZero<u32>,
}

impl usage::Footprint for Span<'_> {
    type Amount = usage::Counted;

    fn footprint(&self) -> Self::Amount {
        usage::Counted::new(
            u32::try_from(self.0.len()).expect("fixture length"),
            std::num::NonZero::<u32>::MIN,
        )
    }
}

impl<'a> usage::Grouped for Span<'a> {
    type Group = Rows<'a>;
}

impl<'a> From<Rows<'a>> for Span<'a> {
    fn from(rows: Rows<'a>) -> Self {
        rows.span
    }
}

impl usage::Footprint for Rows<'_> {
    type Amount = usage::Counted;

    fn footprint(&self) -> Self::Amount {
        usage::Counted::new(
            u32::try_from(self.span.0.len()).expect("fixture length"),
            self.values,
        )
    }
}

pub struct GroupBuild<'a> {
    pub rows: Rows<'a>,
    pub materialize: Result<(), u8>,
}

impl<'a> usage::Build<Rows<'a>> for GroupBuild<'a> {
    type Error = u8;

    fn usage(&self) -> usage::Counted {
        usage::Footprint::footprint(&self.rows)
    }

    fn try_build(self) -> Result<Rows<'a>, (Self, Self::Error)> {
        match self.materialize {
            Ok(()) => Ok(self.rows),
            Err(code) => Err((self, code)),
        }
    }
}

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
    recv::Spec<'s, 'arena, 'credits, 'region, Scalar<'a>> for TakeScalar
{
    type Context = ();
    type Mode = ();
}

impl<'s, 'arena, 'credits: 'arena, 'region: 'credits, 'a: 'arena>
    recv::Extract<'s, 'arena, 'credits, 'region, Scalar<'a>> for TakeScalar
{
    type Output = Result<retain::Node<'arena, 'credits, 'region, Scalar<'a>>, Error>;

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
