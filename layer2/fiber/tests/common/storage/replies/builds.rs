use std::convert;

use dope_fiber::reply::{
    usage,
    usage::{Bytes, Usage, Values},
};

use crate::common::storage::replies::support::{Counted, Drops};

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
