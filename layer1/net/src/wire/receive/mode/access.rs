use std::num::NonZero;

use dope_core::io::{recv, transfer};
use o3::buffer::{
    self, hold,
    write::consume::{self, scope},
};

/// The total length may cover multiple contiguous heads. Each head keeps its original storage.
pub trait Cursor<'d>:
    buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume + transfer::Length
{
    /// Admits the exact positive extent exposed by the current view.
    fn head_len(&self) -> Result<Option<transfer::NonZero>, scope::Error> {
        let Some(count) = NonZero::new(self.as_ref().len()) else {
            return Ok(None);
        };
        let len = transfer::NonZero::try_new(count.get()).ok_or(scope::Error::Maximum {
            count,
            maximum: transfer::MAX_BYTES as u32,
        })?;
        self.try_get(0..count.get()).map_err(scope::Error::Range)?;
        Ok(Some(len))
    }

    /// Commits a whole head and selects its successor. A terminal cursor is released by its owner.
    fn successor(&mut self) -> Result<Option<transfer::NonZero>, scope::Error> {
        Ok(None)
    }
}

impl<'d> Cursor<'d> for recv::Shared<'d> {
    fn head_len(&self) -> Result<Option<transfer::NonZero>, scope::Error> {
        Ok(transfer::Length::len(self).nonzero())
    }
}
