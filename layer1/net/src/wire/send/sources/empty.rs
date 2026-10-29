use std::convert::Infallible;

use crate::wire::send::{self, Payload};

/// No payload; the prepared value still carries the storage borrow lifetime.
#[derive(Clone, Copy, Default)]
pub struct Empty;

impl<'a> Payload<'a> for Empty {
    fn select(&self) -> Option<impl send::raw::Ready<'a>> {
        None::<Infallible>
    }
}
