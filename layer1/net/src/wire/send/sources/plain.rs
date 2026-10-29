use dope_core::io::transfer;
use o3::buffer::storage::Zero;

use crate::wire::send::{self, Payload};

pub struct Plain<'a> {
    view: transfer::View<'a>,
}

impl<'a> Plain<'a> {
    pub(in crate::wire::send) fn into_view(self) -> transfer::View<'a> {
        self.view
    }

    pub(crate) fn proven(bytes: &'a [u8]) -> Self {
        Self {
            view: transfer::View::prefix(bytes),
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &'a [u8] {
        self.view.as_slice()
    }

    #[must_use]
    pub fn len(&self) -> transfer::Len {
        self.view.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.view.as_slice().is_empty()
    }
}

pub(in crate::wire::send) struct Slice<'a> {
    pub(in crate::wire::send) view: transfer::View<'a>,
    pub(in crate::wire::send) bytes: transfer::NonZero,
}

impl<'scope, 'bytes: 'scope> Payload<'scope> for Plain<'bytes> {
    fn select(&self) -> Option<impl send::raw::Ready<'scope>> {
        let view: transfer::View<'scope> = self.view;
        match transfer::NonZero::try_from(view.len()) {
            Ok(bytes) => Some(Slice { view, bytes }),
            Err(Zero) => None,
        }
    }
}
