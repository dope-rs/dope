use std::mem;

use crate::driver::schedule::ready::task::owner;

/// Selection authority for work owned by one statically tagged source.
#[derive(Clone, Copy, Eq)]
pub enum Scope<Source = ()> {
    Exact(owner::Owner<Source>),
    Any,
}

impl<Source> PartialEq for Scope<Source> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Exact(left), Self::Exact(right)) => left == right,
            (left, right) => mem::discriminant(left) == mem::discriminant(right),
        }
    }
}
