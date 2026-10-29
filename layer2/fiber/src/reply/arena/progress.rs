//! Exact progress reported while failing pending replies.

use crate::reply::order;

/// Exact scheduling state of deferred reply retirement.
#[must_use = "arena work state must drive scheduling"]
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Work {
    Idle,
    Pending,
}

const _: () = assert!(std::mem::size_of::<Work>() == std::mem::size_of::<u8>());

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Progress {
    Complete(order::Depth),
    Pending(order::Depth),
}

impl Progress {
    pub const fn depth(self) -> order::Depth {
        match self {
            Self::Complete(depth) | Self::Pending(depth) => depth,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Progress>() == 2 * std::mem::size_of::<order::Depth>());
