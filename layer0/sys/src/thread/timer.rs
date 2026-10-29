use std::mem;

use o3::base::time::Positive;

/// Exact kernel-watchdog construction failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Range { deadline: Positive },
    Unsupported { deadline: Positive },
    Create(crate::Errno),
    Arm(crate::Errno),
}

const _: () = {
    assert!(mem::size_of::<Error>() == 24);
    assert!(mem::size_of::<Positive>() == 16);
    assert!(mem::align_of::<Error>() == mem::align_of::<Positive>());
};
