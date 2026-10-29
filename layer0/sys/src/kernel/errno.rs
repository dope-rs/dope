use std::num;

use crate::{Errno, kernel};

/// Access to the calling thread's native error slot.
pub trait Source: kernel::raw::Source {
    fn try_get() -> Result<Option<Errno>, kernel::Invalid> {
        num::NonZero::new(Self::read())
            .map(Errno::try_from)
            .transpose()
    }

    fn clear();
}
