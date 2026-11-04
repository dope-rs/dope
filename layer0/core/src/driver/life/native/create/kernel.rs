use std::convert;

use dope_sys::kernel;

pub(crate) use crate::driver::result::Kernel;
use crate::driver::route::indices;
impl<O> From<kernel::Errno<O>> for Kernel
where
    <crate::Backend as kernel::Source>::Failure<indices::SlotIndex>: From<kernel::Errno<O>>,
{
    fn from(cause: kernel::Errno<O>) -> Self {
        let cause: <crate::Backend as kernel::Source>::Failure<indices::SlotIndex> = cause.into();
        Self::native(cause)
    }
}

impl From<convert::Infallible> for Kernel {
    fn from(value: convert::Infallible) -> Self {
        match value {}
    }
}

impl From<dope_sys::Errno> for Kernel {
    fn from(cause: dope_sys::Errno) -> Self {
        Self::native(cause)
    }
}
