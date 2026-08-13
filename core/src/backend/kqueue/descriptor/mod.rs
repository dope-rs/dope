mod sealed;

pub(crate) use sealed::Handle;
pub(in crate::backend::kqueue) use sealed::{Init, Options};
