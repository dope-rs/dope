mod sealed;

pub(in crate::backend::kqueue::engine) use sealed::{Data, Kind};
pub(in crate::backend::kqueue) use sealed::{Retry, State};
