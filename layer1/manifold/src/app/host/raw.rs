use dope_runtime::topology::lending;

use crate::app::{Host, dispatch};

// SAFETY: Host contains only this pinned manifold and forwards its complete
// shutdown, liveness, and finalization capabilities without extra native state.
unsafe impl<'d, M: dispatch::raw::Resume<'d>> lending::raw::Resume<'d> for Host<M> {}
