use dope_sys::table::closed::{Completed, File, Submitted};

use crate::{driver::result::poll, io, io::fd::handles::PhysicalSlot};

/// A submitted close retains its native CQ authority; a completed close has
/// already consumed the descriptor and must deliver every leaf result.
pub(super) enum Retired {
    Submitted,
    Closed(io::Reclaimed),
}

impl From<Submitted> for Retired {
    fn from(Submitted {}: Submitted) -> Self {
        Self::Submitted
    }
}

impl<S, M, F> From<Completed<S, M, F>> for Retired
where
    PhysicalSlot: From<S>,
    poll::Missing: From<M>,
    poll::Cause: From<F>,
{
    fn from(done: Completed<S, M, F>) -> Self {
        let Completed {
            slot,
            file,
            reactor,
        } = done;
        let file = match file {
            File::Missing(cause) => poll::Close::Missing(cause.into()),
            File::Closed(result) => poll::Close::Closed(result),
        };
        Self::Closed(io::Reclaimed::Close {
            slot: slot.into(),
            file,
            reactor: reactor.map_err(poll::Cause::from),
        })
    }
}
