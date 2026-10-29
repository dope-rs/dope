use dope_core::driver::result;

use crate::link::event;

/// Bounded result of draining protocol-owned terminal control output.
pub enum Finalize<E> {
    Settled,
    /// A send completion is registered; the evidence identifies its owner boundary.
    Armed(E),
    Abandoned,
    Rejected(event::SendFailure<result::SubmitError>),
}
