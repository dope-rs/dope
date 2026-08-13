use crate::{link, wire::send};

mod proof;

pub(crate) use proof::Proof;

/// Compile-time ownership policy for bytes submitted to the kernel.
/// Sealing prevents a wire from claiming an unproved reclaim policy.
pub trait Policy: Proof + 'static {
    #[doc(hidden)]
    const ON_SUBMIT: bool;

    /// Converts a terminal kernel completion into acknowledged plaintext only
    /// for the exact-input policy.
    #[doc(hidden)]
    fn completed_plain(sent: send::Sent) -> Option<link::Consumed>;
}

/// Prepared output is independent of caller input, which may be reclaimed as
/// soon as the wire accepts it for submission.
pub enum OnSubmit {}

/// Prepared output is the exact caller input retained until its terminal
/// completion or quiescence.
pub enum OnComplete {}

impl Proof for OnSubmit {}
impl Proof for OnComplete {}

impl Policy for OnSubmit {
    const ON_SUBMIT: bool = true;

    fn completed_plain(_: send::Sent) -> Option<link::Consumed> {
        None
    }
}

impl Policy for OnComplete {
    const ON_SUBMIT: bool = false;

    fn completed_plain(sent: send::Sent) -> Option<link::Consumed> {
        Some(link::Consumed::proven(sent.get()))
    }
}
