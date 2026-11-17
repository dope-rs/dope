use std::mem;

use dope_core::io::transfer;
use o3::buffer::bytes::bounded;

mod proof;

pub(crate) use proof::Proof;

/// Compile-time ownership policy for bytes submitted to the kernel.
/// Sealing prevents a wire from claiming an unproved reclaim policy.
pub trait Policy: Proof {
    /// Plaintext acknowledgements waiting on the current kernel send.
    type AckState: Default;
    /// Plaintext ownership carried by one prepared output.
    type Credit: Copy;

    /// Constructs the credit for an output which consumes no plaintext.
    fn empty() -> Self::Credit;

    /// Returns the transformed plaintext prefix, or the exact native input.
    fn submitted(
        state: &Self::AckState,
        credit: Self::Credit,
    ) -> Result<(Self::AckState, Option<transfer::Len>), Overflow>;

    /// Selects the acknowledged queue prefix when no kernel flight was armed.
    fn unsubmitted(
        state: &Self::AckState,
        credit: Self::Credit,
    ) -> Result<(Self::AckState, transfer::Len), Overflow> {
        let (next, _) = Self::submitted(state, credit)?;
        Ok((next, Self::immediate(credit)))
    }

    /// Resolves one terminal queue completion into its plaintext source.
    fn complete(state: &mut Self::AckState) -> Completion;

    /// Resolves cancellation of the current kernel send.
    fn abort(state: &mut Self::AckState) {
        let _ = Self::complete(state);
    }

    /// Reports whether a queue acknowledgement still belongs to a terminal
    /// send chain. Used to reject impossible direct/queued owner overlap.
    fn has_pending(state: &Self::AckState) -> bool;

    /// Plaintext that may be handed back before a terminal completion.
    fn immediate(credit: Self::Credit) -> transfer::Len;

    /// Classifies whether the prepared output attempted plaintext progress.
    fn advance(credit: Self::Credit) -> Advance {
        if Self::immediate(credit).is_zero() {
            Advance::Idle
        } else {
            Advance::Ready
        }
    }

    /// Selects the plaintext ownership of a direct kernel completion.
    fn direct() -> Direct;
}

/// Prepared output is independent of caller input; queued owners still await
/// terminal completion before their consumed prefix is released.
pub enum OnSubmit {}

/// Prepared output is the exact caller input retained until its terminal
/// completion or quiescence.
pub enum OnComplete {}

#[derive(Clone, Copy, Default)]
pub struct Exact;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Overflow {
    pub pending: transfer::Len,
    pub consumed: transfer::Len,
    pub cause: bounded::Overflow,
}

/// Accumulated plaintext awaiting an `OnSubmit` acknowledgement.
pub struct Pending(transfer::Len);

/// Plaintext source selected by a terminal send completion.
pub enum Completion {
    /// Complete the accumulated submission-owned plaintext.
    Submitted(transfer::Len),
    /// Complete the exact retained plaintext.
    Retained,
}

/// Plaintext ownership selected for a direct kernel completion.
#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use]
#[repr(u8)]
pub enum Direct {
    /// The prepared output does not retain caller plaintext.
    Independent,
    /// The kernel completion releases the exact caller plaintext.
    Retained,
}

#[must_use]
#[repr(u8)]
pub enum Advance {
    Idle,
    Ready,
}

impl Proof for OnSubmit {}
impl Proof for OnComplete {}

impl Default for Pending {
    fn default() -> Self {
        Self(transfer::Len::ZERO)
    }
}

impl Pending {
    fn add(&self, consumed: transfer::Len) -> Result<Self, Overflow> {
        match self.0.checked_add(consumed) {
            Ok(next) => Ok(Self(next)),
            Err(cause) => Err(Overflow {
                pending: self.0,
                consumed,
                cause,
            }),
        }
    }
}

impl Policy for OnSubmit {
    type AckState = Pending;
    type Credit = transfer::Len;

    fn empty() -> Self::Credit {
        transfer::Len::ZERO
    }

    fn submitted(
        state: &Self::AckState,
        consumed: transfer::Len,
    ) -> Result<(Self::AckState, Option<transfer::Len>), Overflow> {
        Ok((state.add(consumed)?, Some(consumed)))
    }

    fn complete(state: &mut Self::AckState) -> Completion {
        Completion::Submitted(mem::take(state).0)
    }

    fn has_pending(state: &Self::AckState) -> bool {
        !state.0.is_zero()
    }

    fn immediate(consumed: transfer::Len) -> transfer::Len {
        consumed
    }

    fn direct() -> Direct {
        Direct::Independent
    }
}

impl Policy for OnComplete {
    type AckState = Exact;
    type Credit = Exact;

    fn empty() -> Self::Credit {
        Exact
    }

    fn submitted(
        Exact: &Self::AckState,
        Exact: Self::Credit,
    ) -> Result<(Self::AckState, Option<transfer::Len>), Overflow> {
        Ok((Exact, None))
    }

    fn complete(Exact: &mut Self::AckState) -> Completion {
        Completion::Retained
    }

    fn has_pending(Exact: &Self::AckState) -> bool {
        false
    }

    fn immediate(Exact: Self::Credit) -> transfer::Len {
        transfer::Len::ZERO
    }

    fn direct() -> Direct {
        Direct::Retained
    }
}

const _: () = {
    assert!(mem::size_of::<<OnSubmit as Policy>::AckState>() == mem::size_of::<transfer::Len>());
    assert!(mem::size_of::<<OnComplete as Policy>::AckState>() == 0);
    assert!(mem::size_of::<<OnSubmit as Policy>::Credit>() == mem::size_of::<transfer::Len>());
    assert!(mem::size_of::<<OnComplete as Policy>::Credit>() == 0);
};
