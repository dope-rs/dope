use dope_net::link::egress::{
    self, load,
    reserve::{self, error},
};
use o3::{base::num::bounded, buffer::bytes::bounded::Overflow};

use crate::network::listener::conn::write;

/// One observed value at an ordered reservation position.
pub enum Part<B> {
    Empty,
    Prefix(egress::Charge),
    Body(load::Value<B>),
}

/// Exact reason an observed bounded draft cannot own final-link capacity.
#[derive(Debug)]
pub enum Bounded {
    Prefix(bounded::Error),
    Wire(egress::WireError),
    Geometry(error::Full),
}

/// Exact draft rejection after its physical owners have been released.
#[derive(Debug)]
pub enum Cause {
    Prefix(bounded::Error),
    Wire(egress::WireError),
    Geometry(error::Full),
    Values { cause: error::Cause },
}

/// Rejected values retaining every caller-owned value and terminal intent.
pub enum Mismatch<B, R> {
    Bounded {
        cause: Bounded,
        draft: write::Draft<B, R>,
    },
    Values {
        cause: error::Cause,
        expected: reserve::Bounds,
        first: Part<B>,
        second: Part<B>,
        receipt: R,
        end: write::End,
    },
}

/// Rejected guaranteed-write build retaining every caller-owned value.
pub enum Build<B, R> {
    Prefix {
        cause: bounded::Error,
        body: Option<egress::Load<B>>,
        receipt: R,
        end: write::End,
    },
    Rejected {
        cause: egress::Rejected<egress::Load<B>, load::Error>,
        receipt: R,
        end: write::End,
    },
    Overflow {
        cause: Overflow,
        body: egress::NonEmpty<B>,
        receipt: R,
        end: write::End,
    },
}

/// Recoverable failure of one listener write attempt.
pub enum Error<B, R, E> {
    Capacity(write::Reject),
    Build(E),
    Mismatch(Mismatch<B, R>),
}

impl<B, R> Build<B, R> {
    pub const fn prefix(cause: bounded::Error, body: Option<egress::Load<B>>, receipt: R) -> Self {
        Self::Prefix {
            cause,
            body,
            receipt,
            end: write::End::Open,
        }
    }

    pub(in crate::network::listener) const fn rejected(
        cause: egress::Rejected<egress::Load<B>, load::Error>,
        receipt: R,
    ) -> Self {
        Self::Rejected {
            cause,
            receipt,
            end: write::End::Open,
        }
    }

    pub(in crate::network::listener) const fn overflow(
        cause: Overflow,
        body: egress::NonEmpty<B>,
        receipt: R,
    ) -> Self {
        Self::Overflow {
            cause,
            body,
            receipt,
            end: write::End::Open,
        }
    }

    pub fn close_after(mut self) -> Self {
        match &mut self {
            Self::Prefix { end, .. } | Self::Rejected { end, .. } | Self::Overflow { end, .. } => {
                *end = write::End::Close
            }
        }
        self
    }
}

impl<B, R> Mismatch<B, R> {
    /// Releases rejected payload owners and returns their exact cause and receipt.
    /// The failed bound is carried by the cause; the full reservation plan is released.
    pub fn into_parts(self) -> (Cause, R) {
        match self {
            Self::Bounded { cause, draft } => {
                let (_, _, receipt, _) = draft.into_parts();
                let cause = match cause {
                    Bounded::Prefix(cause) => Cause::Prefix(cause),
                    Bounded::Wire(cause) => Cause::Wire(cause),
                    Bounded::Geometry(cause) => Cause::Geometry(cause),
                };
                (cause, receipt)
            }
            Self::Values { cause, receipt, .. } => (Cause::Values { cause }, receipt),
        }
    }

    pub(in crate::network::listener) const fn bounded(
        cause: Bounded,
        draft: write::Draft<B, R>,
    ) -> Self {
        Self::Bounded { cause, draft }
    }

    pub(in crate::network::listener) const fn values(
        cause: error::Cause,
        expected: reserve::Bounds,
        first: Part<B>,
        second: Part<B>,
        receipt: R,
        end: write::End,
    ) -> Self {
        Self::Values {
            cause,
            expected,
            first,
            second,
            receipt,
            end,
        }
    }
}
