use std::mem;

use dope::{
    manifold::client::app::request,
    net::link::{egress, egress::data},
};
use egress::{load, reserve::exact};
use o3::buffer;

use crate::{
    storage::queries::tx,
    wire::{frames, tx::Id},
};

mod lease;
mod live;

pub(in crate::storage::queries) use lease::Lease;
pub(in crate::storage::queries) use live::Live;

pub(crate) enum Admission {
    Committed(tx::Commit),
    Exhausted(egress::NonEmpty<Frame>),
    Saturated(egress::NonEmpty<Frame>),
    Rejected(egress::Rejected<egress::Load<Frame>, load::Error>),
    Unavailable(exact::Unavailable<Frame>),
}

pub(crate) enum Submission {
    Committed,
    Deferred,
    Rejected,
}

const _: () = {
    assert!(mem::size_of::<Submission>() == 1);
    assert!(
        mem::size_of::<Admission>()
            <= mem::size_of::<request::Admission<'static, 'static, 'static, 'static, Frame>>()
    );
};

pub(crate) struct Frame {
    output: data::Inline<{ frames::MAX_STREAM_FRAME_BYTES }>,
    id: Id,
}

impl Frame {
    pub(in crate::storage::queries) const fn new(
        output: data::Inline<{ frames::MAX_STREAM_FRAME_BYTES }>,
        id: Id,
    ) -> Self {
        Self { output, id }
    }

    pub(crate) const fn prepare(self) -> (Self, tx::Commit) {
        let commit = tx::Commit::new(self.id);
        (self, commit)
    }

    pub(crate) const fn cancel(self) -> Id {
        self.id
    }
}

impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        self.output.as_ref()
    }
}

impl buffer::Residency for Frame {
    fn resident(&self) -> usize {
        buffer::Residency::resident(&self.output)
    }
}

// SAFETY: shared access preserves the inline output's initialized slice and
// allocation charge; the transaction ID neither changes nor contributes bytes.
unsafe impl<'d> data::raw::Contiguous<'d> for Frame {}

const _: () = assert!(
    mem::size_of::<Frame>()
        == mem::size_of::<data::Inline<{ frames::MAX_STREAM_FRAME_BYTES }>>()
            + mem::size_of::<Id>()
);
