use std::mem;

use dope::core::driver::schedule::{app, clock};
use o3::base::cell::region;

use crate::{
    lookup::error,
    storage::queries::{
        self, index, receive,
        stream::{Admission, Frame, Submission},
    },
    wire::{response, tx},
};

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Peer<'a, 's, 'd, const LANES: usize, const N: usize> {
    pub(super) owner: &'a queries::Queries<'s, 'd, LANES, N>,
    pub(super) index: index::Server,
}

impl<'a, 's, 'd, const LANES: usize, const N: usize> Peer<'a, 's, 'd, LANES, N> {
    pub(crate) fn submit(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: clock::Moment<'d>,
        id: tx::Id,
        submit: impl FnOnce(Frame) -> Admission,
    ) -> Submission {
        now.at(self.owner)
            .apply(|owner, raw| owner.submit(token, raw, self.index, id, submit))
    }

    pub(crate) fn status(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> queries::StreamStatus {
        queries::StreamStatus::read(self.owner, token, self.index)
    }

    pub(crate) fn try_recv<'packet>(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        frame: &'packet [u8],
        now: clock::Moment<'d>,
    ) -> Result<queries::StreamStatus, error::Packet> {
        let msg = response::Decoder::try_new(frame)?;
        Ok(now
            .at(self.owner)
            .apply(|owner, raw| receive::stream(owner, token, self.index, msg, raw)))
    }

    pub(crate) fn retire(
        self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        end: queries::Retirement,
        now: clock::Moment<'d>,
        work: app::Work<'_, 'd>,
        branches: &mut index::Retirement<LANES>,
    ) -> queries::RetireStream {
        now.at(self.owner)
            .apply(|owner, raw| owner.retire(token, self.index, end, raw, work, branches))
    }
}

const _: () =
    assert!(mem::size_of::<Peer<'static, 'static, 'static, 1, 1>>() == mem::size_of::<usize>() * 2);
