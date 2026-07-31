use crate::wire::send::{StableVectoredSource, Vectored};
use dope_core::io::socket::msg::{IoVec, MsgHdr};

pub(in crate::link::egress) struct Preparation<'a> {
    iovs: &'a [IoVec],
    storage: &'a mut [IoVec],
    msghdr: &'a mut MsgHdr,
}

impl<'a> Preparation<'a> {
    pub(in crate::link::egress) fn new(
        iovs: &'a [IoVec],
        storage: &'a mut [IoVec],
        msghdr: &'a mut MsgHdr,
    ) -> Self {
        Self {
            iovs,
            storage,
            msghdr,
        }
    }

    pub(in crate::link::egress) fn prepare(self) -> Vectored<'a> {
        Vectored::from_stable(self)
    }
}

// SAFETY: every descriptor is derived from this queue's live entry storage.
// The consuming send protocol retains those entries and the queue-owned
// descriptor storage unchanged until completion.
unsafe impl<'a> StableVectoredSource<'a> for Preparation<'a> {
    #[inline(always)]
    fn into_parts(self) -> (&'a [IoVec], &'a mut [IoVec], &'a mut MsgHdr) {
        (self.iovs, self.storage, self.msghdr)
    }
}
