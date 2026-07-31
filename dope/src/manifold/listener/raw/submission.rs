use crate::DriverContext;
use crate::manifold::listener::state::State;
use dope_core::driver::token::Token;
use dope_core::io::socket::msg::{IoVec, MsgHdr};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use dope_net::wire::send::{StableVectoredSource, Vectored};

struct SplitSource<'a> {
    iovs: &'a [IoVec],
    iov_storage: &'a mut [IoVec],
    msghdr_storage: &'a mut MsgHdr,
}

// SAFETY: all descriptors refer to header/body storage retained by the
// listener slot. Its send state does not mutate or release that storage until
// the matching completion, and owns both descriptor scratch regions.
unsafe impl<'a> StableVectoredSource<'a> for SplitSource<'a> {
    #[inline(always)]
    fn into_parts(self) -> (&'a [IoVec], &'a mut [IoVec], &'a mut MsgHdr) {
        (self.iovs, self.iov_storage, self.msghdr_storage)
    }
}

pub(in crate::manifold::listener) struct Submission<'a, 'c, 'd, W: Wire, C: Default + 'static> {
    slot: &'a mut Slot<'d, W, State<C>>,
    iovs: &'a [IoVec; 2],
    ud: Token,
    driver: &'a mut DriverContext<'c, 'd>,
}

impl<'a, 'c, 'd, W: Wire, C: Default + 'static> Submission<'a, 'c, 'd, W, C> {
    pub(in crate::manifold::listener) fn new(
        slot: &'a mut Slot<'d, W, State<C>>,
        iovs: &'a [IoVec; 2],
        ud: Token,
        driver: &'a mut DriverContext<'c, 'd>,
    ) -> Self {
        Self {
            slot,
            iovs,
            ud,
            driver,
        }
    }

    pub(in crate::manifold::listener) fn submit(self) -> usize {
        let vectored = Vectored::from_stable(SplitSource {
            iovs: self.iovs,
            iov_storage: &mut self.slot.state.send.pending_iovs,
            msghdr_storage: &mut self.slot.state.send.pending_msghdr,
        });
        Slot::<W, State<C>>::submit_wire_vectored(
            &mut self.slot.core,
            &mut self.slot.wire,
            &mut self.slot.send,
            vectored,
            self.ud,
            self.driver,
        )
    }
}
