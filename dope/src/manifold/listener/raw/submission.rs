use crate::DriverContext;
use crate::manifold::listener::state::State;
use dope_core::driver::token::Token;
use dope_core::io::socket::msg::IoVec;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use dope_net::wire::send::Vectored;

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
        // SAFETY: both descriptors refer to the slot's header/body storage,
        // which remains unchanged while the send is in flight. The pending
        // iov and msghdr arrays are owned by the same slot.
        let vectored = unsafe {
            Vectored::from_raw(
                self.iovs,
                &mut self.slot.state.send.pending_iovs,
                &mut self.slot.state.send.pending_msghdr,
            )
        };
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
