use std::pin::Pin;

use crate::DriverContext;
use crate::manifold::datagram::send::SendOp;
use crate::manifold::datagram::{RECV_ARM_TAG, Socket};
use dope_core::backend::RawSqe;
use dope_core::driver::buffers::ProvidedBuffers;
use dope_core::driver::submission::raw::Submission as _;
use dope_core::driver::token::Token;

pub(in crate::manifold::datagram) struct Io<'a, 'c, 'd, const ID: u8> {
    socket: Pin<&'a mut Socket<'d, ID>>,
    driver: &'a mut DriverContext<'c, 'd>,
}

impl<'a, 'c, 'd, const ID: u8> Io<'a, 'c, 'd, ID> {
    pub(in crate::manifold::datagram) fn new(
        socket: Pin<&'a mut Socket<'d, ID>>,
        driver: &'a mut DriverContext<'c, 'd>,
    ) -> Self {
        Self { socket, driver }
    }

    pub(in crate::manifold::datagram) fn arm_recv(&mut self) {
        let this = self.socket.as_mut().project();
        let Some(ud) = this.recv_arm.begin(ID, RECV_ARM_TAG) else {
            return;
        };
        let buf_group = self.driver.buffer_group();
        let sqe = RawSqe::recv_msg_multi(this.fixed_fd, this.recv_msghdr.raw(), buf_group, ud);
        // SAFETY: the pinned socket owns both the fixed fd and recv msghdr
        // template until the multishot arm is canceled and quiesced.
        let pushed = unsafe { self.driver.push_raw(sqe) }.is_ok();
        this.recv_arm.settle(pushed);
    }

    pub(in crate::manifold::datagram) fn flush_outgoing(&mut self) {
        let this = self.socket.as_mut().project();
        while this.in_flight.len() < this.in_flight.capacity() {
            let Some(out) = this.pending_outgoing.pop_front() else {
                break;
            };
            let op = SendOp::new(out);
            let (key, msghdr) = match this.in_flight.insert_entry(op) {
                Ok((key, op)) => (key, op.fill_msghdr()),
                Err(op) => {
                    let out = op.into_outgoing();
                    let Some(entry) = this.pending_outgoing.vacant_entry() else {
                        unreachable!()
                    };
                    entry.push_front(out);
                    break;
                }
            };
            let ud = Token::from_key(key);
            // SAFETY: the fixed slab retains `SendOp` (payload, address, iov,
            // msghdr, and control data) under `ud` until its completion.
            let pushed = unsafe {
                self.driver
                    .push_raw(RawSqe::send_msg(this.fixed_fd, msghdr.raw(), ud))
            }
            .is_ok();
            if !pushed {
                if let Some(op) = this.in_flight.remove(key) {
                    let out = op.into_outgoing();
                    let Some(entry) = this.pending_outgoing.vacant_entry() else {
                        unreachable!()
                    };
                    entry.push_front(out);
                }
                break;
            }
        }
    }
}
