use std::pin;

use dope_core::driver::{retained, schedule};

use crate::datagram;

pub(super) struct Drive<'a, 'c, 'owner, 'd: 'owner, const ID: u8> {
    socket: pin::Pin<&'a mut datagram::Socket<'d, ID>>,
    work: schedule::Application<'a, 'd>,
    driver: &'a mut retained::Context<'c, 'owner, 'd>,
}

impl<'a, 'c, 'owner, 'd: 'owner, const ID: u8> Drive<'a, 'c, 'owner, 'd, ID> {
    pub(super) fn run(
        socket: pin::Pin<&'a mut datagram::Socket<'d, ID>>,
        work: schedule::Application<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) {
        if !socket.sender.accepts_work() {
            socket.project().receive.retry_stop(driver);
            return;
        }
        let mut drive = Self {
            socket,
            work,
            driver,
        };
        drive.arm_recv();
        drive.flush_sends();
    }

    fn arm_recv(&mut self) {
        let this = self.socket.as_mut().project();
        this.receive
            .arm(this.binding.descriptor(), self.work, self.driver);
    }

    fn flush_sends(&mut self) {
        let this = self.socket.as_mut().project();
        this.sender
            .flush(this.binding.descriptor(), self.work, self.driver);
    }
}
