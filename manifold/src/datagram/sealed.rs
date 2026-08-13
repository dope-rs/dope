use std::{ops, pin};

use dope_core::{
    driver::{
        self, flight, lifecycle, retained,
        route::{self, table},
        schedule,
    },
    io::fd::handles,
};
use o3::cell::region;

use crate::{
    datagram::{self, drive, send},
    dispatch::typed::{self, arms},
};

pub(super) struct Submission;

impl Submission {
    pub(super) fn recv<'d, const ID: u8>(
        self,
        fd: &handles::DatagramDescriptor<'d>,
        slots: &flight::Slots<'d, datagram::RecvTag<ID>>,
        arming: arms::Arming<'_, 'd, datagram::RecvTag<ID>>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        use dope_core::driver::ops::Submit;

        let flight = Submit::submit_recv_datagram(driver, slots, fd, arming.identity());
        arming.resolve_submission(flight);
    }

    pub(super) fn send<'owner, 'd: 'owner, const ID: u8>(
        self,
        fd: &handles::DatagramDescriptor<'d>,
        slots: &flight::Slots<'d, datagram::SendTag<ID>>,
        send: &mut send::Send<'d>,
        key: table::Key<route::KeyTag<ID, { route::SEND }>>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let target = route::Space::for_driver(fd.driver()).bind_key(key);
        let submission = retained::raw::Submission::send_msg(fd, send.fill_message(), target);
        // SAFETY: `send` is already installed in Sender's fixed-capacity
        // in-flight slab and owns the payload, address, iovec, header, and
        // control storage referenced by the message. The exact key is removed
        // only after terminal completion; Socket owns the descriptor through
        // Endpoint finish and runtime quiescence.
        unsafe { retained::raw::Owner::submit(driver, slots, submission) }
    }
}

// SAFETY: Endpoint owns every retained socket phase through finish.
unsafe impl<'d, const ID: u8, H: datagram::Handler<'d, ID>> crate::dispatch::raw::Manifold<'d>
    for datagram::Endpoint<'d, ID, H>
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Plain;
    type Activate = crate::dispatch::raw::Plain;
    type PrePark = crate::dispatch::raw::Retained;
    type Shutdown = crate::dispatch::raw::Plain;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.project().socket.install(install);
    }

    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        event: crate::DriverEvent<'d>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        let this = self.project();
        this.socket.dispatch(event, this.handler, driver.turn_now());
        ops::ControlFlow::Continue(())
    }

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _target: typed::Token<'d, Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        self.project().socket.project().receive.resume_buffer();
    }

    unsafe fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let this = self.project();
        let mut socket = this.socket;
        if !socket.as_ref().get_ref().sender.accepts_work() {
            socket
                .as_mut()
                .project()
                .sender
                .drain(turn.reborrow().maintenance(), |payload| {
                    this.handler.recycle(payload);
                });
        }
        let now = driver.turn_now();
        this.handler
            .pre_park(socket.as_mut(), now, turn.reborrow().application());
        drive::Drive::run(socket, turn.application(), driver);
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let this = self.project_ref();
        this.socket
            .progress(region)
            .reduce(this.handler.progress(region))
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        let this = self.project();
        this.socket
            .shutdown(turn.maintenance(), this.handler, driver);
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        self.project().socket.finish(finish);
    }
}
