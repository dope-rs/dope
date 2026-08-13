use std::{net, process};

use dope_core::{
    driver::{
        self, flight, lifecycle, retained, route,
        schedule::{self, ready},
    },
    io::{datagram, fd::handles, recv},
};
use o3::cell::region;

use crate::{datagram::packet, dispatch::typed::arms};

pub(super) struct Receive<'d, const ID: u8> {
    arm: arms::Arm<'d, super::RecvTag<ID>>,
    flights: flight::Slots<'d, super::RecvTag<ID>>,
    retention: packet::Retention<'d>,
    ready: ready::Handle<'d>,
    target: route::Operation<'d, super::RecvTag<ID>>,
}

impl<'d, const ID: u8> Receive<'d, ID> {
    pub(super) fn new(
        target: route::Target<'d, super::RecvTag<ID>>,
        descriptor: &handles::DatagramDescriptor<'d>,
        flights: flight::Slots<'d, super::RecvTag<ID>>,
        retention: packet::Retention<'d>,
    ) -> Self {
        Self {
            arm: arms::Arm::new(target),
            flights,
            retention,
            ready: descriptor.ready_handle(),
            target: target.dispatch(),
        }
    }

    pub(super) fn arm<'turn, 'owner>(
        &mut self,
        descriptor: &handles::DatagramDescriptor<'d>,
        work: schedule::Application<'turn, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) where
        'd: 'owner,
    {
        let Self { arm, flights, .. } = self;
        let Some(arming) = arm.begin() else {
            return;
        };
        if !work.take() {
            return;
        }
        super::Submission.recv(descriptor, flights, arming, driver);
    }

    pub(super) fn complete(
        &mut self,
        token: route::Token,
        more: bool,
        event: crate::RecvEvent<'d>,
    ) -> Option<(net::SocketAddr, recv::View<'d>)> {
        use dope_core::io::RecvEvent;

        if self.arm.complete_multishot(token, more) == arms::Disposition::Discard {
            return None;
        }
        let buffer = match event {
            RecvEvent::Data(buffer) => buffer,
            RecvEvent::BufferExhausted => {
                let waiting = self.arm.wait_resource();
                let armed = waiting && self.ready.arm_recv_buffer(self.target);
                if !armed {
                    process::abort();
                }
                return None;
            }
            RecvEvent::Failed(_) | RecvEvent::Eof | RecvEvent::Cancelled | RecvEvent::Starved => {
                return None;
            }
        };
        match datagram::Decoded::decode(buffer) {
            datagram::Decoded::Packet { source, payload } => Some((source, payload)),
            datagram::Decoded::Malformed(_) | datagram::Decoded::Truncated(_) => None,
        }
    }

    pub(super) fn retain<'turn>(
        &self,
        packet: packet::Packet<'turn, 'd>,
    ) -> Result<packet::Retained<'d>, packet::Packet<'turn, 'd>> {
        self.retention.retain(packet)
    }

    pub(super) fn retainer<'turn>(&'turn self) -> packet::Retainer<'turn, 'd> {
        packet::Retainer::new(&self.retention)
    }

    pub(super) fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        self.arm.progress(region)
    }

    pub(super) fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        self.ready.cancel_recv_buffer(self.target);
        self.arm.stop(driver);
    }

    pub(super) fn resume_buffer(&mut self) {
        let Some(credit) = self.ready.take_recv_buffer(self.target) else {
            return;
        };
        if self.arm.resume_resource() {
            credit.consume();
        }
    }

    pub(super) fn retry_stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        self.arm.retry_stop(driver);
    }

    pub(super) fn finish(&mut self, context: &mut lifecycle::Finalize<'_, 'd>) {
        self.arm.finish_quiesced(context);
    }
}
