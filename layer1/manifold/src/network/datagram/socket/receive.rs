use std::{net, ops::ControlFlow, pin, task::Poll};

use dope_core::{
    driver::{
        self,
        life::finish,
        receive, result,
        retained::{self, arm, flight},
        route::{self, Identity, kind::Recv},
        schedule::{self, app, ready},
    },
    io::{
        self,
        datagram::{Decoded, ReceiveError, Residency},
        event::{
            self,
            net::recv::{Completion, Outcome},
        },
        fd::handles,
        recv,
    },
};
use o3::{base::cell::region, mem::quota};

use crate::network::datagram::packet;

/// Packet delivery begins only after the mutable receive-state loan has ended.
pub(in crate::network::datagram) enum Received<'d> {
    Consumed,
    Packet {
        source: net::SocketAddr,
        payload: recv::Lease<'d>,
    },
    Failed(ReceiveError),
}

impl<'d> Received<'d> {
    pub(in crate::network::datagram) fn dispatch<'turn>(
        self,
        receive: impl FnOnce(Result<(net::SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>),
    ) {
        match self {
            Self::Consumed => {}
            Self::Packet { source, payload } => receive(Ok((source, packet::Packet::new(payload)))),
            Self::Failed(error) => receive(Err(error)),
        }
    }
}

#[pin_project::pin_project]
pub(in crate::network::datagram) struct Receive<'d, ID: Identity> {
    arm: arm::State<'d, <ID as route::Materialize<Recv>>::With>,
    retention: packet::Retainer<'d>,
    ready: ready::Handle<'d>,
    retry: Option<receive::Attempt<'d>>,
}

impl<'d, ID: Identity> Receive<'d, ID> {
    pub(in crate::network::datagram) fn new(
        target: route::Target<'d, <ID as route::Materialize<Recv>>::With>,
        fd: &handles::Datagram<'d>,
        keep: packet::Retainer<'d>,
    ) -> Self {
        let ready = fd.ready_handle();
        ready.set_target(target.dispatch());
        Self {
            arm: arm::State::new(target),
            retention: keep,
            ready,
            retry: None,
        }
    }

    pub(in crate::network::datagram) fn arm<'turn, 'owner>(
        self: pin::Pin<&mut Self>,
        fd: &handles::Datagram<'d>,
        flights: &flight::Slots<'d, <ID as route::Materialize<Recv>>::With>,
        work: app::Work<'turn, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<(), result::SubmitError>
    where
        'd: 'owner,
    {
        let this = self.project();
        let arm = this.arm;
        let result = work.run(|| {
            let arming = if this.retry.is_some() {
                arm.begin_retry()
            } else {
                arm.begin()
            };
            let arming = arming?;
            let retry = this.retry.take();
            let sent = arming.recv(driver, flights, fd);
            match sent {
                arm::Submission::Armed => {
                    if let Some(retry) = retry {
                        retry.commit();
                    }
                }
                arm::Submission::Deferred => *this.retry = retry,
                arm::Submission::Failed(error) => {
                    receive::Registration::clear(this.ready.target());
                    return Some(Err(error));
                }
            }
            Some(Ok(()))
        });
        match result {
            quota::Admission::Item(result) => result,
            quota::Admission::Empty | quota::Admission::Exhausted => Ok(()),
        }
    }

    pub(in crate::network::datagram) fn route(
        self: pin::Pin<&mut Self>,
        event: io::Event<'d>,
    ) -> ControlFlow<io::Event<'d>, Received<'d>> {
        let done = match event.into_kind() {
            event::Kind::Recv(done) if route::Id::try_from(done.token().route()) == Ok(ID::ID) => {
                done
            }
            event => return ControlFlow::Break(event.into()),
        };
        self.complete(done).map_break(Completion::retire)
    }

    pub(in crate::network::datagram) fn complete(
        self: pin::Pin<&mut Self>,
        done: Completion<'d>,
    ) -> ControlFlow<Completion<'d>, Received<'d>> {
        let this = self.project();
        let mode = this.arm.complete(done.token(), done.flow());
        if mode == arm::Disposition::Discard {
            return ControlFlow::Break(done);
        }
        done.dispatch(|_, _, outcome| {
            let buffer = match outcome {
                Outcome::Data(buffer) => {
                    if mode == arm::Disposition::Probe {
                        let target = this.ready.target();
                        match receive::Registration::try_acquire(target) {
                            Ok(reg) => {
                                if let Err(receive::Stale) = reg.cancel() {
                                    this.arm.settle_probe();
                                    return ControlFlow::Break(Outcome::Data(buffer));
                                }
                                this.arm.settle_probe();
                            }
                            Err(receive::Absent) => this.arm.settle_probe(),
                        }
                    }
                    buffer
                }
                Outcome::Exhausted(proof) => {
                    let target = this.ready.target();
                    let reg = match receive::Registration::try_acquire(target) {
                        Ok(reg) => reg,
                        Err(receive::Absent) => {
                            return ControlFlow::Break(Outcome::Exhausted(proof));
                        }
                    };
                    match reg.exhausted(proof) {
                        Ok(Poll::Ready(retry)) => {
                            if this.retry.is_none() {
                                *this.retry = Some(retry);
                            }
                        }
                        Ok(Poll::Pending) => {
                            this.arm.wait();
                        }
                        Err(stopped) => {
                            let (state, receive::Stale) = stopped;
                            return ControlFlow::Break(Outcome::Exhausted(state));
                        }
                    }
                    return ControlFlow::Continue(Received::Consumed);
                }
                Outcome::Failed(error) => {
                    if mode == arm::Disposition::Probe {
                        Self::settle(this.arm, this.ready.target());
                    }
                    return ControlFlow::Continue(Received::Failed(ReceiveError::Socket(error)));
                }
                control @ (Outcome::Eof | Outcome::Cancelled | Outcome::Starved(_)) => {
                    if mode == arm::Disposition::Probe {
                        Self::settle(this.arm, this.ready.target());
                    }
                    return ControlFlow::Break(control);
                }
            };
            ControlFlow::Continue(match Decoded::decode(buffer) {
                Decoded::Packet { source, payload } => Received::Packet { source, payload },
                Decoded::Address { error, .. } => Received::Failed(ReceiveError::Address(error)),
                Decoded::Rejected(failed) => {
                    Received::Failed(ReceiveError::Rejected(failed.release()))
                }
                Decoded::Truncated(failed) => {
                    Received::Failed(ReceiveError::Truncated(failed.release()))
                }
            })
        })
    }

    fn settle(
        arm: &mut arm::State<'d, <ID as route::Materialize<Recv>>::With>,
        target: ready::Target<'d>,
    ) {
        receive::Registration::clear(target);
        arm.settle_probe();
    }

    pub(in crate::network::datagram) fn retainer(&self) -> packet::Retainer<'d> {
        self.retention
    }

    pub(in crate::network::datagram) fn residency(&self) -> Option<Residency> {
        self.retention.residency()
    }

    pub(in crate::network::datagram) fn wake_target(&self) -> ready::Target<'d> {
        self.ready.target()
    }

    pub(in crate::network::datagram) fn is_live(&self) -> bool {
        self.arm.is_live()
    }

    pub(in crate::network::datagram) fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        self.arm.progress(region)
    }

    pub(in crate::network::datagram) fn stop(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.project();
        let target = this.ready.target();
        receive::Registration::clear(target);
        *this.retry = None;
        this.arm.stop(driver);
    }

    pub(in crate::network::datagram) fn resume(self: pin::Pin<&mut Self>) {
        let this = self.project();
        let target = this.ready.target();
        match receive::Registration::try_acquire(target) {
            Ok(reg) => match reg.resume() {
                Ok(Poll::Ready(retry)) => {
                    if this.retry.is_none() && this.arm.try_resume() == arm::Resume::Resumed {
                        *this.retry = Some(retry);
                    }
                }
                Ok(Poll::Pending) => (),
                Err(receive::Stale) => {
                    this.arm.resume();
                }
            },
            Err(receive::Absent) => this.arm.resume(),
        }
    }

    pub(in crate::network::datagram) fn retry_stop(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.project().arm.stop(driver);
    }

    pub(in crate::network::datagram) fn finish(
        self: pin::Pin<&mut Self>,
        proof: &mut finish::Finish<'_, 'd>,
    ) {
        let this = self.project();
        let target = this.ready.target();
        receive::Registration::clear(target);
        *this.retry = None;
        this.arm.finish(proof);
    }
}
