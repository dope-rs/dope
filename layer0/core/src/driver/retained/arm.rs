use std::mem;

use o3::base::cell::region;

use crate::{
    driver,
    driver::{
        life::finish,
        result, retained,
        retained::{flight, flight::cancel},
        route,
        route::{indices, kind},
        schedule,
    },
    io::{event::batch::Flow, fd::handles},
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Ready,
    Armed,
    Deferred,
    CancelDeferred,
    Cancelling,
    CancelRejected,
    Idle,
    Waiting,
    Probing,
    ProbeDeferred,
}

pub(super) struct Base<'d, Tag: route::Tag> {
    epoch: route::Epoch,
    slot: indices::SlotIndex,
    driver: route::Brand<'d, Tag>,
    phase: Phase,
    flight: Option<flight::Flight<'d, Tag::Class>>,
}

pub(super) enum End {
    Stale,
    Terminal,
    Cancelled,
}

pub(super) enum Start {
    Ready,
    Blocked,
}

impl<'d, Tag: route::Tag> Base<'d, Tag> {
    pub(super) fn new(target: route::Target<'d, Tag>) -> Self {
        Self {
            epoch: target.epoch(),
            slot: target.slot(),
            driver: target.driver,
            phase: Phase::Ready,
            flight: None,
        }
    }

    pub(super) fn progress<'a>(
        &self,
        region: &region::Token<region::Identity<'a>>,
    ) -> schedule::Progress<'a> {
        match self.phase {
            Phase::Ready | Phase::ProbeDeferred | Phase::CancelDeferred => {
                schedule::Progress::Runnable
            }
            Phase::Deferred
            | Phase::Armed
            | Phase::Probing
            | Phase::Cancelling
            | Phase::CancelRejected
            | Phase::Waiting => schedule::Progress::waiting(region),
            Phase::Idle => schedule::Progress::Quiescent,
        }
    }

    /// Each caller retains an exclusive arming loan, so its abandoned outcome
    /// can be installed before native submission without a transient phase.
    pub(super) fn begin(&mut self) -> Start {
        if !matches!(self.phase, Phase::Ready | Phase::Deferred) {
            return Start::Blocked;
        }
        self.phase = Phase::Ready;
        Start::Ready
    }

    fn begin_probe(&mut self) -> Start {
        if !matches!(
            self.phase,
            Phase::Ready | Phase::Deferred | Phase::ProbeDeferred
        ) {
            return Start::Blocked;
        }
        self.phase = Phase::ProbeDeferred;
        Start::Ready
    }

    pub(super) fn operation(&self) -> route::Operation<'d, Tag> {
        route::Space {
            driver: self.driver,
        }
        .bind(self.slot, self.epoch)
        .dispatch()
    }

    pub(super) fn apply(
        &mut self,
        flight: Result<flight::Flight<'d, Tag::Class>, result::SubmitError>,
    ) -> Submission {
        let probing = self.phase == Phase::ProbeDeferred;
        match flight {
            Ok(flight) => {
                self.flight = Some(flight);
                self.phase = if probing {
                    Phase::Probing
                } else {
                    Phase::Armed
                };
                Submission::Armed
            }
            Err(error) => match error.cause() {
                result::SubmitCause::Full => {
                    self.phase = if probing {
                        Phase::ProbeDeferred
                    } else {
                        Phase::Deferred
                    };
                    Submission::Deferred
                }
                result::SubmitCause::IdentityExhausted => {
                    self.phase = Phase::Idle;
                    Submission::Failed(error)
                }
            },
        }
    }

    pub(super) fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        match self.phase {
            Phase::Ready | Phase::Deferred | Phase::Waiting | Phase::ProbeDeferred => {
                self.phase = Phase::Idle
            }
            Phase::Armed | Phase::Probing | Phase::CancelDeferred => self.cancel(driver),
            Phase::Cancelling | Phase::CancelRejected | Phase::Idle => {}
        }
    }

    pub(super) fn is_live(&self) -> bool {
        !matches!(
            self.phase,
            Phase::Ready | Phase::Deferred | Phase::ProbeDeferred | Phase::Idle
        )
    }

    pub(super) fn finish(&mut self, proof: &mut finish::Finish<'_, 'd>) {
        if let Some(flight) = self.flight.take() {
            flight.finish(proof);
        }
        self.phase = Phase::Idle;
    }

    pub(super) fn retire(&mut self, token: route::Erased<'d, Tag::Class>) -> End {
        if !self.operation().matches(token) {
            return End::Stale;
        }
        let (phase, end) = match self.phase {
            Phase::Armed => (Phase::Ready, End::Terminal),
            Phase::CancelDeferred | Phase::Cancelling | Phase::CancelRejected => {
                (Phase::Idle, End::Cancelled)
            }
            Phase::Ready
            | Phase::Deferred
            | Phase::Idle
            | Phase::Waiting
            | Phase::Probing
            | Phase::ProbeDeferred => return End::Stale,
        };
        let Some(flight) = self.flight.take_if(|flight| !flight.is_pending()) else {
            return End::Stale;
        };
        drop(flight);
        self.phase = phase;
        end
    }

    fn cancel(&mut self, driver: &mut driver::Context<'_, 'd>) {
        let Some(mut flight) = self.flight.take() else {
            self.phase = Phase::Idle;
            return;
        };
        let (phase, flight, rejected) = match flight.try_stop(driver) {
            Ok(cancel::Outcome::Submitted | cancel::Outcome::Queued) => {
                (Phase::Cancelling, Some(flight), None)
            }
            Ok(cancel::Outcome::Completed) => {
                drop(flight);
                (Phase::Idle, None, None)
            }
            Ok(cancel::Outcome::Deferred) => (Phase::CancelDeferred, Some(flight), None),
            Err(failure) => (Phase::CancelRejected, Some(flight), Some(failure)),
        };
        self.phase = phase;
        self.flight = flight;
        if let Some(failure) = rejected {
            driver.rejected(failure);
        }
    }
}

#[must_use]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Disposition {
    Deliver,
    Discard,
    Probe,
}

#[must_use]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Submission {
    Armed,
    Deferred,
    /// Admission stopped permanently without submitting a native operation.
    Failed(result::SubmitError),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Resume {
    Resumed,
    Unchanged,
}

#[repr(transparent)]
pub struct State<'d, Tag: route::Tag>(Base<'d, Tag>);

impl<'d, Tag: route::Tag> State<'d, Tag> {
    pub fn new(target: route::Target<'d, Tag>) -> Self {
        Self(Base::new(target))
    }

    pub fn progress<'a>(
        &self,
        region: &region::Token<region::Identity<'a>>,
    ) -> schedule::Progress<'a> {
        self.0.progress(region)
    }

    pub fn begin(&mut self) -> Option<Arming<'_, 'd, Tag>> {
        match self.0.begin() {
            Start::Ready => Some(Arming { state: self }),
            Start::Blocked => None,
        }
    }

    pub fn begin_retry(&mut self) -> Option<Arming<'_, 'd, Tag>> {
        match self.0.begin_probe() {
            Start::Ready => Some(Arming { state: self }),
            Start::Blocked => None,
        }
    }

    pub fn wait(&mut self) {
        if self.0.phase == Phase::Ready {
            self.0.phase = Phase::Waiting;
        }
    }

    pub fn resume(&mut self) {
        if self.0.phase == Phase::Waiting {
            self.0.phase = Phase::Ready;
        }
    }

    /// Restarts a stopped operation only after its native flight has retired.
    pub fn restart(&mut self) {
        if self.0.phase == Phase::Idle {
            self.0.phase = Phase::Ready;
        }
    }

    pub fn try_resume(&mut self) -> Resume {
        if self.0.phase != Phase::Waiting {
            return Resume::Unchanged;
        }
        self.0.phase = Phase::Ready;
        Resume::Resumed
    }

    pub fn settle_probe(&mut self) {
        if self.0.phase == Phase::Probing {
            self.0.phase = Phase::Armed;
        }
    }

    pub fn complete(&mut self, token: route::Erased<'d, Tag::Class>, flow: Flow) -> Disposition {
        if !self.0.operation().matches(token) {
            return Disposition::Discard;
        }
        let (phase, result) = match self.0.phase {
            Phase::Armed => (Phase::Ready, Disposition::Deliver),
            Phase::Probing => (Phase::Ready, Disposition::Probe),
            Phase::CancelDeferred | Phase::Cancelling | Phase::CancelRejected => {
                (Phase::Idle, Disposition::Discard)
            }
            Phase::Ready
            | Phase::Deferred
            | Phase::Idle
            | Phase::Waiting
            | Phase::ProbeDeferred => return Disposition::Discard,
        };
        if flow == Flow::Last {
            let Some(flight) = self.0.flight.take_if(|flight| !flight.is_pending()) else {
                return Disposition::Discard;
            };
            drop(flight);
            self.0.phase = phase;
        }
        result
    }

    pub fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        self.0.stop(driver);
    }

    pub fn is_live(&self) -> bool {
        self.0.is_live()
    }

    pub fn finish(&mut self, proof: &mut finish::Finish<'_, 'd>) {
        self.0.finish(proof);
    }
}

#[must_use]
pub struct Arming<'a, 'd, Tag: route::Tag> {
    state: &'a mut State<'d, Tag>,
}

impl<'d, Tag: route::Tag> Arming<'_, 'd, Tag> {
    pub fn accept(
        self,
        driver: &mut driver::Context<'_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
    ) -> Submission
    where
        Tag: route::Tag<Class = kind::Accept>,
    {
        if !matches!(
            retained::accept::Readiness::from(driver.driver_ref()),
            retained::accept::Readiness::Ready
        ) {
            return Submission::Deferred;
        }
        let flight = slots.accept(driver.reborrow(), fd, self.state.0.operation());
        self.state.0.apply(flight)
    }

    pub fn recv<'owner>(
        self,
        driver: &mut retained::Context<'_, 'owner, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Datagram<'d>,
    ) -> Submission
    where
        Tag: route::Tag<Class = kind::Recv>,
    {
        let flight = slots.recv_dgram(driver.driver().reborrow(), fd, self.state.0.operation());
        self.state.0.apply(flight)
    }
}

const _: () = {
    type Route = route::KeyTag<1, kind::Recv>;
    assert!(mem::size_of::<Phase>() == 1);
    assert!(mem::size_of::<State<'static, Route>>() == 24);
    assert!(mem::size_of::<Option<State<'static, Route>>>() == 24);
    assert!(mem::align_of::<State<'static, Route>>() == mem::align_of::<route::Epoch>());
    assert!(
        mem::size_of::<State<'static, Route>>()
            == mem::size_of::<(
                route::Operation<'static, Route>,
                Option<flight::Flight<'static>>,
            )>()
    );
    assert!(mem::size_of::<Arming<'static, 'static, Route>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Submission>() == 1);
};
