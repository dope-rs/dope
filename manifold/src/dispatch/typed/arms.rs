//! Typed activation arm state.

use std::{marker, mem};

use dope_core::driver::{self, flight, lifecycle, ops, route, route::kind, schedule};
use o3::cell::region;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
struct Phase(u8);

impl Phase {
    const READY: Self = Self(1);
    const SUBMITTING: Self = Self(2);
    const ARMED: Self = Self(3);
    const DEFERRED: Self = Self(4);
    const CANCEL_DEFERRED: Self = Self(5);
    const CANCELLING: Self = Self(6);
    const IDLE: Self = Self(7);
    const RETIRED: Self = Self(8);
    const WAITING: Self = Self(9);
}

#[repr(transparent)]
struct Word<'d, Tag: route::Tag>(route::Operation<'d, Tag>);

impl<'d, Tag: route::Tag> Word<'d, Tag> {
    fn new(target: route::Target<'d, Tag>) -> Self {
        Self(target.operation(Phase::READY.0))
    }

    fn phase(&self) -> Phase {
        Phase(self.0.kind())
    }

    fn set_phase(&mut self, phase: Phase) {
        self.0 = self.0.with_kind(phase.0);
    }

    fn identity(&self) -> route::Operation<'d, Tag> {
        self.0
    }

    fn dispatch(&self) -> route::Operation<'d, Tag> {
        self.0.with_kind(Tag::KIND)
    }
}

/// Result of matching a retained completion against its exact operation.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Disposition {
    Deliver,
    Discard,
}

/// One typed retained operation, including its exact route, kind, slot, and epoch.
pub(crate) struct OneShot;
pub(crate) struct MultiShot;

pub(crate) trait ArmTag: route::Tag {
    type Mode;
}

impl<const ID: u8> ArmTag for route::KeyTag<ID, { kind::ACCEPT }> {
    type Mode = OneShot;
}

impl<const ID: u8> ArmTag for route::KeyTag<ID, { kind::RECV }> {
    type Mode = MultiShot;
}

pub(crate) struct Arm<'d, Tag: ArmTag, Mode = <Tag as ArmTag>::Mode> {
    word: Word<'d, Tag>,
    flight: Option<flight::Flight<'d>>,
    _tag: marker::PhantomData<(Tag, Mode)>,
}

impl<'d, Tag: ArmTag, Mode> Arm<'d, Tag, Mode> {
    pub(crate) fn new(target: route::Target<'d, Tag>) -> Self {
        Self {
            word: Word::new(target),
            flight: None,
            _tag: marker::PhantomData,
        }
    }

    pub(crate) fn needs_arm(&self) -> bool {
        matches!(self.word.phase(), Phase::READY | Phase::DEFERRED)
    }

    pub(crate) fn progress<'r>(&self, region: &region::Token<'r>) -> schedule::Progress<'r> {
        match self.word.phase() {
            Phase::READY => schedule::Progress::Runnable,
            Phase::DEFERRED
            | Phase::ARMED
            | Phase::CANCEL_DEFERRED
            | Phase::CANCELLING
            | Phase::WAITING => schedule::Progress::waiting(region),
            Phase::SUBMITTING => schedule::Progress::Runnable,
            Phase::IDLE | Phase::RETIRED => schedule::Progress::Quiescent,
            _ => schedule::Progress::waiting(region),
        }
    }

    pub(crate) fn begin(&mut self) -> Option<Arming<'_, 'd, Tag, Mode>> {
        self.begin_if(|| true)
    }

    pub(crate) fn begin_if(
        &mut self,
        enabled: impl FnOnce() -> bool,
    ) -> Option<Arming<'_, 'd, Tag, Mode>> {
        if !self.needs_arm() || !enabled() {
            return None;
        }
        self.word.set_phase(Phase::SUBMITTING);
        Some(Arming { arm: self })
    }

    pub(crate) fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        match self.word.phase() {
            Phase::READY | Phase::SUBMITTING | Phase::DEFERRED | Phase::WAITING => {
                self.word.set_phase(Phase::IDLE)
            }
            Phase::ARMED => self.submit_cancel(driver),
            _ => {}
        }
    }

    pub(crate) fn retry_stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        if self.word.phase() == Phase::CANCEL_DEFERRED {
            self.submit_cancel(driver);
        }
    }

    pub(crate) fn has_in_flight(&self) -> bool {
        !matches!(
            self.word.phase(),
            Phase::READY | Phase::SUBMITTING | Phase::DEFERRED | Phase::IDLE | Phase::RETIRED
        )
    }

    pub(crate) fn finish_quiesced(&mut self, _finish: &mut lifecycle::Finalize<'_, '_>) {
        if let Some(flight) = self.flight.take() {
            let _ = flight.complete();
        }
        self.word.set_phase(Phase::IDLE);
    }

    fn submit_cancel(&mut self, driver: &mut driver::Context<'_, 'd>) {
        self.word.set_phase(Phase::CANCELLING);
        let target = self.word.dispatch();
        let Some(flight) = self.flight.as_mut() else {
            self.word.set_phase(Phase::IDLE);
            return;
        };
        if ops::Submit::cancel(driver, flight, target).is_err() {
            self.word.set_phase(Phase::CANCEL_DEFERRED);
        }
    }
}

impl<Tag: ArmTag> Arm<'_, Tag, MultiShot> {
    pub(crate) fn wait_resource(&mut self) -> bool {
        if self.word.phase() != Phase::READY {
            return false;
        }
        self.word.set_phase(Phase::WAITING);
        true
    }

    pub(crate) fn resume_resource(&mut self) -> bool {
        if self.word.phase() != Phase::WAITING {
            return false;
        }
        self.word.set_phase(Phase::READY);
        true
    }

    pub(crate) fn complete_multishot(&mut self, token: route::Token, more: bool) -> Disposition {
        if !self.word.dispatch().matches(token) {
            return Disposition::Discard;
        }
        match self.word.phase() {
            Phase::ARMED => {
                if !more {
                    if let Some(flight) = self.flight.take() {
                        let _ = flight.complete();
                    }
                    self.word.set_phase(Phase::READY);
                }
                Disposition::Deliver
            }
            Phase::CANCEL_DEFERRED | Phase::CANCELLING => {
                if !more {
                    if let Some(flight) = self.flight.take() {
                        let _ = flight.complete();
                    }
                    self.word.set_phase(Phase::IDLE);
                }
                Disposition::Discard
            }
            _ => Disposition::Discard,
        }
    }
}

impl<'d, Tag: ArmTag> Arm<'d, Tag, OneShot> {
    pub(crate) fn complete_oneshot(
        &mut self,
        token: route::Token,
    ) -> Option<Retirement<'_, 'd, Tag>> {
        if !self.word.dispatch().matches(token) {
            return None;
        }
        let terminal = match self.word.phase() {
            Phase::ARMED => {
                if let Some(flight) = self.flight.take() {
                    let _ = flight.complete();
                }
                self.word.set_phase(Phase::READY);
                Some(Terminal {
                    _arm: marker::PhantomData,
                })
            }
            Phase::CANCEL_DEFERRED | Phase::CANCELLING => {
                if let Some(flight) = self.flight.take() {
                    let _ = flight.complete();
                }
                self.word.set_phase(Phase::IDLE);
                None
            }
            _ => return None,
        };
        Some(Retirement { terminal })
    }
}

#[must_use]
pub(crate) struct Armed<'a, 'd, Tag: ArmTag> {
    _arm: marker::PhantomData<&'a mut Arm<'d, Tag, OneShot>>,
}

#[must_use]
pub(crate) struct Retirement<'a, 'd, Tag: ArmTag> {
    terminal: Option<Terminal<'a, 'd, Tag>>,
}

impl<'a, 'd, Tag: ArmTag> Retirement<'a, 'd, Tag> {
    pub(crate) fn into_terminal(self) -> Option<Terminal<'a, 'd, Tag>> {
        self.terminal
    }
}

#[must_use]
pub(crate) struct Terminal<'a, 'd, Tag: ArmTag> {
    _arm: marker::PhantomData<&'a mut Arm<'d, Tag, OneShot>>,
}

/// Linear permission to materialize and submit exactly one retained source.
#[must_use]
pub(crate) struct Arming<'a, 'd, Tag: ArmTag, Mode = <Tag as ArmTag>::Mode> {
    arm: &'a mut Arm<'d, Tag, Mode>,
}

impl<'d, Tag: ArmTag, Mode> Arming<'_, 'd, Tag, Mode> {
    pub(crate) fn identity(&self) -> route::Operation<'d, Tag> {
        self.arm.word.identity()
    }

    fn resolve_submission_inner(
        &mut self,
        flight: Result<flight::Flight<'d>, driver::SubmitError>,
    ) -> bool {
        match flight {
            Ok(flight) => {
                self.arm.flight = Some(flight);
                self.arm.word.set_phase(Phase::ARMED);
                true
            }
            Err(_) => {
                self.arm.word.set_phase(Phase::DEFERRED);
                false
            }
        }
    }
}

impl<'d, Tag: ArmTag> Arming<'_, 'd, Tag, MultiShot> {
    pub(crate) fn resolve_submission(
        mut self,
        flight: Result<flight::Flight<'d>, driver::SubmitError>,
    ) {
        self.resolve_submission_inner(flight);
    }
}

impl<'a, 'd, Tag: ArmTag> Arming<'a, 'd, Tag, OneShot> {
    pub(crate) fn resolve_submission(
        mut self,
        flight: Result<flight::Flight<'d>, driver::SubmitError>,
    ) -> Option<Armed<'a, 'd, Tag>> {
        self.resolve_submission_inner(flight).then_some(Armed {
            _arm: marker::PhantomData,
        })
    }
}

impl<Tag: ArmTag, Mode> Drop for Arming<'_, '_, Tag, Mode> {
    fn drop(&mut self) {
        if self.arm.word.phase() == Phase::SUBMITTING {
            self.arm.word.set_phase(Phase::READY);
        }
    }
}

type LayoutTag = route::KeyTag<1, { route::RECV }>;
const _: () = assert!(
    mem::size_of::<Arm<'static, LayoutTag>>()
        == mem::size_of::<(
            route::Operation<'static, LayoutTag>,
            Option<flight::Flight<'static>>,
        )>(),
);
const _: () =
    assert!(mem::size_of::<Arming<'static, 'static, LayoutTag>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Armed<'static, 'static, LayoutTag>>() == 0);
const _: () = assert!(mem::size_of::<Option<Armed<'static, 'static, LayoutTag>>>() == 1);
const _: () = assert!(mem::size_of::<Retirement<'static, 'static, LayoutTag>>() == 1);
const _: () = assert!(mem::size_of::<Option<Retirement<'static, 'static, LayoutTag>>>() == 1);
