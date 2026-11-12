//! Generation-bound request admission and final-link publication.

use std::{mem, task};

use data::compose;
use dope_core::driver::{
    self,
    route::{Identity, KeyTag},
    schedule::{app, clock, ready},
};
use dope_net::link::egress::{self, data};
use egress::{load::lease, reserve};
use o3::base::cell::region;
use reserve::{demand, exact};

use crate::client::{self, close, conn};

/// Static request-admission capability selected by an application.
pub trait Mode {}

/// Request admission is present. This type has no values or runtime state.
pub enum Enabled {}

/// Request admission is absent. This type has no values or runtime state.
pub enum Disabled {}

impl Mode for Enabled {}
impl Mode for Disabled {}

const _: () = {
    assert!(mem::size_of::<Enabled>() == 0);
    assert!(mem::size_of::<Disabled>() == 0);
};

/// Mutable capabilities for draining one exact connection generation.
/// Engine-private construction binds its state, egress, driver, wake target,
/// and close authority to the same slot.
pub struct Turn<'turn, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity = KeyTag<0>> {
    connection: conn::Id<'d, ID>,
    state: &'turn mut C,
    drain: Drain<'turn, 'e, 'd, B>,
    token: &'turn mut region::Token<region::Identity<'d, 1>>,
}

/// One-word reborrow of a turn's request-admission authority.
pub struct Requests<'view, 'turn, 'e, 'd, B: data::raw::Payload<'d>, ID: Identity = KeyTag<0>> {
    connection: conn::Id<'d, ID>,
    drain: &'view mut Drain<'turn, 'e, 'd, B>,
}

pub(in crate::client) struct Drain<'turn, 'e, 'd, B: data::raw::Payload<'d>> {
    queue: egress::Queue<'turn, 'e, 'd, { client::IOV_CAP }, B>,
    work: app::Work<'turn, 'd>,
    wake: ready::Target<'d>,
    region: &'turn mut region::Token<region::Identity<'d>>,
    state: State,
    close: Option<close::Directive>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(in crate::client) enum Flag {
    Exhausted = 1 << 1,
    Waiting = 1 << 2,
    WriteShutdown = 1 << 4,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::client) struct State(u8);

impl State {
    const EMPTY: Self = Self(0);

    pub(in crate::client) const fn contains(self, flag: Flag) -> bool {
        self.0 & flag as u8 != 0
    }

    fn insert(&mut self, flag: Flag) {
        self.0 |= flag as u8;
    }

    fn remove(&mut self, flag: Flag) {
        self.0 &= !(flag as u8);
    }
}

#[must_use]
pub struct Work<'permit, 'turn, 'e, 'd, B: data::raw::Payload<'d>> {
    drain: &'permit mut Drain<'turn, 'e, 'd, B>,
    permit: app::Permit<'turn, 'd>,
}

/// An owned payload whose final-link slot and geometry are already proven.
#[must_use]
pub struct Ready<'permit, 'turn, 'e, 'd, B: data::raw::Payload<'d>, S = B> {
    value: exact::Prepared<'turn, 'permit, 'e, 'd, B, demand::Fallible, S>,
}

#[must_use = "owned admission determines the payload's exact destination"]
pub enum Admission<'permit, 'turn, 'e, 'd, B: data::raw::Payload<'d>, S = B> {
    Ready(Ready<'permit, 'turn, 'e, 'd, B, S>),
    Saturated(egress::NonEmpty<S>),
    Rejected(exact::Unavailable<S>),
}

impl<'turn, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity> Turn<'turn, 'e, 'd, C, B, ID> {
    pub(in crate::client) fn new(
        conn: conn::Id<'d, ID>,
        state: &'turn mut C,
        queue: egress::Queue<'turn, 'e, 'd, { client::IOV_CAP }, B>,
        work: app::Work<'turn, 'd>,
        wake: ready::Target<'d>,
        driver: driver::Context<'turn, 'd>,
    ) -> Self {
        let (region, token) = driver.into_regions();
        Self {
            connection: conn,
            state,
            drain: Drain {
                queue,
                work,
                wake,
                region,
                state: State::EMPTY,
                close: None,
            },
            token,
        }
    }

    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.connection
    }

    pub fn state(&mut self) -> &mut C {
        self.state
    }

    pub fn now(&self) -> clock::Moment<'d> {
        self.drain.work.clock().now()
    }

    pub fn region(&mut self) -> &mut region::Token<region::Identity<'d>> {
        self.drain.region
    }

    pub fn requests(
        &mut self,
    ) -> (
        Requests<'_, 'turn, 'e, 'd, B, ID>,
        &mut region::Token<region::Identity<'d, 1>>,
    ) {
        (
            Requests {
                connection: self.connection,
                drain: &mut self.drain,
            },
            self.token,
        )
    }

    pub fn admit(
        &mut self,
        value: egress::NonEmpty<B>,
    ) -> Result<Admission<'_, 'turn, 'e, 'd, B>, egress::NonEmpty<B>> {
        self.drain.admit(value)
    }

    /// Requests retirement of this exact connection generation.
    pub fn close(&mut self, close: close::Directive) {
        self.drain.close(close);
    }

    /// Projects protocol state and the sole request-admission authority.
    pub fn parts(&mut self) -> (&mut C, Requests<'_, 'turn, 'e, 'd, B, ID>) {
        (
            &mut *self.state,
            Requests {
                connection: self.connection,
                drain: &mut self.drain,
            },
        )
    }

    pub(in crate::client) fn finish(self) -> (State, Option<close::Directive>) {
        self.drain.finish()
    }
}

impl<'turn, 'e, 'd, B: data::raw::Payload<'d>, ID: Identity> Requests<'_, 'turn, 'e, 'd, B, ID> {
    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.connection
    }

    pub fn now(&self) -> clock::Moment<'d> {
        self.drain.work.clock().now()
    }

    pub fn region(&mut self) -> &mut region::Token<region::Identity<'d>> {
        self.drain.region
    }

    pub fn admit<S>(
        &mut self,
        value: egress::NonEmpty<S>,
    ) -> Result<Admission<'_, 'turn, 'e, 'd, B, S>, egress::NonEmpty<S>> {
        self.drain.admit(value)
    }

    pub fn close(&mut self, close: close::Directive) {
        self.drain.close(close);
    }

    pub fn finish_write(&mut self) {
        self.drain.finish_write();
    }

    /// Detaches a source only after this turn admits its retained front.
    pub fn claim<'a, R: app::Drain<region::Token<region::Identity<'d>>>>(
        &'a mut self,
        ready: &'a mut R,
    ) -> Option<(R::Item<'a>, Work<'a, 'turn, 'e, 'd, B>)> {
        let drain = &mut *self.drain;
        match ready.next_when(drain.region, || drain.work.permit())? {
            Ok((value, permit)) => Some((value, Work { drain, permit })),
            Err(_) => {
                drain.state.insert(Flag::Exhausted);
                None
            }
        }
    }

    pub fn admit_front(&mut self) -> Option<Work<'_, 'turn, 'e, 'd, B>> {
        self.drain.admit_front()
    }
}

impl<'turn, 'e, 'd, B: data::raw::Payload<'d>> Drain<'turn, 'e, 'd, B> {
    /// Charges exactly one application-work unit before protocol validation.
    fn admit_front(&mut self) -> Option<Work<'_, 'turn, 'e, 'd, B>> {
        match self.work.permit() {
            Some(permit) => Some(Work {
                drain: self,
                permit,
            }),
            None => {
                self.state.insert(Flag::Exhausted);
                None
            }
        }
    }

    fn admit<S>(
        &mut self,
        value: egress::NonEmpty<S>,
    ) -> Result<Admission<'_, 'turn, 'e, 'd, B, S>, egress::NonEmpty<S>> {
        match self.admit_front() {
            Some(work) => Ok(work.admit(value)),
            None => Err(value),
        }
    }

    fn finish(self) -> (State, Option<close::Directive>) {
        if !self.state.contains(Flag::Waiting) {
            self.queue.cancel_wait(self.region);
        }
        (self.state, self.close)
    }

    fn close(&mut self, close: close::Directive) {
        self.close = Some(match self.close {
            Some(current) => current.merge(close),
            None => close,
        });
    }

    fn finish_write(&mut self) {
        self.state.insert(Flag::WriteShutdown);
    }
}

pub(in crate::client) enum Capacity {
    Waiting,
    Rejected(exact::Failure),
}

impl<'e, 'd, B: data::raw::Payload<'d>> Ready<'_, '_, 'e, 'd, B> {
    pub fn push(self) {
        self.value.publish();
    }

    pub fn push_with(self, commit: impl FnOnce(&mut region::Token<region::Identity<'d>>)) {
        self.value.publish_with(commit);
    }
}

impl<'d, L: data::raw::Payload<'d>, R: data::raw::Payload<'d>, U: lease::Commit<Item = L>>
    Ready<'_, '_, '_, 'd, compose::Either<L, R>, lease::Lease<U>>
{
    pub fn commit_left(self) {
        self.value.commit_left();
    }
}

impl<'d, L: data::raw::Payload<'d>, R: data::raw::Payload<'d>, U: lease::Commit<Item = R>>
    Ready<'_, '_, '_, 'd, compose::Either<L, R>, lease::Lease<U>>
{
    pub fn commit_right(self) {
        self.value.commit_right();
    }
}

impl<'permit, 'turn, 'e, 'd, B: data::raw::Payload<'d>> Work<'permit, 'turn, 'e, 'd, B> {
    pub fn admit<S>(self, value: egress::NonEmpty<S>) -> Admission<'permit, 'turn, 'e, 'd, B, S> {
        let Self {
            drain,
            permit: _permit,
        } = self;
        let plan = match drain.queue.offer(value) {
            Ok(plan) => plan,
            Err(rejected) => {
                drain.state.remove(Flag::Waiting);
                return Admission::Rejected(rejected);
            }
        };
        match plan.poll_reserve(drain.region, drain.wake) {
            exact::Outcome::Ready(value) => {
                drain.state.remove(Flag::Waiting);
                Admission::Ready(Ready { value })
            }
            exact::Outcome::Pending(value) => {
                drain.state.insert(Flag::Waiting);
                Admission::Saturated(value)
            }
            exact::Outcome::Rejected(rejected) => {
                drain.state.remove(Flag::Waiting);
                Admission::Rejected(rejected)
            }
        }
    }

    pub(in crate::client) fn region(&mut self) -> &mut region::Token<region::Identity<'d>> {
        self.drain.region
    }

    pub(in crate::client) fn move_with(
        self,
        transfer: impl FnOnce(
            &mut region::Token<region::Identity<'d>>,
            &egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, B>,
            ready::Target<'d>,
        ) -> Option<task::Poll<Result<(), exact::Failure>>>,
    ) -> Option<Result<(), Capacity>> {
        let Self {
            drain,
            permit: _permit,
        } = self;
        let moved = transfer(drain.region, &drain.queue, drain.wake)?;
        Some(match moved {
            task::Poll::Ready(Ok(())) => {
                drain.state.remove(Flag::Waiting);
                Ok(())
            }
            task::Poll::Pending => {
                drain.state.insert(Flag::Waiting);
                Err(Capacity::Waiting)
            }
            task::Poll::Ready(Err(cause)) => {
                drain.state.remove(Flag::Waiting);
                Err(Capacity::Rejected(cause))
            }
        })
    }
}

const _: () = {
    assert!(mem::size_of::<State>() == mem::size_of::<u8>());
    assert!(
        mem::size_of::<Work<'static, 'static, 'static, 'static, &'static [u8]>>()
            == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Work<'static, 'static, 'static, 'static, &'static [u8]>>>()
            == mem::size_of::<Work<'static, 'static, 'static, 'static, &'static [u8]>>()
    );
    assert!(
        mem::size_of::<Requests<'static, 'static, 'static, 'static, &'static [u8], KeyTag<0>>>()
            == mem::size_of::<conn::Id<'static, KeyTag<0>>>() + mem::size_of::<usize>()
    );
};
