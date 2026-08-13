mod engine;
mod state;

use std::mem;

use dope_core::driver::{route, schedule, schedule::ready};
use dope_net::{
    link::{pool, slot::types},
    wire,
};
pub use engine::Engine;

use crate::{
    connector::lifecycle,
    dispatch::typed::{self, identity},
};

struct Kind;

/// One connector connection, branded by driver lifetime and route.
///
/// Route IDs are part of the type, so a connection cannot be sent to a
/// different connector route:
///
/// ```compile_fail
/// use dope_manifold::connector::connection::Id;
///
/// fn cross_route<'d>(id: Id<'d, 1>) -> Id<'d, 2> {
///     id
/// }
/// ```
///
/// Listener and connector identities remain distinct even on the same route:
///
/// ```compile_fail
/// use dope_manifold::{connector, listener};
///
/// fn cross_family<'d>(id: listener::connection::Id<'d, 1>)
///     -> connector::connection::Id<'d, 1>
/// {
///     id
/// }
/// ```
///
/// The invariant driver brand cannot be widened:
///
/// ```compile_fail
/// use dope_manifold::connector::connection::Id;
///
/// fn widen<'short, 'long>(id: Id<'short, 1>) -> Id<'long, 1>
/// where
///     'long: 'short,
/// {
///     id
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Id<'d, const ID: u8>(typed::Id<'d, ID, Kind>);

const _: () = assert!(mem::size_of::<Id<'static, 0>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Id<'static, 0>>() == mem::align_of::<route::Token>());

impl<'d, const ID: u8> Id<'d, ID> {
    pub(in crate::connector) fn from_key(key: pool::Key<'d, ID>) -> Self {
        Self(typed::Id::from_key(key))
    }

    pub const fn index(self) -> usize {
        self.0.key.index()
    }

    pub(in crate::connector) const fn key(self) -> pool::Key<'d, ID> {
        self.0.key
    }
}

impl<const ID: u8> identity::Brand for Id<'_, ID> {}

impl<const ID: u8> identity::Identity for Id<'_, ID> {
    fn index(self) -> usize {
        self.0.key.index()
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum CloseState {
    Open,
    Reconnect,
    Permanent,
    Draining,
}

pub(super) struct Closing {
    state: CloseState,
    reason: Option<lifecycle::CloseReason>,
}

pub(super) enum Retirement {
    Reconnect(lifecycle::CloseReason),
    Permanent,
}

const _: () = assert!(mem::size_of::<Closing>() <= mem::size_of::<u16>());

pub(crate) use state::State;

impl Closing {
    pub(super) fn request(&mut self, reason: lifecycle::CloseReason) -> lifecycle::CloseReason {
        let reason = match self.reason {
            Some(existing) => existing,
            None => {
                self.reason = Some(reason);
                reason
            }
        };
        if matches!(self.state, CloseState::Open) {
            self.state = CloseState::Reconnect;
        }
        reason
    }

    pub(super) fn request_permanent(&mut self) {
        if !matches!(self.state, CloseState::Draining) {
            self.state = CloseState::Permanent;
        }
    }

    pub(super) fn is_draining(&self) -> bool {
        matches!(self.state, CloseState::Draining)
    }

    pub(super) fn retire(&mut self, reason: lifecycle::CloseReason) -> Option<Retirement> {
        let retirement = match self.state {
            CloseState::Open | CloseState::Reconnect => Retirement::Reconnect(reason),
            CloseState::Permanent => Retirement::Permanent,
            CloseState::Draining => return None,
        };
        self.reason = Some(reason);
        self.state = CloseState::Draining;
        Some(retirement)
    }

    fn reason_mut(&mut self) -> &mut Option<lifecycle::CloseReason> {
        &mut self.reason
    }
}

#[repr(transparent)]
pub struct Ref<'a, 'd, const ID: u8, W: wire::Wire, C, O = ()> {
    slot: &'a types::Connection<'d, ID, W, State<C, O>>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, C, O> Ref<'a, 'd, ID, W, C, O> {
    pub(super) const fn new(slot: &'a types::Connection<'d, ID, W, State<C, O>>) -> Self {
        Self { slot }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(typed::Id::from_key(self.slot.key()))
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }
}

pub struct Ctx<'a, 'd, const ID: u8, W: wire::Wire, C, O = ()> {
    slot: &'a mut types::Connection<'d, ID, W, State<C, O>>,
    work: schedule::Application<'a, 'd>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, C, O> Ctx<'a, 'd, ID, W, C, O> {
    pub(super) const fn new(
        slot: &'a mut types::Connection<'d, ID, W, State<C, O>>,
        work: schedule::Application<'a, 'd>,
    ) -> Self {
        Self { slot, work }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(typed::Id::from_key(self.slot.key()))
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.slot.state.conn
    }

    /// Returns the exact application-turn capability carried by this callback.
    pub fn application_work(&self) -> schedule::Application<'a, 'd> {
        self.work
    }

    /// Returns the exact driver- and generation-branded readiness target.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.slot.io().wake_target()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Id<'d, ID>,
        &'a mut C,
        &'a mut Option<lifecycle::CloseReason>,
        schedule::Application<'a, 'd>,
    ) {
        let id = Id(typed::Id::from_key(self.slot.key()));
        let State { conn, closing, .. } = &mut self.slot.state;
        (id, conn, closing.reason_mut(), self.work)
    }
}

const _: () = assert!(
    mem::size_of::<Ctx<'static, 'static, 0, wire::Identity, ()>>()
        == 2 * mem::size_of::<&'static mut ()>()
);
const _: () = assert!(
    mem::size_of::<Ref<'static, 'static, 0, wire::Identity, ()>>() == mem::size_of::<&'static ()>()
);
