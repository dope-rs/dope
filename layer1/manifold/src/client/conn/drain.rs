use dope_core::driver::route::Identity;
use dope_net::{
    Transport,
    link::{
        egress::reserve::demand,
        pool::{self, change, recv::input},
        slot::types,
    },
    wire,
};

use crate::client::{
    app, attempt,
    close::{Directive, Reason},
    conn::{self, State, claim, closing},
};

pub(in crate::client) struct OwnedDrain<'d, ID: Identity> {
    owner: attempt::Id<'d, ID>,
    drain: Drain<'d, ID>,
}

#[must_use = "a committed close decision must remain inside its pool claim"]
pub(in crate::client) struct Commit<'d, ID: Identity, C> {
    pub(super) app: Application<'d, ID, C>,
    pub(super) rule: Directive,
}

#[must_use = "a retired drain must transfer its exact connection and decision"]
pub(in crate::client) struct Retired<'d, ID: Identity, C> {
    app: Application<'d, ID, C>,
    rule: Directive,
}

impl<'d, ID: Identity, C> Retired<'d, ID, C> {
    pub(in crate::client) fn resolve<A, V>(
        self,
        resolve: impl FnOnce(Directive) -> (Reason, V),
        notify: impl FnOnce(conn::Retired<'d, ID, A>, &mut C),
    ) -> V
    where
        A: app::Application<'d, ID, Conn = C>,
    {
        let (reason, value) = resolve(self.rule);
        match self.app {
            Application::Detached => {}
            Application::Drained {
                proof,
                mut conn,
                transmit,
            } => {
                notify(conn::Retired::new(proof, reason, transmit), &mut conn);
            }
        }
        value
    }
}

#[must_use = "application retirement state must reach the connection"]
pub(in crate::client) enum Application<'d, ID: Identity, C> {
    Detached,
    Drained {
        proof: app::Quiesced<'d, ID>,
        conn: C,
        transmit: Option<conn::Transmit>,
    },
}

pub(in crate::client) trait BindOwner<'d, ID: Identity> {
    type Bound;

    fn bind(self, owner: attempt::Id<'d, ID>) -> Self::Bound;
}

impl<'d, ID: Identity> OwnedDrain<'d, ID> {
    pub(super) const fn new(owner: attempt::Id<'d, ID>, drain: Drain<'d, ID>) -> Self {
        Self { owner, drain }
    }

    pub(in crate::client) fn run<R: BindOwner<'d, ID>>(
        self,
        retire: impl FnOnce(Drain<'d, ID>) -> R,
    ) -> R::Bound {
        retire(self.drain).bind(self.owner)
    }
}

#[must_use = "claim restoration must preserve an exact frozen decision"]
pub(super) enum Restore {
    Current(Resume),
    Stale,
}

pub(super) enum Resume {
    Restored,
    Pending,
}

pub(in crate::client) enum Recovery<'d, ID: Identity> {
    Restored,
    Rescheduled,
    Obsolete(Obsolete<'d, ID>),
}

#[must_use = "obsolete drain evidence must reach owner reconciliation"]
pub(in crate::client) struct Obsolete<'d, ID: Identity>(Drain<'d, ID>);

impl<ID: Identity> Obsolete<'_, ID> {
    pub(in crate::client) fn resolve(self) {
        let Self(Drain { conn: _, rule: _ }) = self;
    }
}

#[must_use = "absent drain evidence must reach owner reconciliation"]
pub(in crate::client) struct Absent<'d, ID: Identity>(Drain<'d, ID>);

impl<ID: Identity> Absent<'_, ID> {
    pub(in crate::client) fn resolve(self) {
        let Self(Drain { conn: _, rule: _ }) = self;
    }
}

pub(in crate::client) enum Access {
    Current(claim::Transport),
    Stale,
}

#[must_use = "drain readiness owns one finalized connection generation"]
pub(in crate::client) struct Drain<'d, ID: Identity> {
    conn: conn::Id<'d, ID>,
    rule: Directive,
}

impl<'d, ID: Identity> Drain<'d, ID> {
    pub(in crate::client) const fn connection(&self) -> conn::Id<'d, ID> {
        self.conn
    }

    pub(super) const fn rule(&self) -> Directive {
        self.rule
    }

    pub(super) const fn new(conn: conn::Id<'d, ID>, claim: closing::Claim) -> Self {
        Self {
            conn,
            rule: claim.into_rule(),
        }
    }

    pub(in crate::client) fn release<'w, T, W, C, O, M, B, const IOV: usize, D>(
        self,
        pool: &mut pool::Connections<'_, 'w, 'd, ID, T, W, State<'d, C, O>, M, B, IOV, D>,
    ) -> Recovery<'d, ID>
    where
        'd: 'w,
        T: Transport,
        W: wire::Spec,
        M: input::Mode<W>,
        D: demand::Mode,
    {
        let Some(slot) = pool.get_mut(self.key()) else {
            return Recovery::Obsolete(Obsolete(self));
        };
        match slot.state.closing.restore(self.rule) {
            Restore::Current(Resume::Restored) => Recovery::Restored,
            Restore::Current(Resume::Pending) => {
                slot.io().wake_target().wake();
                Recovery::Rescheduled
            }
            Restore::Stale => Recovery::Obsolete(Obsolete(self)),
        }
    }

    pub(in crate::client) const fn key(&self) -> pool::Key<'d, ID> {
        self.conn.key()
    }

    pub(in crate::client) fn obsolete(self) -> Obsolete<'d, ID> {
        Obsolete(self)
    }

    pub(in crate::client) fn absent(self) -> Absent<'d, ID> {
        Absent(self)
    }

    pub(in crate::client) fn authorize<'w, C, O, W>(
        &self,
        slot: &types::Connection<'w, 'd, ID, W, State<'d, C, O>>,
    ) -> Access
    where
        'd: 'w,
        W: wire::Spec,
    {
        if self.key() != slot.key() {
            return Access::Stale;
        }
        match slot.state.closing.authorize(self.rule) {
            Some(io) => Access::Current(io),
            None => Access::Stale,
        }
    }
}

impl<'d, ID: Identity, C> change::Finalize<Commit<'d, ID, C>> for Drain<'d, ID> {
    type Output = Retired<'d, ID, C>;

    fn finalize(self, proof: Commit<'d, ID, C>) -> Self::Output {
        let Self { conn: _, rule: _ } = self;
        let Commit { app, rule } = proof;
        Retired { app, rule }
    }
}
