use dope_core::{
    driver::{
        result, retained,
        route::{
            Identity, indices,
            table::{self, conn},
        },
        schedule::clock,
    },
    io::socket::option,
};
use dope_net::link::{event, pool::change::opening};
use o3::{
    base::cell::region,
    buffer::hold,
    depot::{arena::slab::key, capacity},
};
use sync::State as _;

use crate::{
    app::receive,
    client::{attempt, close},
    service::{self, discover, health, sync},
};

mod control;
mod dials;
mod members;
mod peer;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum Member {}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct MemberKey<'h>(capacity::Key<'h, key::Handle<Member>>);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Target<'h> {
    member: MemberKey<'h>,
    health: health::Generation,
}

enum Impact {
    Transport,
    Connect,
    Idle,
    Protocol,
    Remote,
    Neutral,
}

impl From<close::Reason> for Impact {
    fn from(reason: close::Reason) -> Self {
        use close::{Capacity, LocalOrigin, Reason};

        use crate::client::deadline;
        match reason {
            Reason::Transport | Reason::Timeout(deadline::Kind::Send) => Self::Transport,
            Reason::Timeout(deadline::Kind::Connect) => Self::Connect,
            Reason::Timeout(deadline::Kind::Inbound) => Self::Idle,
            Reason::Protocol
            | Reason::Ingress(
                receive::Failure::Contract | receive::Failure::Hold(hold::Error::Range(_)),
            ) => Self::Protocol,
            Reason::Remote => Self::Remote,
            Reason::Local(
                LocalOrigin::ApiRequest
                | LocalOrigin::SideComplete
                | LocalOrigin::OperationCancelled
                | LocalOrigin::Superseded
                | LocalOrigin::ScheduledRotation
                | LocalOrigin::RuntimeShutdown
                | LocalOrigin::ResourceFailure,
            )
            | Reason::Capacity(
                Capacity::IngressStaging
                | Capacity::IngressRetention
                | Capacity::Transport
                | Capacity::IdentityExhausted,
            )
            | Reason::EndpointRetired
            | Reason::Timeout(deadline::Kind::Lifetime)
            | Reason::Receive(_)
            | Reason::Ingress(_)
            | Reason::Egress(_) => Self::Neutral,
        }
    }
}

pub struct Pool<
    'q,
    'h,
    'd,
    ID: Identity,
    T: dope_net::Transport,
    R: sync::Policy,
    I = <T as dope_net::Transport>::Addr,
    const N: usize = 16,
    F = discover::Notify,
> where
    I: Eq,
{
    membership: members::Set<'h, 'd, I, T::Addr, R>,
    dials: dials::Dials<'q, 'h, 'd, ID, T>,
    options: option::Set,
    backoff: health::Backoff,
    pub(crate) feedback: F,
    retirements: R::State,
}

impl<T, R, I, const N: usize, F, ID: Identity> attempt::Contract
    for Pool<'_, '_, '_, ID, T, R, I, N, F>
where
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
{
}

impl<'q, 'h, 'd, ID: Identity, T, R, I, const N: usize, F> Pool<'q, 'h, 'd, ID, T, R, I, N, F>
where
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
    F: discover::Access,
{
    pub(crate) fn try_with<O>(
        cap: conn::Capacity,
        retry: health::Backoff,
        opts: option::Set,
        feedback: F,
        build: impl for<'a, 'b> FnOnce(Pool<'a, 'b, 'd, ID, T, R, I, N, F>) -> O,
    ) -> Result<O, service::Error> {
        let () = service::Limit::<N>::BOUNDED;
        const {
            assert!(
                service::MAX_ENDPOINTS as u64 + dope_core::driver::route::SLOT_MASK
                    <= u32::MAX as u64
            );
        }
        let preserve = !R::AUTHORITATIVE;
        let retained = if preserve { cap.entries().raw() } else { 0 };
        let limit = capacity::Count::new(N as u32 + retained);
        members::Set::try_with(limit, |members| {
            dials::Dials::try_with(cap, |dials| {
                build(Pool {
                    membership: members,
                    dials,
                    options: opts,
                    backoff: retry,
                    feedback,
                    retirements: Default::default(),
                })
            })
            .map_err(service::Error::Allocation)
        })?
    }

    pub(crate) fn poll<'ctx, 'owner, E>(
        &mut self,
        now: clock::Moment<'d>,
        driver: &mut retained::Context<'ctx, 'owner, 'd>,
        submit: impl FnOnce(
            attempt::Id<'d, ID>,
            attempt::Plan<T>,
            &mut retained::Context<'ctx, 'owner, 'd>,
        ) -> opening::Attempt<'d, ID, attempt::Plan<T>, (), E>,
    ) -> service::Opening<'d, E, ID>
    where
        F: discover::Eligibility<'d, I>,
    {
        if !self.feedback.is_enabled() {
            self.dials.refresh(false);
            return self.dials.terminal();
        }
        if F::WATCHED {
            self.dials.reset_front(|target| {
                self.membership
                    .cancel_if(target, |endpoint| {
                        !self.feedback.is_eligible(endpoint, driver.state_token())
                    })
                    .is_some()
            });
        }
        self.dials.poll::<_, _, _, R>(
            &mut self.membership,
            self.options,
            now,
            driver,
            |endpoint, driver| self.feedback.is_eligible(endpoint, driver.state_token()),
            submit,
        )
    }

    pub(crate) fn try_sync(
        &mut self,
        set: service::Snapshot<service::Endpoint<I, T::Addr>, N>,
        cap: table::Capacity,
    ) -> Result<service::Change, service::Reconcile> {
        let change = self.membership.try_apply(set)?;
        if R::AUTHORITATIVE {
            self.dials.reset_front(|target| {
                self.membership.cancel(target);
                true
            });
        }
        if change.retired != 0
            && let Some(scan) = self.retirements.scan()
        {
            scan.arm(cap);
        }
        self.refresh();
        Ok(change)
    }

    fn is_present(&self) -> bool {
        self.feedback.is_enabled()
            && (!self.membership.is_empty() || (!R::AUTHORITATIVE && self.dials.is_deferred()))
    }

    fn refresh(&mut self) {
        self.dials.refresh(self.is_present());
    }

    pub(crate) fn is_retiring(&self) -> bool {
        R::AUTHORITATIVE && self.retirements.is_pending()
    }

    pub(crate) fn next_retirement(&mut self) -> Option<indices::SlotIndex> {
        if !R::AUTHORITATIVE {
            return None;
        }
        self.retirements.next()
    }

    pub(crate) fn try_retain(&self, incoming: service::Revision) -> Result<(), service::Reconcile> {
        let current = self.membership.revision();
        if incoming != current {
            return Err(service::Reconcile::Stale { current, incoming });
        }
        Ok(())
    }

    pub(crate) fn is_retired(&self, key: attempt::Id<'d, ID>) -> bool {
        let Some(member) = self.dials.active(key) else {
            return false;
        };
        !self.feedback.is_enabled() || !self.membership.contains(member)
    }

    /// Retires one connection and returns the exact cause of member unavailability.
    /// `None` means a neutral cancellation or a health policy that left the member unchanged.
    fn retire(
        &mut self,
        key: attempt::Id<'d, ID>,
        reason: close::Reason,
        now: clock::Moment<'d>,
    ) -> Option<health::Cause> {
        let present = self.is_present();
        let mut cause = None;
        self.dials.recycle(key, present, |target| {
            let next = match Impact::from(reason) {
                Impact::Transport => health::Cause::Transport,
                Impact::Connect => health::Cause::ConnectTimeout,
                Impact::Idle => health::Cause::Idle,
                Impact::Protocol => health::Cause::Protocol,
                Impact::Remote => health::Cause::Remote,
                Impact::Neutral => {
                    self.membership.cancel(target);
                    return;
                }
            };
            if let members::Update::Applied = self.membership.fail(target, &mut self.backoff, now) {
                cause = Some(next);
            }
        });
        cause
    }

    /// Applies one failed connection attempt to its member, returning the exact
    /// health update the membership recorded; `None` reports a lane that had no
    /// submitted attempt left to update.
    fn failed(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: event::ConnectError,
        now: clock::Moment<'d>,
    ) -> Option<members::Update> {
        if let event::ConnectError::Admission(error) = cause
            && error.cause() == result::SubmitCause::IdentityExhausted
        {
            return self.exhaust(key, opening::Exhaustion::Submission(error));
        }
        let present = self.is_present();
        let mut update = None;
        self.dials.retry(key, present, |target| {
            update = Some(self.membership.fail(target, &mut self.backoff, now));
        });
        update
    }

    fn exhaust(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: opening::Exhaustion,
    ) -> Option<members::Update> {
        let target = self.dials.close(key)?;
        self.membership.cancel(target);
        self.reject(key, cause);
        Some(members::Update::Applied)
    }

    fn reject(&mut self, key: attempt::Id<'d, ID>, cause: opening::Exhaustion) {
        self.dials.reject(key, cause, |target| {
            self.membership.cancel(target);
        });
    }

    pub(crate) fn disconnect(
        &mut self,
        key: attempt::Id<'d, ID>,
        reason: close::Reason,
        now: clock::Moment<'d>,
    ) where
        F: discover::Feedback,
    {
        if let Some(cause) = self.retire(key, reason, now)
            && let Some(target) = self.feedback.cause_mut()
        {
            *target = Some(cause);
        }
    }

    pub(crate) fn fail(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: event::ConnectError,
        now: clock::Moment<'d>,
    ) where
        F: discover::Feedback,
    {
        if let Some(members::Update::Applied) = self.failed(key, cause, now)
            && let Some(target) = self.feedback.cause_mut()
        {
            *target = Some(health::Cause::Connect(cause));
        }
    }
}

impl<'q, 'h, 'd, ID: Identity, T, R, I, const N: usize, D>
    Pool<'q, 'h, 'd, ID, T, R, I, N, discover::Gate<D>>
where
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
    D: discover::Demand<'d, Endpoint = I>,
{
    pub(crate) fn is_stale(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        use discover::Access as _;

        let source = self.feedback.source();
        self.feedback.is_enabled() != source.is_needed(token)
            || self
                .membership
                .is_stale(|endpoint| source.is_eligible(endpoint, token))
    }

    pub(crate) fn set_demand(
        &mut self,
        token: &region::Token<region::Identity<'d, 1>>,
        cap: table::Capacity,
    ) {
        let enabled = self.feedback.source().is_needed(token);
        self.feedback.set(enabled);
        self.membership
            .set_demand(|endpoint| self.feedback.source().is_eligible(endpoint, token));
        if !enabled && let Some(scan) = self.retirements.scan() {
            scan.arm(cap);
        }
        self.refresh();
    }
}
