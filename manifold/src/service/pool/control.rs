use std::time;

use dope_core::io::socket::option;
use dope_net::link::pool;

use crate::{
    connector::{attempt, lifecycle},
    service::{self, health, reconcile},
};

impl<'d, T, R, I, const N: usize, const ID: u8> attempt::Control<'d, T, ID>
    for service::Pool<T, R, I, N>
where
    T: dope_net::Transport,
    R: reconcile::Policy,
    I: Eq,
{
    fn resize(&mut self, max_connections: usize) -> Result<(), attempt::ResizeError> {
        let available = self.dials.capacity();
        if available < max_connections {
            return Err(attempt::ResizeError::new(max_connections, available));
        }
        Ok(())
    }

    fn poll_connect(&mut self, now: time::Instant) -> attempt::Action<'d, T, ID> {
        if <R as reconcile::Supported>::AUTHORITATIVE {
            let membership = &self.membership;
            self.dials
                .reset_front(|member| !membership.contains(member));
        }
        let membership = &mut self.membership;
        let options = self.options;
        let started = self.dials.start::<_, ID>(|| {
            let choice = membership.choose(now)?;
            Ok((choice.member, attempt::Plan::<T>::new(choice.addr, options)))
        });
        match started {
            Ok(Some(started)) => {
                self.dials.refresh_pending(!self.membership.is_empty());
                attempt::Action::Connect {
                    key: started.key,
                    plan: started.output,
                }
            }
            Ok(None) | Err(None) => {
                self.dials.needs_poll = false;
                attempt::Action::Idle
            }
            Err(Some(min_retry_at)) => {
                self.dials.needs_poll = false;
                attempt::Action::Backoff { min_retry_at }
            }
        }
    }

    fn has_pending(&self) -> bool {
        self.dials.needs_poll
    }

    fn connect_succeeded(
        &mut self,
        key: attempt::Id<'d, ID>,
        _now: time::Instant,
    ) -> attempt::Transition {
        let Some(member) = self.dials.connected(key) else {
            return attempt::Transition::Stale;
        };
        self.membership.recover(member);
        attempt::Transition::Applied
    }

    fn connect_failed(&mut self, key: attempt::Id<'d, ID>, now: time::Instant) {
        let Some(member) = self.dials.retry(key, !self.membership.is_empty()) else {
            return;
        };
        self.record_failure(member, health::Cause::Connect, now);
    }

    fn connect_options(&mut self, key: attempt::Id<'d, ID>) -> Option<option::StreamOptions> {
        self.dials.is_submitted(key).then_some(self.options)
    }

    fn connect_deferred(
        &mut self,
        key: attempt::Id<'d, ID>,
        plan: attempt::Plan<T>,
        _now: time::Instant,
    ) {
        self.dials.defer(key, plan, !self.membership.is_empty());
    }

    fn disconnect(
        &mut self,
        key: attempt::Id<'d, ID>,
        reason: lifecycle::CloseReason,
        now: time::Instant,
    ) {
        use lifecycle::CloseReason;
        let cause = match reason {
            CloseReason::Local | CloseReason::Capacity | CloseReason::EndpointRetired => None,
            CloseReason::Transport => Some(health::Cause::Transport),
            CloseReason::Timeout(lifecycle::TimeoutKind::Connect) => Some(health::Cause::Connect),
            CloseReason::Timeout(lifecycle::TimeoutKind::Inbound) => Some(health::Cause::Idle),
            CloseReason::Timeout(lifecycle::TimeoutKind::Send) => Some(health::Cause::Transport),
            CloseReason::Timeout(
                lifecycle::TimeoutKind::Lifetime | lifecycle::TimeoutKind::Auxiliary,
            ) => None,
            CloseReason::Protocol => Some(health::Cause::Protocol),
            CloseReason::Remote => Some(health::Cause::Remote),
        };
        let Some(member) = self.dials.recycle(key, !self.membership.is_empty()) else {
            return;
        };
        if let Some(cause) = cause {
            self.record_failure(member, cause, now);
        }
    }

    fn kill(&mut self, key: attempt::Id<'d, ID>) {
        self.dials.kill(key);
    }

    fn bind(
        &mut self,
        key: attempt::Id<'d, ID>,
        _local: pool::Key<'d, ID>,
        options: Option<option::StreamOptions>,
    ) {
        if options.is_some() {
            self.dials.submitted(key);
        } else {
            self.dials.kill(key);
        }
    }
}
