use attempt::Report as _;
use dope_core::driver::{self, result, route::Identity, schedule};
use dope_net::link::{event, pool::change::opening};

use crate::{
    app::env,
    client::{
        app,
        attempt::{self, mode, queue},
        close,
        conn::{
            drain,
            engine::{
                self, liveness,
                ops::{fail, retire},
            },
        },
        deadline,
        side::mode::Mode,
    },
    service::{self, discover, sync},
};

fn run<'a, 'p, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X>(
    context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
    entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
    close: drain::OwnedDrain<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
    retry: impl FnOnce(&mut S, attempt::Id<'d, ID>, close::Reason) -> Result<(), S::Error>,
) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    S::Error: attempt::Report<'d, ID, A>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: retire::RetirePhase<'p, 'w, 'd, ID, A, S, E, X>,
{
    let (owner, drain) = close.into_parts();
    let (entry, outcome) = <engine::Machine<'p, 'w, 'd, ID, A, S, E, X> as retire::RetirePhase<'p, 'w, 'd, ID, A, S, E, X>>::drain_close(
        context, entry,
        drain,
        turn,
        driver,
        std::convert::identity,
    );
    let result = match outcome {
        retire::Outcome::Retired(rule) => match rule.policy() {
            close::Policy::Permanent | close::Policy::ImmediatePermanent => {
                let cause = match rule.reason() {
                    close::Reason::Capacity(close::Capacity::IdentityExhausted) => {
                        Some(opening::Exhaustion::Identity)
                    }
                    close::Reason::Receive(event::ParkError::Submit(error))
                        if error.cause() == result::SubmitCause::IdentityExhausted =>
                    {
                        Some(opening::Exhaustion::Submission(error))
                    }
                    _ => None,
                };
                context.controller.kill(owner, cause)
            }
            close::Policy::Reconnect | close::Policy::ImmediateReconnect => {
                retry(context.controller, owner, rule.reason())
            }
        },
        retire::Outcome::Pending => return entry,
        retire::Outcome::Obsolete(proof) => {
            proof.resolve();
            context.controller.kill(owner, None)
        }
    };
    if let Err(cause) = result {
        cause.retire(
            attempt::Completion::reported(owner, context.controller),
            context.app,
            driver,
        );
    }
    if context.shutdown.is_open() && context.controller.is_pending() {
        context.target.wake();
    }
    entry
}

pub(super) trait Finalize<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    fn begin(
        _: &mut S,
        _: &mut A,
        _: attempt::Id<'d, ID>,
        _: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) {
    }

    fn finish<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        close: drain::OwnedDrain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>>;
}

impl<'p, 'source, 'w, 'd: 'w, ID: Identity + 'd, A, E, X>
    Finalize<'p, 'w, 'd, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, X> for mode::Local
where
    A: app::Lifecycle<'w, 'd, ID> + app::Failed<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn begin(
        ctrl: &mut queue::Control<'source, 'd, E::Transport, ID>,
        app: &mut A,
        owner: attempt::Id<'d, ID>,
        reason: close::Reason,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let cause = match reason {
            close::Reason::Timeout(deadline::Kind::Connect) => {
                attempt::ConnectFault::ConnectTimeout
            }
            close::Reason::Local(close::LocalOrigin::RuntimeShutdown) => {
                attempt::ConnectFault::RuntimeShutdown
            }
            _ => return,
        };
        match ctrl.close(owner) {
            Ok(Some(receipt)) => app.failed(receipt, cause, driver),
            Ok(None) => {}
            Err(error) => error.retire(attempt::Completion::reported(owner, ctrl), app, driver),
        }
    }

    fn finish<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, X>,
        entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        close: drain::OwnedDrain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>> {
        run(context, entry, close, turn, driver, |ctrl, owner, _| ctrl.disconnect(owner))
    }
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, E, X, R, I, const N: usize, F>
    Finalize<'p, 'w, 'd, ID, A, service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>, E, X>
    for mode::Service
where
    A: app::Lifecycle<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    R: sync::Policy,
    I: Eq,
    F: discover::Feedback,
    A::Failure: fail::Fail<'d, ID, A, service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>, E::Transport>,
{
    fn finish<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>, E, X>,
        entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        close: drain::OwnedDrain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>> {
        let now = driver.driver_ref().clock().now();
        run(context, entry, close, turn, driver, |pool, owner, reason| {
            pool.disconnect(owner, reason, now);
            Ok(())
        })
    }
}
