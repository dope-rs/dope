use std::pin;

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

fn run<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>(
    mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    close: drain::OwnedDrain<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
    retry: impl FnOnce(&mut S, attempt::Id<'d, ID>, close::Reason) -> Result<(), S::Error>,
) where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    S::Error: attempt::Report<'d, ID, A>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: retire::RetirePhase<'w, 'd, ID, A, S, E>,
{
    let outcome = close.run(|drain| {
        retire::RetirePhase::drain_close(machine.as_mut(), drain, turn, driver, |rule| rule)
    });
    let this = machine.project();
    let (owner, result) = match outcome {
        retire::OwnedOutcome::Retired(retired) => retired.resolve(|owner, rule| {
            let result = match rule.policy() {
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
                    this.controller.kill(owner, cause)
                }
                close::Policy::Reconnect | close::Policy::ImmediateReconnect => {
                    retry(this.controller, owner, rule.reason())
                }
            };
            (owner, result)
        }),
        retire::OwnedOutcome::Pending => return,
        retire::OwnedOutcome::Missing(proof) => {
            proof.resolve(|owner| (owner, this.controller.kill(owner, None)))
        }
        retire::OwnedOutcome::Obsolete(proof) => {
            proof.resolve(|owner| (owner, this.controller.kill(owner, None)))
        }
    };
    if let Err(cause) = result {
        cause.retire(
            attempt::Completion::reported(owner, this.controller),
            this.app,
            driver,
        );
    }
    if this.shutdown.is_open() && this.controller.is_pending() {
        this.drive.target().wake();
    }
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

    fn finish(
        machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        close: drain::OwnedDrain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );
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

    fn finish(
        machine: pin::Pin<
            &mut engine::Machine<
                'p,
                'w,
                'd,
                ID,
                A,
                queue::Control<'source, 'd, E::Transport, ID>,
                E,
                X,
            >,
        >,
        close: drain::OwnedDrain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        run(machine, close, turn, driver, |ctrl, owner, _| {
            ctrl.disconnect(owner)
        });
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
    fn finish(
        machine: pin::Pin<
            &mut engine::Machine<
                'p,
                'w,
                'd,
                ID,
                A,
                service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>,
                E,
                X,
            >,
        >,
        close: drain::OwnedDrain<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let now = driver.driver_ref().clock().now();
        run(machine, close, turn, driver, |pool, owner, reason| {
            pool.disconnect(owner, reason, now);
            Ok(())
        });
    }
}
