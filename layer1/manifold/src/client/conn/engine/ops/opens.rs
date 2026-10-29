use std::pin;

use attempt::Report as _;
use dope_core::driver::{retained, route::Identity, schedule::clock};
use dope_net::{
    link::pool::{change::opening, pending},
    wire::{
        self,
        failure::{Class, Open},
        open,
    },
};

use crate::{
    app::env,
    client::{
        app::{self, defer},
        attempt::{self, queue},
        close::{self, Directive},
        conn::{
            self,
            engine::{self, liveness},
        },
        deadline, side,
        side::mode::Mode,
    },
    service::{self, discover, sync},
    timing,
};

pub(crate) enum Step<'d> {
    Continue,
    Stop,
    Retry,
    Backoff(clock::Deadline<'d>),
    Idle,
}

impl From<opening::Deferred> for Step<'_> {
    fn from(cause: opening::Deferred) -> Self {
        match cause {
            opening::Deferred::Capacity => Self::Stop,
            opening::Deferred::WireBackpressure | opening::Deferred::Submission(_) => Self::Retry,
        }
    }
}

fn report<'w, 'd: 'w, ID: Identity, A>(
    app: &mut A,
    key: attempt::Completion<'_, 'd, ID>,
    cause: opening::Failure<open::Error<'w, A::Wire>>,
    driver: &mut retained::Context<'_, '_, 'd>,
) -> Step<'d>
where
    A: app::Lifecycle<'w, 'd, ID> + app::Opened<'w, 'd, ID> + app::Exhausted<'d, ID>,
{
    match cause.into_wire() {
        Ok(cause) => app.open(key, cause, driver),
        Err(cause) => app.exhaust(key, cause, driver),
    }
    Step::Continue
}

trait Deferred<'d, ID: Identity, A: app::Application<'d, ID>> {
    fn report(
        app: &mut A,
        key: attempt::Id<'d, ID>,
        cause: opening::Deferred,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Step<'d>;
}

impl<'d, ID: Identity, A: app::Application<'d, ID, Deferred = Self>> Deferred<'d, ID, A>
    for defer::Controlled
{
    fn report(
        _: &mut A,
        _: attempt::Id<'d, ID>,
        cause: opening::Deferred,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Step<'d> {
        cause.into()
    }
}

impl<'d, ID: Identity, A> Deferred<'d, ID, A> for defer::Observed
where
    A: app::Application<'d, ID, Deferred = Self> + app::Deferred<'d, ID>,
{
    fn report(
        app: &mut A,
        key: attempt::Id<'d, ID>,
        cause: opening::Deferred,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Step<'d> {
        app.deferred(key, cause, driver);
        cause.into()
    }
}

pub(crate) trait Phase<'d, ID: Identity, A, S, E, D, F>
where
    A: app::Application<'d, ID, Deferred = D>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn poll(
        self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Step<'d>;
}

impl<'p, 'source, 'w, 'd: 'w, ID: Identity + 'd, A, E, X, D, F>
    Phase<'d, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, D, F>
    for engine::Machine<'p, 'w, 'd, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, X>
where
    A: app::Lifecycle<'w, 'd, ID, Deferred = D> + app::Opened<'w, 'd, ID> + app::Failed<'d, ID>,
    A::Wire: wire::Spec<OpenFailure = F>,
    F: Class<A::Wire, Open>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    D: Deferred<'d, ID, A>,
{
    fn poll(
        mut self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Step<'d> {
        let (key, plan) = match self.as_mut().project().controller.poll() {
            attempt::Action::Connect { key, plan } => (key, plan),
            attempt::Action::Idle => return Step::Idle,
        };
        let outcome = {
            let socket = plan.socket();
            let target = self.as_ref().backoff();
            let fields = self.as_mut().project();
            fields.links.open(
                socket,
                plan,
                |plan| {
                    let (target, opts) = plan.parts();
                    (conn::State::opening(side::Primary::new(key), opts), target)
                },
                |slot| {
                    fields.controller.bind(key, slot.key())?;
                    slot.arm_after(
                        deadline::Fixed::Connect,
                        now,
                        <E::Timing as timing::Policy>::CONNECT_DEADLINE,
                    );
                    Ok::<(), queue::Lookup<'d, ID>>(())
                },
                target,
                driver,
            )
        };
        match outcome {
            opening::Attempt::Submitted { key: conn, output } => {
                if let Err(cause) = output {
                    let this = self.as_mut().project();
                    if let Some((slot, handle)) = pending::Mut::of(this.links.pool_mut()).get(conn)
                    {
                        slot.state
                            .closing
                            .apply(Directive::reconnect(close::Reason::Transport));
                        handle.wake();
                    }
                    cause.report(
                        attempt::Completion::reported(key, this.controller),
                        this.app,
                        driver,
                    );
                } else {
                    self.as_mut().project().drive.as_mut().reset_retry();
                }
                Step::Continue
            }
            opening::Attempt::Deferred { cause, input } => {
                {
                    let this = self.as_mut().project();
                    if let Err((cause, _)) = this.controller.defer(key, input) {
                        cause.report(
                            attempt::Completion::reported(key, this.controller),
                            this.app,
                            driver,
                        );
                    }
                }
                D::report(self.as_mut().project().app, key, cause, driver)
            }
            opening::Attempt::Failed { error } => {
                {
                    let this = self.as_mut().project();
                    if let Err(cause) = this.controller.reject(key) {
                        cause.report(
                            attempt::Completion::reported(key, this.controller),
                            this.app,
                            driver,
                        );
                    }
                }
                let fields = self.as_mut().project();
                report(
                    fields.app,
                    attempt::Completion::reported(key, fields.controller),
                    error,
                    driver,
                )
            }
        }
    }
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, E, X, R, I, const N: usize, F, D, W>
    Phase<'d, ID, A, service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>, E, D, W>
    for engine::Machine<
        'p,
        'w,
        'd,
        ID,
        A,
        service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>,
        E,
        X,
    >
where
    A: app::Lifecycle<'w, 'd, ID, Deferred = D> + app::Opened<'w, 'd, ID> + app::Exhausted<'d, ID>,
    A::Wire: wire::Spec<OpenFailure = W>,
    W: Class<A::Wire, Open>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    R: sync::Policy,
    I: Eq,
    F: discover::Eligibility<'d, I>,
    D: Deferred<'d, ID, A>,
{
    fn poll(
        mut self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Step<'d> {
        let target = self.as_ref().backoff();
        let outcome = {
            let fields = self.as_mut().project();
            fields.controller.poll(now, driver, |key, plan, driver| {
                let socket = plan.socket();
                fields.links.open(
                    socket,
                    plan,
                    |plan| {
                        let (target, opts) = plan.parts();
                        (conn::State::opening(side::Primary::new(key), opts), target)
                    },
                    |slot| {
                        slot.arm_after(
                            deadline::Fixed::Connect,
                            now,
                            <E::Timing as timing::Policy>::CONNECT_DEADLINE,
                        );
                    },
                    target,
                    driver,
                )
            })
        };
        match outcome {
            service::Opening::Submitted => {
                self.as_mut().project().drive.as_mut().reset_retry();
                Step::Continue
            }
            service::Opening::Deferred { attempt, cause } => {
                D::report(self.as_mut().project().app, attempt, cause, driver)
            }
            service::Opening::Failed { attempt, error } => {
                let fields = self.as_mut().project();
                report(
                    fields.app,
                    attempt::Completion::reported(attempt, fields.controller),
                    error,
                    driver,
                )
            }
            service::Opening::Backoff(at) => Step::Backoff(at),
            service::Opening::Idle => Step::Idle,
        }
    }
}
