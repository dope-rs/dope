use crate::client::side::mode::Mode;
mod cancel;
pub(super) mod cycle;
pub(super) mod phase;

use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::wire;
use flush::Flush as _;

use crate::{
    app::{env, receive::ingress},
    client::{
        app,
        attempt::{self, queue},
        conn::engine::{
            self,
            liveness::{self, turn},
            ops::{close, dial, opens},
            transfer::{flush, send},
        },
        side::{self, registry},
    },
    service::{self, discover, sync},
};

/// Drives the deferred-dial submit protocol shared by every controller state.
///
/// One state arms a retry when the dial is deferred, then keeps the receive pool
/// flushed until it reports no rearm.
fn submit<'p, 'w, 'd: 'p + 'w, ID, A, S, E, X>(
    mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    work: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    ID: Identity + 'd,
    A: app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>:
        opens::Phase<'d, ID, A, S, E, A::Deferred, <A::Wire as wire::Spec>::OpenFailure>,
{
    if matches!(
        dial::Dial::new(machine.as_mut()).retry_ready(driver),
        dial::Retry::Pending
    ) {
        return;
    }
    match dial::Dial::new(machine.as_mut()).poll(work.reborrow(), driver) {
        dial::Source::Deferred => machine.as_mut().flush_recv(work.reborrow().maint(), driver),
        dial::Source::Idle => {
            machine.as_mut().flush_recv(work.reborrow().maint(), driver);
            let this = machine.as_mut().project();
            if this.links.pool().inspect().has_rearm() {
                dial::Dial::new(machine.as_mut()).defer_retry(driver.driver_ref().clock().now());
            } else {
                dial::Dial::new(machine.as_mut()).finish_retry();
            }
        }
    }
}

pub(in crate::client) trait Run<'d> {
    fn run(
        self: pin::Pin<&mut Self>,
        work: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'p, 'source, 'w, 'd: 'w, ID: Identity + 'd, A, E> Run<'d>
    for engine::Machine<'p, 'w, 'd, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>
        + flush::Flush<
            'p,
            'w,
            'd,
            ID,
            A,
            queue::Control<'source, 'd, E::Transport, ID>,
            E,
            side::Disabled,
        >,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    Self: opens::Phase<
            'd,
            ID,
            A,
            queue::Control<'source, 'd, E::Transport, ID>,
            E,
            A::Deferred,
            <A::Wire as wire::Spec>::OpenFailure,
        > + close::ClosePhase<'d, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E>,
{
    fn run(
        mut self: pin::Pin<&mut Self>,
        work: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        for _ in 0..phase::Local::COUNT {
            let step = self.as_mut().project().drive.as_mut().advance();
            match step {
                phase::Local::Dirty => {
                    A::Continuation::flush(self.as_mut(), work.reborrow(), driver)
                }
                phase::Local::Cancellations => {
                    self.as_mut().cancel(work.reborrow());
                }
                phase::Local::Submission => submit(self.as_mut(), work.reborrow(), driver),
                phase::Local::Credit => {
                    self.as_ref()
                        .get_ref()
                        .links
                        .pool()
                        .credit_step(driver.region_token(), work.reborrow().maint());
                }
                phase::Local::Liveness => {
                    turn::Turn::new(self.as_mut()).poll(work.reborrow(), driver);
                }
            }
            if work.reborrow().maint().remaining() == 0 {
                return;
            }
        }
    }
}

impl<'p, 'source, 'r, 'w, 'd: 'w, ID: Identity + 'd, A, E, I> Run<'d>
    for engine::Machine<
        'p,
        'w,
        'd,
        ID,
        A,
        queue::Control<'source, 'd, E::Transport, ID>,
        E,
        side::Enabled<&'r registry::Registry<'d, I, ID>>,
    >
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>
        + flush::Flush<
            'p,
            'w,
            'd,
            ID,
            A,
            queue::Control<'source, 'd, E::Transport, ID>,
            E,
            side::Enabled<&'r registry::Registry<'d, I, ID>>,
        >,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
    Self: opens::Phase<
            'd,
            ID,
            A,
            queue::Control<'source, 'd, E::Transport, ID>,
            E,
            A::Deferred,
            <A::Wire as wire::Spec>::OpenFailure,
        > + close::ClosePhase<'d, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E>,
{
    fn run(
        mut self: pin::Pin<&mut Self>,
        work: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        for _ in 0..phase::Local::COUNT {
            let step = self.as_mut().project().drive.as_mut().advance();
            match step {
                phase::Local::Dirty => {
                    A::Continuation::flush(self.as_mut(), work.reborrow(), driver)
                }
                phase::Local::Cancellations => {
                    use crate::client::side::turn;

                    turn::Turn::new(self.as_mut()).cancel(work.reborrow(), driver);
                    self.as_mut().cancel(work.reborrow());
                }
                phase::Local::Submission => {
                    use crate::client::side::turn;

                    turn::Turn::new(self.as_mut()).poll(work.reborrow(), driver);
                    submit(self.as_mut(), work.reborrow(), driver);
                }
                phase::Local::Credit => {
                    self.as_ref()
                        .get_ref()
                        .links
                        .pool()
                        .credit_step(driver.region_token(), work.reborrow().maint());
                }
                phase::Local::Liveness => {
                    turn::Turn::new(self.as_mut()).poll(work.reborrow(), driver);
                }
            }
            if work.reborrow().maint().remaining() == 0 {
                return;
            }
        }
    }
}

impl<'q, 'h, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, R, I, E, const N: usize, F> Run<'d>
    for engine::Machine<
        'p,
        'w,
        'd,
        ID,
        A,
        service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>,
        E,
    >
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>
        + flush::Flush<
            'p,
            'w,
            'd,
            ID,
            A,
            service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>,
            E,
            side::Disabled,
        >,
    R: sync::Policy,
    I: Eq,
    F: discover::Access,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    Self: opens::Phase<
            'd,
            ID,
            A,
            service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>,
            E,
            A::Deferred,
            <A::Wire as wire::Spec>::OpenFailure,
        > + close::ClosePhase<'d, ID, A, service::Pool<'q, 'h, 'd, ID, E::Transport, R, I, N, F>, E>,
{
    fn run(
        mut self: pin::Pin<&mut Self>,
        work: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        for _ in 0..phase::Service::COUNT {
            let step = self.as_mut().project().drive.as_mut().advance();
            match step {
                phase::Service::Dirty => {
                    A::Continuation::flush(self.as_mut(), work.reborrow(), driver)
                }
                phase::Service::Submission => submit(self.as_mut(), work.reborrow(), driver),
                phase::Service::Credit => {
                    self.as_ref()
                        .get_ref()
                        .links
                        .pool()
                        .credit_step(driver.region_token(), work.reborrow().maint());
                }
                phase::Service::Liveness => {
                    turn::Turn::new(self.as_mut()).poll(work.reborrow(), driver);
                }
            }
            if work.reborrow().maint().remaining() == 0 {
                return;
            }
        }
    }
}

impl<'q, 'h, 'p, 'r, 'w, 'd: 'w, ID: Identity + 'd, A, R, K, E, const N: usize, F, I> Run<'d>
    for engine::Machine<
        'p,
        'w,
        'd,
        ID,
        A,
        service::Pool<'q, 'h, 'd, ID, E::Transport, R, K, N, F>,
        E,
        side::Enabled<&'r registry::Registry<'d, I, ID>>,
    >
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>
        + flush::Flush<
            'p,
            'w,
            'd,
            ID,
            A,
            service::Pool<'q, 'h, 'd, ID, E::Transport, R, K, N, F>,
            E,
            side::Enabled<&'r registry::Registry<'d, I, ID>>,
        >,
    R: sync::Policy,
    K: Eq,
    F: discover::Access,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
    Self: opens::Phase<
            'd,
            ID,
            A,
            service::Pool<'q, 'h, 'd, ID, E::Transport, R, K, N, F>,
            E,
            A::Deferred,
            <A::Wire as wire::Spec>::OpenFailure,
        > + close::ClosePhase<'d, ID, A, service::Pool<'q, 'h, 'd, ID, E::Transport, R, K, N, F>, E>,
{
    fn run(
        mut self: pin::Pin<&mut Self>,
        work: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        for _ in 0..phase::Local::COUNT {
            let step = self.as_mut().project().drive.as_mut().advance();
            match step {
                phase::Local::Dirty => {
                    A::Continuation::flush(self.as_mut(), work.reborrow(), driver)
                }
                phase::Local::Cancellations => {
                    use crate::client::side::turn;

                    turn::Turn::new(self.as_mut()).cancel(work.reborrow(), driver);
                }
                phase::Local::Submission => {
                    use crate::client::side::turn;

                    turn::Turn::new(self.as_mut()).poll(work.reborrow(), driver);
                    submit(self.as_mut(), work.reborrow(), driver);
                }
                phase::Local::Credit => {
                    self.as_ref()
                        .get_ref()
                        .links
                        .pool()
                        .credit_step(driver.region_token(), work.reborrow().maint());
                }
                phase::Local::Liveness => {
                    turn::Turn::new(self.as_mut()).poll(work.reborrow(), driver);
                }
            }
            if work.reborrow().maint().remaining() == 0 {
                return;
            }
        }
    }
}

mod policy;
