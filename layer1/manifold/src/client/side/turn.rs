use std::pin;

use dope_core::{
    driver::{retained, route::Identity, schedule},
    io::event::net::{connect, send, socket},
};
use dope_net::link::oneshot::{self, Busy, Dispatch, Unsent};
use o3::mem::quota;

use crate::{
    app::env,
    client::{
        app, attempt,
        conn::engine,
        side::{self, registry},
    },
    timing,
};
type Machine<'p, 'r, 'w, 'd, ID, A, S, E, I> =
    engine::Machine<'p, 'w, 'd, ID, A, S, E, side::Enabled<&'r registry::Registry<'d, I, ID>>>;

pub(in crate::client) struct Turn<'a, 'r, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, I>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
{
    machine: pin::Pin<&'a mut Machine<'p, 'r, 'w, 'd, ID, A, S, E, I>>,
}

impl<'p, 'a, 'r, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, I> Turn<'a, 'r, 'p, 'w, 'd, ID, A, S, E, I>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
{
    pub(in crate::client) fn new(
        state: pin::Pin<&'a mut Machine<'p, 'r, 'w, 'd, ID, A, S, E, I>>,
    ) -> Self {
        Self { machine: state }
    }

    pub(in crate::client) fn poll(
        &mut self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let now = driver.driver_ref().clock().now();
        let target = self.machine.as_ref().backoff();
        let retry = self
            .machine
            .as_mut()
            .project()
            .links
            .liveness_mut()
            .retry_ready(now, target)
            .is_ready();
        if retry {
            self.machine.as_ref().project_ref().side.registry.retry();
        }
        loop {
            let admit = self
                .machine
                .as_ref()
                .project_ref()
                .side
                .registry
                .take_request(turn.reborrow().app());
            match admit {
                quota::Admission::Item(req) => {
                    if self.try_submit(req, driver).is_err() {
                        self.machine
                            .as_mut()
                            .project()
                            .links
                            .liveness_mut()
                            .defer_retry(now, target);
                    }
                }
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            }
        }
        if retry {
            loop {
                let admit = turn.reborrow().maint().run(|| {
                    if !self
                        .machine
                        .as_ref()
                        .project_ref()
                        .links
                        .liveness()
                        .has_retries()
                    {
                        return None;
                    }
                    Some(self.edit_io(driver, |io| io.retry_one()))
                });
                match admit {
                    quota::Admission::Item(Ok(done)) => self.settle(done),
                    quota::Admission::Item(Err(_)) => {
                        self.machine
                            .as_mut()
                            .project()
                            .links
                            .liveness_mut()
                            .defer_retry(now, target);
                        break;
                    }
                    quota::Admission::Empty | quota::Admission::Exhausted => break,
                }
            }
        }
        let this = self.machine.as_mut().project();
        this.links
            .liveness_mut()
            .finish_retry(this.side.registry.pending().deferred, target);
    }

    pub(in crate::client) fn cancel(
        &mut self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        loop {
            let admit = self
                .machine
                .as_ref()
                .project_ref()
                .side
                .registry
                .take_cancel(turn.reborrow().maint());
            let stop = match admit {
                quota::Admission::Item(stop) => stop,
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            };
            let local = stop.target().key().lane();
            let target = self.machine.as_ref().backoff();
            let done = self
                .machine
                .as_mut()
                .project()
                .links
                .liveness_mut()
                .edit_io(target, |link| link.try_cancel(local, driver));
            let done = match done {
                Ok(done) => done,
                Err(cause) => {
                    self.machine
                        .as_ref()
                        .project_ref()
                        .side
                        .registry
                        .fail_cancel(stop, cause);
                    continue;
                }
            };
            let ticket = match done {
                oneshot::Progress::Complete {
                    authority: owner, ..
                } => Some(owner),
                oneshot::Progress::Idle | oneshot::Progress::Pending => None,
            };
            self.machine
                .as_mut()
                .project()
                .side
                .registry
                .end_cancel(stop, ticket);
        }
    }

    pub(in crate::client) fn socket(
        &mut self,
        done: socket::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let step = self.edit_io(driver, |io| io.socket(done));
        self.settle(step);
    }

    pub(in crate::client) fn connect(
        &mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), connect::Completion<'d>> {
        let step = self.edit_io(driver, |io| io.connect(done))?;
        self.settle(step);
        Ok(())
    }

    pub(in crate::client) fn send(
        &mut self,
        done: send::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), send::Completion<'d>> {
        let step = self.edit_io(driver, |io| io.send(done))?;
        self.settle(step);
        Ok(())
    }

    fn edit_io<R>(
        &mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
        edit: impl FnOnce(
            &mut oneshot::raw::Io<
                '_,
                '_,
                '_,
                'd,
                ID,
                side::Ticket<'d, ID>,
                <E::Transport as dope_net::Transport>::Prepared,
                I::Payload,
            >,
        ) -> R,
    ) -> R {
        let target = self.machine.as_ref().backoff();
        self.machine
            .as_mut()
            .project()
            .links
            .liveness_mut()
            .edit_io(target, |pool| {
                // SAFETY: Turn is entered from this installed Machine's raw
                // dispatch/drive callbacks. This is its original side pool;
                // Machine retains the pool and its stable entry owners until
                // terminal completion or finish with the driver's proof.
                // The callback receives no pool extraction or replacement API.
                let mut io = unsafe { oneshot::raw::Io::new(pool, driver) };
                edit(&mut io)
            })
    }

    fn try_submit(
        &mut self,
        req: side::Request<'d, I::Payload, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), Busy> {
        let target = req.target();
        let found = {
            let this = self.machine.as_ref().project_ref();
            this.links
                .pool()
                .target(target.key(), |slot| slot.state.options())
        };
        let Some((peer, opts)) = found else {
            self.reject(req, side::Error::Stale);
            return Ok(());
        };
        let local = target.key().lane();
        let now = driver.driver_ref().clock().now();
        let span = <E::Timing as timing::Policy>::CONNECT_DEADLINE;
        let until = match now.try_add(span) {
            Ok(until) => until,
            Err(range) => {
                self.reject(req, side::Error::ConnectDeadline(range));
                return Ok(());
            }
        };
        let window = <E::Timing as timing::Policy>::SEND_DEADLINE;
        let target = self.machine.as_ref().backoff();
        let sent = {
            let this = self.machine.as_mut().project();
            let (pool, liveness) = this.links.parts();
            let fds = pool.fds();
            liveness.edit_io(target, |link| {
                link.try_send(
                    fds,
                    local,
                    oneshot::Request::new(peer, opts, req.ticket, req.payload, until, window),
                    driver,
                )
            })
        };
        let (req, cause) = match sent {
            Dispatch::Sent => return Ok(()),
            Dispatch::Unsent(Unsent::Busy {
                request: req,
                cause,
            }) => {
                let oneshot::Parts {
                    authority: ticket,
                    payload,
                    ..
                } = req.into_parts();
                self.machine
                    .as_mut()
                    .project()
                    .side
                    .registry
                    .defer(side::Request { ticket, payload });
                return Err(cause);
            }
            Dispatch::Unsent(Unsent::Exhausted(req, cause)) => (req, side::Error::Identity(cause)),
            Dispatch::Unsent(Unsent::Rejected {
                request: req,
                cause,
            }) => (req, side::Error::Delivery(oneshot::Error::Submit(cause))),
            Dispatch::Unsent(Unsent::Outside {
                request: req,
                cause,
            }) => (req, side::Error::Route(cause)),
        };
        let parts = req.into_parts();
        self.machine
            .as_mut()
            .project()
            .side
            .registry
            .complete(parts.authority, Err(cause));
        Ok(())
    }

    fn settle(&mut self, step: oneshot::Progress<side::Ticket<'d, ID>>) {
        let oneshot::Progress::Complete {
            authority: owner,
            result,
        } = step
        else {
            return;
        };
        self.machine
            .as_mut()
            .project()
            .side
            .registry
            .complete(owner, result.map_err(side::Error::Delivery));
    }

    fn reject(&mut self, req: side::Request<'d, I::Payload, ID>, error: side::Error) {
        let owner = req.reject();
        self.machine
            .as_mut()
            .project()
            .side
            .registry
            .complete(owner, Err(error));
    }
}
