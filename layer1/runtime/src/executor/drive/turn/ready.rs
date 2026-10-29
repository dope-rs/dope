use std::{ops::ControlFlow, pin, task};

use dope_core::{
    driver,
    driver::{
        result::poll,
        schedule,
        schedule::{ready, ready::Target},
    },
    io::{Event, event::Kind},
};
use drive::turn;
use poll::fault;
use turn::{modes, routes, scope::Core};

use crate::executor::{self, boot, drive, root};

const READY_ROUNDS: usize = 2;

#[must_use]
pub(crate) struct Ready<'turn, 'd, M> {
    pub(super) core: Core<'turn, 'd, M>,
    pub(super) events: poll::Drain,
    pub(super) status: turn::Status,
}

pub(crate) struct RootReady<'turn, 'd, M> {
    ready: Ready<'turn, 'd, M>,
    root: ready::Readiness,
}

impl<'turn, 'app, 'd: 'app, D, S> Ready<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>
where
    D: executor::raw::App<'d>,
{
    pub(super) fn new(
        core: Core<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>,
    ) -> Result<Self, fault::Dispatch> {
        let mut ready = Self::observe(core)?;
        ready.activate(None);
        Ok(ready)
    }

    fn observe(
        mut core: Core<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>,
    ) -> Result<Self, fault::Dispatch> {
        let mut status = turn::Status::Active;
        let events = {
            let (lane, work) = core.turn.split();
            let held = match core.pending.take() {
                None => ControlFlow::Continue(()),
                Some(event) => Self::dispatch(
                    event,
                    &mut core.mode,
                    &mut core.driver,
                    work.reborrow(),
                    &mut status,
                ),
            };
            match held {
                ControlFlow::Break(event) => {
                    *core.pending = Some(event);
                    poll::Drain::Pending
                }
                ControlFlow::Continue(()) => {
                    let batch =
                        driver::Context::dispatch(&mut core.driver, lane, |event, driver| {
                            Self::dispatch(
                                event,
                                &mut core.mode,
                                driver,
                                work.reborrow(),
                                &mut status,
                            )
                        });
                    match batch {
                        poll::Dispatch::Ready { drain, retained } => {
                            *core.pending = retained;
                            drain
                        }
                        poll::Dispatch::Fault(fault) => return Err(fault),
                    }
                }
            }
        };

        Ok(Self {
            core,
            events,
            status,
        })
    }

    fn dispatch(
        event: Event<'d>,
        mode: &mut modes::App<'turn, 'app, 'd, D, S>,
        driver: &mut driver::Context<'_, 'd>,
        work: schedule::Turn<'_, 'd>,
        status: &mut turn::Status,
    ) -> ControlFlow<Event<'d>> {
        match event.into_kind() {
            Kind::Shutdown => {
                *status = turn::Status::Shutdown;
                ControlFlow::Continue(())
            }
            event => mode.view.dispatch(mode.token, event.into(), work, driver),
        }
    }

    fn activate(
        &mut self,
        mut root: Option<(&ready::Slot<'d, routes::Wake>, &mut ready::Readiness)>,
    ) {
        let work = self.core.turn.turn();
        let mut rounds = self.core.turn.ready();
        let mut left = READY_ROUNDS;
        while left != 0 {
            left -= 1;
            let step = rounds.drain(|target| {
                self.core.mode.view.activate(
                    self.core.mode.token,
                    target,
                    work.reborrow(),
                    &mut self.core.driver.reborrow(),
                );
            });
            if let Some((slot, observed)) = root.as_mut()
                && let ready::Readiness::Ready = slot.take()
            {
                **observed = ready::Readiness::Ready;
            }
            if step.is_none() || rounds.is_exhausted() {
                break;
            }
        }
    }
}

impl<'turn, 'd, M> Ready<'turn, 'd, M>
where
    M: modes::Mode<'d>,
{
    pub(crate) fn skip(self) -> turn::Polled<'turn, 'd, M> {
        turn::Polled::new(self.core, self.events, self.status)
    }

    pub(crate) fn sync(mut self) -> Result<poll::Commit, poll::Error> {
        let work = self.core.turn.reactor();
        driver::Context::commit(&mut self.core.driver, work)
    }

    pub(crate) fn is_quiet(&self) -> bool {
        self.core.pending.is_none()
            && matches!(self.events, poll::Drain::Drained)
            && self.core.is_quiet()
    }
}

impl<'app, 'd: 'app, D, S> Ready<'_, 'd, modes::App<'_, 'app, 'd, D, S>>
where
    D: executor::raw::App<'d>,
{
    pub(crate) fn begin_drain(mut self)
    where
        D: executor::raw::Drain<'d>,
    {
        self.core.mode.view.begin_drain(
            self.core.mode.token,
            self.core.turn.turn(),
            self.core.driver.reborrow(),
        );
    }

    pub(crate) fn force_drain(mut self)
    where
        D: executor::raw::Drain<'d>,
    {
        self.core.mode.view.force_drain(
            self.core.mode.token,
            self.core.turn.turn(),
            self.core.driver.reborrow(),
        );
    }

    pub(crate) fn shutdown(&self) -> schedule::Progress<'d> {
        self.core.shutdown()
    }
}

impl<'turn, 'app, 'd: 'app, D, S> RootReady<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>
where
    D: executor::raw::App<'d>,
{
    pub(super) fn new_app(
        core: Core<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>,
        root: &ready::Slot<'d, routes::Wake>,
    ) -> Result<Self, fault::Dispatch> {
        let mut ready = Ready::observe(core)?;
        let mut observed = root.take();
        ready.activate(Some((root, &mut observed)));
        Ok(Self {
            ready,
            root: observed,
        })
    }
}

impl<'turn, 'd> RootReady<'turn, 'd, modes::Boot> {
    pub(super) fn new_boot(
        mut core: Core<'turn, 'd, modes::Boot>,
        root: &ready::Slot<'d, routes::Wake>,
    ) -> Result<Self, fault::Dispatch> {
        let mut status = turn::Status::Active;
        let events = {
            let lane = core.turn.reactor();
            let batch = driver::Context::dispatch(&mut core.driver, lane, |event, _source| {
                match event.into_kind() {
                    Kind::Shutdown => {
                        status = turn::Status::Shutdown;
                        ControlFlow::Continue(())
                    }
                    event => ControlFlow::Break(event.into()),
                }
            });
            match batch {
                poll::Dispatch::Ready { drain, retained } => {
                    *core.pending = retained;
                    drain
                }
                poll::Dispatch::Fault(fault) => return Err(fault),
            }
        };
        let root = root.take();
        Ok(Self {
            ready: Ready {
                core,
                events,
                status,
            },
            root,
        })
    }

    pub(crate) fn poll_boot<R>(
        self,
        root: R,
        wake: Target<'d>,
        cause: boot::Cause,
    ) -> (
        turn::Polled<'turn, 'd, modes::Boot>,
        boot::Poll<R, R::Output>,
    )
    where
        R: boot::Root<'d>,
    {
        let context = boot::Context::new(
            wake,
            self.ready.core.turn.turn().app(),
            self.ready.core.driver.driver_ref().clock(),
            cause,
        );
        let output = R::poll(root, context);
        (self.skip(), output)
    }
}

impl<'turn, 'd, M> RootReady<'turn, 'd, M>
where
    M: modes::Mode<'d>,
{
    pub(crate) const fn is_root(&self) -> bool {
        matches!(self.root, ready::Readiness::Ready)
    }

    pub(crate) fn skip(self) -> turn::Polled<'turn, 'd, M> {
        self.ready.skip()
    }
}

impl<'turn, 'app, 'd: 'app, D, S> RootReady<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>
where
    D: executor::raw::App<'d>,
{
    pub(crate) fn poll<P>(
        mut self,
        root: pin::Pin<&mut P>,
        wake: Target<'d>,
    ) -> (
        turn::Polled<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>,
        task::Poll<P::Output>,
    )
    where
        P: root::Root<'d>,
    {
        let core = &mut self.ready.core;
        let state = core.mode.view.context(core.driver.reborrow());
        let output = P::poll(root::Context::new(
            root,
            wake,
            core.turn.turn().app(),
            state,
        ));
        (self.skip(), output)
    }
}
