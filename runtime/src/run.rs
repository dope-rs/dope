use std::{io, ops, pin, task, time};

use dope_core::driver::{
    self,
    ops::poll,
    route::{self, kind},
    schedule::{self, ready},
};
use o3::cell::brand;

use crate::executor;

type Progress<'d> = schedule::Progress<'d>;

const TURN_WORK_BUDGET: usize = schedule::MAX_TURN_WORK_BUDGET;
const LOCAL_READY_ROUNDS_PER_TURN: usize = 2;
type RootTag = route::KeyTag<{ route::FRAMEWORK }, { kind::ONE_SHOT }>;

struct TurnState {
    shutdown_seen: bool,
    events: poll::Drain,
}

enum BlockExit<T> {
    Complete(T),
    Shutdown,
    DriverError(io::Error),
}

enum OutputStep {
    Complete,
    Shutdown,
    Continue,
}

pub(super) struct Run<'run, 'driver, 'app, 'd: 'app, D> {
    driver: &'run mut driver::Context<'driver, 'd>,
    installed: crate::Installed<'app, 'd, D>,
    token: &'run mut brand::Token<'d>,
    turn: schedule::Controller<'driver, 'd>,
    completions: &'run mut crate::Events<'d>,
}

struct ActiveCycle<'cycle, 'driver, 'app, 'd: 'app, D> {
    driver: &'cycle mut driver::Context<'driver, 'd>,
    installed: &'cycle mut crate::Installed<'app, 'd, D>,
    token: &'cycle mut brand::Token<'d>,
    turn: schedule::ActiveTurn<'cycle, 'd>,
    completions: &'cycle mut crate::Events<'d>,
}

/// Retains a pinned root and its wake slot for the complete poll/commit frame.
struct PinnedRootGuard<'guard, 'run, 'driver, 'app, 'd: 'app, D, R>
where
    D: executor::Application<'d>,
    R: executor::Root<'d>,
{
    run: &'guard mut Run<'run, 'driver, 'app, 'd, D>,
    root: pin::Pin<&'guard mut R>,
    _slot: &'guard ready::Slot<'d, RootTag>,
}

impl<'guard, 'run, 'driver, 'app, 'd: 'app, D, R>
    PinnedRootGuard<'guard, 'run, 'driver, 'app, 'd, D, R>
where
    D: executor::Application<'d>,
    R: executor::Root<'d>,
{
    fn new(
        run: &'guard mut Run<'run, 'driver, 'app, 'd, D>,
        root: pin::Pin<&'guard mut R>,
        slot: &'guard ready::Slot<'d, RootTag>,
    ) -> Self {
        Self {
            run,
            root,
            _slot: slot,
        }
    }

    fn drive(
        self,
        root_target: route::Target<'d, RootTag>,
        wake: ready::Target<'d>,
    ) -> io::Result<R::Output> {
        let Self {
            run,
            mut root,
            _slot,
        } = self;
        let mut first_poll = true;
        let outcome = loop {
            let mut cycle = run.begin_cycle();
            let (state, poll_root) = cycle.drive_batch(Some(root_target));
            if (first_poll || poll_root)
                && let task::Poll::Ready(output) = cycle.poll_root(root.as_mut(), wake)
            {
                let step = cycle.output_step(state);
                match step {
                    Ok(OutputStep::Complete) => break BlockExit::Complete(output),
                    Ok(OutputStep::Shutdown) => break BlockExit::Shutdown,
                    Ok(OutputStep::Continue) => {
                        break run.drain_output(root_target, output);
                    }
                    Err(error) => break BlockExit::DriverError(error),
                }
            }
            first_poll = false;
            cycle.prepare_park();
            if state.shutdown_seen {
                break BlockExit::Shutdown;
            }
            match cycle.redrive_events(state.events) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => break BlockExit::DriverError(error),
            }
            let timeout = cycle.park_timeout();
            if let Err(error) = poll::Poll::wait(&mut *cycle.driver, cycle.turn.reactor(), timeout)
            {
                break BlockExit::DriverError(error);
            }
        };
        match outcome {
            BlockExit::Complete(output) => Ok(output),
            BlockExit::Shutdown => Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "runtime shut down while blocking on a root task",
            )),
            BlockExit::DriverError(error) => Err(error),
        }
    }
}

impl<'run, 'driver, 'app, 'd: 'app, D> Run<'run, 'driver, 'app, 'd, D>
where
    D: executor::Application<'d>,
{
    pub(super) fn new(
        driver: &'run mut driver::Context<'driver, 'd>,
        installed: crate::Installed<'app, 'd, D>,
        token: &'run mut brand::Token<'d>,
        turn: schedule::Controller<'driver, 'd>,
        completions: &'run mut crate::Events<'d>,
    ) -> Self {
        Self {
            driver,
            installed,
            token,
            turn,
            completions,
        }
    }

    fn begin_cycle(&mut self) -> ActiveCycle<'_, 'driver, 'app, 'd, D> {
        ActiveCycle {
            driver: &mut *self.driver,
            installed: &mut self.installed,
            token: &mut *self.token,
            turn: self.turn.begin(TURN_WORK_BUDGET),
            completions: &mut *self.completions,
        }
    }

    pub(super) fn block_on<R>(mut self, root: R) -> io::Result<R::Output>
    where
        R: executor::Root<'d>,
    {
        let reference = self.driver.driver_ref();
        let root_target = route::Space::<RootTag>::for_driver(reference)
            .bind(route::SlotIndex::ZERO, route::Epoch::INITIAL);
        let slot = reference.ready().make_ready_slot(root_target.dispatch())?;
        let wake = slot.target();
        let mut root = pin::pin!(root);
        PinnedRootGuard::new(&mut self, root.as_mut(), &slot).drive(root_target, wake)
    }

    pub(super) fn run(mut self) -> io::Result<()> {
        self.run_until_shutdown()
    }

    /// Drives the dispatcher until it observes a shutdown event or the driver
    /// reports an error.  The caller must finish the dispatcher while its
    /// wire-owned buffers and slots are still alive.
    fn run_until_shutdown(&mut self) -> io::Result<()> {
        loop {
            let mut cycle = self.begin_cycle();
            let (state, _) = cycle.drive_batch(None);
            cycle.prepare_park();
            if state.shutdown_seen {
                return Ok(());
            }
            if cycle.redrive_events(state.events)? {
                continue;
            }
            let timeout = cycle.park_timeout();
            poll::Poll::wait(&mut *cycle.driver, cycle.turn.reactor(), timeout)?;
        }
    }

    fn drain_output<T>(
        &mut self,
        root_target: route::Target<'d, RootTag>,
        output: T,
    ) -> BlockExit<T> {
        loop {
            let mut cycle = self.begin_cycle();
            let (state, _) = cycle.drive_batch(Some(root_target));
            match cycle.output_step(state) {
                Ok(OutputStep::Complete) => return BlockExit::Complete(output),
                Ok(OutputStep::Shutdown) => return BlockExit::Shutdown,
                Ok(OutputStep::Continue) => {}
                Err(error) => return BlockExit::DriverError(error),
            }
        }
    }

    fn drain(
        &mut self,
        deadline: time::Instant,
        _pending: &executor::raw::Pending<'app, 'd, D>,
    ) -> io::Result<()> {
        loop {
            if time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "runtime retained-owner shutdown did not quiesce within its deadline",
                ));
            }
            let mut cycle = self.begin_cycle();
            let (state, _) = cycle.drive_batch(None);
            cycle.prepare_park();
            if cycle.redrive_events(state.events)? {
                continue;
            }
            if !cycle.turn.exhausted()
                && !cycle.driver.driver_ref().ready().has_ready()
                && matches!(cycle.shutdown_progress(), Progress::Quiescent)
            {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(cycle.driver.turn_now());
            let timeout = match cycle.park_timeout_with(|cycle| cycle.shutdown_progress()) {
                Some(timeout) => timeout.min(remaining),
                None => remaining,
            };
            poll::Poll::wait(&mut *cycle.driver, cycle.turn.reactor(), Some(timeout))?;
        }
    }

    pub(super) fn shutdown(&mut self) -> executor::raw::Pending<'app, 'd, D> {
        let cycle = self.begin_cycle();
        cycle
            .installed
            .shutdown(cycle.token, cycle.turn.turn(), cycle.driver.reborrow())
    }

    pub(super) fn drain_for(
        &mut self,
        drain_window: time::Duration,
        pending: &executor::raw::Pending<'app, 'd, D>,
    ) -> io::Result<()> {
        let Some(deadline) = time::Instant::now().checked_add(drain_window) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime shutdown drain exceeds the monotonic clock range",
            ));
        };
        self.drain(deadline, pending)
    }
}

impl<'cycle, 'driver, 'app, 'd: 'app, D> ActiveCycle<'cycle, 'driver, 'app, 'd, D>
where
    D: executor::Application<'d>,
{
    fn drive_batch(
        &mut self,
        root_target: Option<route::Target<'d, RootTag>>,
    ) -> (TurnState, bool) {
        self.turn.refresh_clock();
        let (reactor, turn) = self.turn.reactor_with_turn();
        let mut shutdown_seen = false;
        let blocked = self.completions.take().and_then(|event| {
            if event.is_shutdown() {
                shutdown_seen = true;
                return None;
            }
            match self.installed.dispatch(
                self.token,
                event,
                turn.reborrow(),
                &mut self.driver.reborrow(),
            ) {
                ops::ControlFlow::Continue(()) => None,
                ops::ControlFlow::Break(event) => Some(event),
            }
        });
        let events = match blocked {
            Some(event) => {
                *self.completions = Some(event);
                poll::Drain::Pending
            }
            None => {
                let dispatched =
                    poll::Source::dispatch(&mut *self.driver, reactor, |event, driver| {
                        if event.is_shutdown() {
                            shutdown_seen = true;
                            return ops::ControlFlow::Continue(());
                        }
                        self.installed
                            .dispatch(self.token, event, turn.reborrow(), driver)
                    });
                let (drain, retained) = dispatched.into_parts();
                *self.completions = retained;
                drain
            }
        };
        let mut root_ready = false;
        for _ in 0..LOCAL_READY_ROUNDS_PER_TURN {
            let ready = self.driver.driver_ref();
            if !ready.ready().has_ready() {
                break;
            }
            let drained = turn.reborrow().drain_ready(ready, usize::MAX, |target| {
                if root_target.is_some_and(|root| root.dispatch().matches(target)) {
                    root_ready = true;
                } else {
                    self.installed.activate(
                        self.token,
                        target,
                        turn.reborrow(),
                        &mut self.driver.reborrow(),
                    );
                }
            });
            if drained == 0 {
                break;
            }
        }
        (
            TurnState {
                shutdown_seen,
                events,
            },
            root_ready,
        )
    }

    fn poll_root<R>(
        &mut self,
        root: pin::Pin<&mut R>,
        wake: ready::Target<'d>,
    ) -> task::Poll<R::Output>
    where
        R: executor::Root<'d>,
    {
        let work = self.turn.turn().application();
        let driver = self.installed.retained_context(self.driver.reborrow());
        R::poll(executor::RootContext::new(root, wake, work, driver))
    }

    fn prepare_park(&mut self) {
        let turn = self.turn.turn();
        let now = self.turn.refresh_clock();
        let timer = self.driver.timer();
        timer.expire(turn.reborrow().timers(), self.driver, now);
        self.installed
            .pre_park(self.token, turn, &mut self.driver.reborrow());
    }

    fn redrive_events(&mut self, events: poll::Drain) -> io::Result<bool> {
        if self.completions.is_some() {
            return Ok(true);
        }
        if matches!(events, poll::Drain::Pending) {
            return match poll::Poll::commit(&mut *self.driver, self.turn.reactor())? {
                poll::Commit::Drained | poll::Commit::Pending => Ok(true),
            };
        }
        Ok(false)
    }

    fn output_step(mut self, state: TurnState) -> io::Result<OutputStep> {
        self.prepare_park();
        if state.shutdown_seen {
            return Ok(OutputStep::Shutdown);
        }
        if self.redrive_events(state.events)? {
            return Ok(OutputStep::Continue);
        }
        match poll::Poll::commit(&mut *self.driver, self.turn.reactor())? {
            poll::Commit::Drained => Ok(OutputStep::Complete),
            poll::Commit::Pending => Ok(OutputStep::Continue),
        }
    }

    fn progress(&self) -> Progress<'d> {
        let region = self.driver.region_token_ref();
        let maintenance = self.driver.driver_ref().maintenance_progress();
        let timer = match self.driver.timer().earliest(region) {
            Some(deadline) => Progress::until(region, deadline),
            None => Progress::Quiescent,
        };
        timer
            .reduce(self.installed.progress(self.token, region))
            .reduce(maintenance)
    }

    fn shutdown_progress(&self) -> Progress<'d> {
        let region = self.driver.region_token_ref();
        let maintenance = self.driver.driver_ref().maintenance_progress();
        self.installed
            .shutdown_progress(self.token, region)
            .reduce(maintenance)
    }

    fn park_timeout(&self) -> Option<time::Duration> {
        self.park_timeout_with(Self::progress)
    }

    fn park_timeout_with(
        &self,
        progress: impl FnOnce(&Self) -> Progress<'d>,
    ) -> Option<time::Duration> {
        if self.turn.exhausted() || self.driver.driver_ref().ready().has_ready() {
            return Some(time::Duration::ZERO);
        }
        match progress(self) {
            Progress::Runnable => Some(time::Duration::ZERO),
            Progress::Waiting(wait) => wait
                .deadline()
                .map(|deadline| deadline.saturating_duration_since(self.driver.turn_now())),
            Progress::Quiescent => None,
        }
    }
}
