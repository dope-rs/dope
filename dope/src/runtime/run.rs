use std::cell::Cell;
use std::io::{self, Error, ErrorKind};
use std::mem::take;
use std::pin::{Pin, pin};
use std::time::{Duration, Instant};

use dope_core::driver::completion::Completion;
use o3::cell::{BrandCell, BrandToken};

use crate::driver::token::Token;
use crate::runtime::__private::{Deadline, RootTask};
use crate::runtime::dispatcher::{Dispatcher, FinishContext, Idle};
use crate::{DriverContext, Event};

const DRAIN_BATCH: usize = 256;
const LOCAL_READY_ROUNDS_PER_TURN: usize = 2;

struct TurnState {
    cq_saturated: bool,
    shutdown_seen: bool,
}

pub(super) struct Run<'run, 'driver, 'd, D> {
    driver: &'run mut DriverContext<'driver, 'd>,
    cell: Pin<&'run BrandCell<'d, D>>,
    token: &'run mut BrandToken<'d>,
    shutdown: Option<&'run Cell<bool>>,
    completions: [Option<Event<'d>>; DRAIN_BATCH],
}

impl<'run, 'driver, 'd, D> Run<'run, 'driver, 'd, D>
where
    D: Dispatcher<'d>,
{
    pub(super) fn new(
        driver: &'run mut DriverContext<'driver, 'd>,
        cell: Pin<&'run BrandCell<'d, D>>,
        token: &'run mut BrandToken<'d>,
        shutdown: Option<&'run Cell<bool>>,
    ) -> Self {
        Self {
            driver,
            cell,
            token,
            shutdown,
            completions: [const { None }; DRAIN_BATCH],
        }
    }

    pub(super) fn block_on<R, T>(mut self, root: R) -> io::Result<T>
    where
        R: RootTask<'d, T>,
    {
        let mut root = pin!(root);
        let root_target = root.as_ref().target();
        self.driver.refresh_turn_clock();
        root.as_mut().pre_park(&mut self.driver.reborrow());
        let mut poll_root = false;
        loop {
            let state = self.drive_batch(|target| {
                if target == root_target {
                    poll_root = true;
                    true
                } else {
                    false
                }
            });
            if take(&mut poll_root) {
                root.as_mut().pre_park(&mut self.driver.reborrow());
            }
            self.prepare_park();
            if state.shutdown_seen {
                self.finish_shutdown(D::SHUTDOWN_DRAIN)?;
                return Err(Error::new(
                    ErrorKind::Interrupted,
                    "runtime shut down while blocking on a root task",
                ));
            }
            if let Some(output) = root.as_mut().take_output() {
                self.driver.wait(Some(Duration::ZERO))?;
                return Ok(output);
            }
            let timeout = self.park_timeout(state.cq_saturated);
            self.driver.wait(timeout)?;
        }
    }

    pub(super) fn run(mut self) -> io::Result<()> {
        loop {
            let state = self.drive_batch(|_| false);
            self.prepare_park();
            if state.shutdown_seen {
                return self.finish_shutdown(D::SHUTDOWN_DRAIN);
            }
            let timeout = self.park_timeout(state.cq_saturated);
            self.driver.wait(timeout)?;
        }
    }

    fn drive_batch<F>(&mut self, mut consume_ready: F) -> TurnState
    where
        F: FnMut(Token) -> bool,
    {
        self.driver.refresh_turn_clock();
        let n = self.driver.drain(&mut self.completions);
        let mut shutdown_seen = false;
        for event in &mut self.completions[..n] {
            let Some(event) = event.take() else {
                continue;
            };
            if event.is_shutdown() {
                shutdown_seen = true;
                continue;
            }
            Dispatcher::dispatch(
                self.cell.borrow_pin_mut(self.token),
                event,
                &mut self.driver.reborrow(),
            );
        }
        for _ in 0..LOCAL_READY_ROUNDS_PER_TURN {
            let ready = self.driver.driver_ref();
            if !ready.has_ready() {
                break;
            }
            ready.drain_ready(|target| {
                if !consume_ready(target) {
                    Dispatcher::activate(
                        self.cell.borrow_pin_mut(self.token),
                        target,
                        &mut self.driver.reborrow(),
                    );
                }
            });
        }
        TurnState {
            cq_saturated: n == self.completions.len(),
            shutdown_seen,
        }
    }

    fn prepare_park(&mut self) {
        self.driver.refresh_turn_clock();
        Dispatcher::pre_park(
            self.cell.borrow_pin_mut(self.token),
            &mut self.driver.reborrow(),
        );
    }

    fn park_timeout(&self, cq_saturated: bool) -> Option<Duration> {
        if cq_saturated || self.driver.driver_ref().has_ready() {
            return Some(Duration::ZERO);
        }
        match Dispatcher::idle(self.cell.borrow_pin(self.token)) {
            Idle::Busy => Some(Duration::ZERO),
            Idle::Park(None) => None,
            Idle::Park(Some(deadline)) => {
                Some(deadline.saturating_duration_since(self.driver.turn_now()))
            }
        }
    }

    fn drain(&mut self, drain_window: Duration) -> io::Result<()> {
        let deadline = Deadline::after(Instant::now(), drain_window);
        loop {
            if Instant::now() >= deadline {
                return Ok(());
            }
            let state = self.drive_batch(|_| false);
            self.prepare_park();
            if !state.cq_saturated
                && !self.driver.driver_ref().has_ready()
                && matches!(
                    Dispatcher::idle(self.cell.borrow_pin(self.token)),
                    Idle::Park(None)
                )
            {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(self.driver.turn_now());
            let timeout = self
                .park_timeout(state.cq_saturated)
                .map_or(remaining, |timeout| timeout.min(remaining));
            self.driver.wait(Some(timeout))?;
        }
    }

    fn finish_shutdown(&mut self, drain_window: Duration) -> io::Result<()> {
        let should_shutdown = match self.shutdown {
            Some(state) => !state.replace(true),
            None => true,
        };
        if should_shutdown {
            Dispatcher::shutdown(
                self.cell.borrow_pin_mut(self.token),
                &mut self.driver.reborrow(),
            );
        }
        let drained = self.drain(drain_window);
        if should_shutdown {
            let mut context = FinishContext::new(self.driver.reborrow());
            Dispatcher::finish(self.cell.borrow_pin_mut(self.token), &mut context);
        }
        drained
    }
}
