use std::cell::Cell;
use std::io;
use std::mem::take;
use std::pin::{Pin, pin};
use std::time::{Duration, Instant};

use crate::driver::token::{SHUTDOWN, Token};
use crate::hash::Seed;
use crate::runtime::__private::{RootTask, saturating_deadline};
use crate::runtime::{Dispatcher, Idle};
use crate::{Cqe, Driver, DriverContext, DriverRef, Event, driver};
use dope_core::driver::completion::Completion;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ext::DriverExt;
use o3::cell::{BrandCell, BrandToken};

const DRAIN_BATCH: usize = 256;
// A receive can wake a connection task, which can in turn wake the listener
// after staging a send. Draining only one ready snapshot per driver turn puts a
// zero-time io_uring_enter between every step of that local chain. Follow a
// bounded number of ready generations in-process so ordinary request paths
// reach their submission without an otherwise empty kernel round trip. The
// bound keeps a pathological self-waking task from starving completions.
const READY_DRAIN_ROUNDS: usize = 2;

struct BatchState {
    cq_saturated: bool,
    shutdown_seen: bool,
}

pub trait StorageFactory: 'static {
    type Output<'d>: 'd;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d>;
}

pub struct ValueStorage<T>(T);

impl StorageFactory for () {
    type Output<'d> = ();

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {}
}

impl<T: 'static> StorageFactory for ValueStorage<T> {
    type Output<'d> = T;

    fn build<'d>(self, _driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        self.0
    }
}

impl<A: StorageFactory, B: StorageFactory> StorageFactory for (A, B) {
    type Output<'d> = (A::Output<'d>, B::Output<'d>);

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        let first = self.0.build(&mut driver.reborrow());
        let second = self.1.build(driver);
        (first, second)
    }
}

pub struct Executor<S = ()> {
    storage: S,
    driver: Driver,
    seed: Seed,
}

impl Executor<()> {
    pub fn new(cfg: driver::Config) -> io::Result<Self> {
        Self::with_seed(cfg, Seed::random()?)
    }

    pub fn with_seed(cfg: driver::Config, seed: Seed) -> io::Result<Self> {
        let driver = Driver::new(cfg)?;
        Ok(Self {
            storage: (),
            driver,
            seed,
        })
    }
}

impl<S> Executor<S> {
    pub fn with_storage<T: 'static>(self, storage: T) -> Executor<ValueStorage<T>> {
        Executor {
            storage: ValueStorage(storage),
            driver: self.driver,
            seed: self.seed,
        }
    }

    pub fn with_storage_factory<T: StorageFactory>(self, storage: T) -> Executor<T> {
        Executor {
            storage,
            driver: self.driver,
            seed: self.seed,
        }
    }
}

impl<S: StorageFactory> Executor<S> {
    pub fn enter<R>(
        self,
        f: impl for<'scope, 'd> FnOnce(Session<'scope, 'd, S::Output<'d>>) -> R,
    ) -> R {
        let Self {
            storage,
            driver,
            seed,
        } = self;
        let mut driver = pin!(driver);
        driver.as_mut().scope(move |mut access, token| {
            let storage = storage.build(&mut access.reborrow());
            let storage = pin!(storage);
            let mut core = SessionCore {
                driver: access,
                seed,
                token,
            };
            f(Session {
                storage: storage.as_ref(),
                core: &mut core,
            })
        })
    }
}

pub struct Session<'scope, 'd: 'scope, S = ()> {
    storage: Pin<&'scope S>,
    core: &'scope mut SessionCore<'d>,
}

struct SessionCore<'d> {
    driver: DriverContext<'d, 'd>,
    seed: Seed,
    token: BrandToken<'d>,
}

pub struct AppSession<'a, 'scope, 'd: 'scope, S, D> {
    session: &'a mut Session<'scope, 'd, S>,
    cell: Pin<&'a BrandCell<'d, D>>,
    shutdown: &'a Cell<bool>,
}

struct AppScope<'a, 'scope, 'd: 'scope, S, D>
where
    D: Dispatcher<'d>,
{
    session: &'a mut Session<'scope, 'd, S>,
    cell: Pin<&'a BrandCell<'d, D>>,
    shutdown: Cell<bool>,
}

impl<'scope, 'd: 'scope, S, D> Drop for AppScope<'_, 'scope, 'd, S, D>
where
    D: Dispatcher<'d>,
{
    fn drop(&mut self) {
        if self.shutdown.replace(true) {
            return;
        }
        let core = &mut *self.session.core;
        Dispatcher::shutdown(
            self.cell.borrow_pin_mut(&mut core.token),
            &mut core.driver.reborrow(),
        );
    }
}

impl Drop for SessionCore<'_> {
    fn drop(&mut self) {
        // Storage may own buffers referenced by in-flight operations. The backend
        // must stop touching them before Rust starts dropping the fields below.
        self.driver.prepare_drop();
    }
}

impl<'scope, 'd: 'scope, S> Session<'scope, 'd, S> {
    pub fn storage(&self) -> &'scope S {
        self.storage.get_ref()
    }

    pub fn storage_pin(&self) -> Pin<&'scope S> {
        self.storage
    }

    pub fn storage_and_driver(&mut self) -> (Pin<&'scope S>, DriverContext<'_, 'd>) {
        (self.storage, self.core.driver.reborrow())
    }

    #[doc(hidden)]
    pub fn token_and_driver(&mut self) -> (&mut BrandToken<'d>, DriverContext<'_, 'd>) {
        let core = &mut *self.core;
        (&mut core.token, core.driver.reborrow())
    }

    pub fn driver(&self) -> DriverRef<'d> {
        self.core.driver.driver_ref()
    }

    #[doc(hidden)]
    pub fn driver_access(&mut self) -> DriverContext<'_, 'd> {
        self.core.driver.reborrow()
    }

    pub const fn seed(&self) -> Seed {
        self.core.seed
    }

    pub fn token(&mut self) -> &mut BrandToken<'d> {
        &mut self.core.token
    }

    pub fn with_app<D, R>(
        &mut self,
        app: D,
        f: impl for<'a> FnOnce(AppSession<'a, 'scope, 'd, S, D>) -> R,
    ) -> R
    where
        D: Dispatcher<'d>,
    {
        let cell = pin!(BrandCell::new(app));
        let scope = AppScope {
            session: self,
            cell: cell.as_ref(),
            shutdown: Cell::new(false),
        };
        f(AppSession {
            session: scope.session,
            cell: scope.cell,
            shutdown: &scope.shutdown,
        })
    }

    #[doc(hidden)]
    pub fn block_on_with<D, R, T>(
        &mut self,
        dispatcher: Pin<&BrandCell<'d, D>>,
        root: R,
    ) -> io::Result<T>
    where
        D: Dispatcher<'d>,
        R: RootTask<'d, T>,
    {
        Self::block_on_parts(
            &mut self.core.driver,
            &mut self.core.token,
            dispatcher,
            root,
            None,
        )
    }

    fn block_on_parts<D, R, T>(
        driver: &mut DriverContext<'_, 'd>,
        token: &mut BrandToken<'d>,
        dispatcher: Pin<&BrandCell<'d, D>>,
        root: R,
        shutdown: Option<&Cell<bool>>,
    ) -> io::Result<T>
    where
        D: Dispatcher<'d>,
        R: RootTask<'d, T>,
    {
        let cell = dispatcher;
        let mut one_shot = pin!(root);
        let one_shot_target = one_shot.as_ref().target();
        // Register the root before advancing already-ready application tasks.
        // Otherwise a bounded ready cascade can complete and release a scarce
        // resource before the root has had a chance to join its handoff queue.
        driver.refresh_turn_clock();
        one_shot.as_mut().pre_park(&mut driver.reborrow());
        let mut poll_one_shot = false;
        let mut buf = [Cqe::ZERO; DRAIN_BATCH];
        loop {
            let state = Self::drive_batch(driver, cell, token, &mut buf, |target| {
                if target == one_shot_target {
                    poll_one_shot = true;
                    true
                } else {
                    false
                }
            });
            if take(&mut poll_one_shot) {
                one_shot.as_mut().pre_park(&mut driver.reborrow());
            }
            Self::prepare_park(driver, cell, token);
            if state.shutdown_seen {
                Self::finish_shutdown(driver, cell, token, D::SHUTDOWN_DRAIN, shutdown)?;
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "runtime shut down while blocking on a root task",
                ));
            }
            if let Some(output) = one_shot.as_mut().take_output() {
                driver.wait(Some(Duration::ZERO))?;
                return Ok(output);
            }
            let timeout = Self::park_timeout(driver, cell, token, state.cq_saturated);
            driver.wait(timeout)?;
        }
    }

    pub fn run<D: Dispatcher<'d>>(&mut self, dispatcher: Pin<&BrandCell<'d, D>>) -> io::Result<()> {
        Self::run_parts(
            &mut self.core.driver,
            &mut self.core.token,
            dispatcher,
            None,
        )
    }

    fn run_parts<D: Dispatcher<'d>>(
        driver: &mut DriverContext<'_, 'd>,
        token: &mut BrandToken<'d>,
        dispatcher: Pin<&BrandCell<'d, D>>,
        shutdown: Option<&Cell<bool>>,
    ) -> io::Result<()> {
        let cell = dispatcher;
        let mut buf = [Cqe::ZERO; DRAIN_BATCH];
        loop {
            let state = Self::drive_batch(driver, cell, token, &mut buf, |_| false);
            Self::prepare_park(driver, cell, token);
            if state.shutdown_seen {
                return Self::finish_shutdown(driver, cell, token, D::SHUTDOWN_DRAIN, shutdown);
            }
            let timeout = Self::park_timeout(driver, cell, token, state.cq_saturated);
            driver.wait(timeout)?;
        }
    }

    fn drive_batch<D, F>(
        driver: &mut DriverContext<'_, 'd>,
        cell: Pin<&BrandCell<'d, D>>,
        token: &mut BrandToken<'d>,
        buf: &mut [Cqe; DRAIN_BATCH],
        mut consume_ready: F,
    ) -> BatchState
    where
        D: Dispatcher<'d>,
        F: FnMut(Token) -> bool,
    {
        driver.refresh_turn_clock();
        let n = driver.drain(buf);
        let mut shutdown_seen = false;
        for cqe in &buf[..n] {
            if cqe.user_data == SHUTDOWN.raw() {
                shutdown_seen = true;
                continue;
            }
            let Ok(ev) = Event::decode(*cqe) else {
                continue;
            };
            Dispatcher::dispatch(cell.borrow_pin_mut(token), ev, &mut driver.reborrow());
        }
        for _ in 0..READY_DRAIN_ROUNDS {
            let ready = driver.driver_ref();
            if !ready.has_ready() {
                break;
            }
            ready.drain_ready(|target| {
                if !consume_ready(target) {
                    Dispatcher::activate(
                        cell.borrow_pin_mut(token),
                        target,
                        &mut driver.reborrow(),
                    );
                }
            });
        }
        BatchState {
            cq_saturated: n == buf.len(),
            shutdown_seen,
        }
    }

    fn prepare_park<D: Dispatcher<'d>>(
        driver: &mut DriverContext<'_, 'd>,
        cell: Pin<&BrandCell<'d, D>>,
        token: &mut BrandToken<'d>,
    ) {
        // Application callbacks can be arbitrarily long. Refresh after them so
        // expiration and the subsequent park timeout never use the batch-entry
        // snapshot from before user work.
        driver.refresh_turn_clock();
        Dispatcher::pre_park(cell.borrow_pin_mut(token), &mut driver.reborrow());
    }

    fn park_timeout<D: Dispatcher<'d>>(
        driver: &DriverContext<'_, 'd>,
        cell: Pin<&BrandCell<'d, D>>,
        token: &BrandToken<'d>,
        cq_saturated: bool,
    ) -> Option<Duration> {
        if cq_saturated || driver.driver_ref().has_ready() {
            return Some(Duration::ZERO);
        }
        match Dispatcher::idle(cell.borrow_pin(token)) {
            Idle::Busy => Some(Duration::ZERO),
            Idle::Park(None) => None,
            Idle::Park(Some(deadline)) => {
                Some(deadline.saturating_duration_since(driver.turn_now()))
            }
        }
    }

    fn drain_loop<D: Dispatcher<'d>>(
        driver: &mut DriverContext<'_, 'd>,
        cell: Pin<&BrandCell<'d, D>>,
        token: &mut BrandToken<'d>,
        drain_window: Duration,
    ) -> io::Result<()> {
        let deadline = saturating_deadline(Instant::now(), drain_window);
        let mut buf = [Cqe::ZERO; DRAIN_BATCH];
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(());
            }
            let state = Self::drive_batch(driver, cell, token, &mut buf, |_| false);
            Self::prepare_park(driver, cell, token);
            if !state.cq_saturated
                && !driver.driver_ref().has_ready()
                && matches!(Dispatcher::idle(cell.borrow_pin(token)), Idle::Park(None))
            {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(driver.turn_now());
            let timeout = Self::park_timeout(driver, cell, token, state.cq_saturated)
                .map_or(remaining, |timeout| timeout.min(remaining));
            driver.wait(Some(timeout))?;
        }
    }

    fn finish_shutdown<D: Dispatcher<'d>>(
        driver: &mut DriverContext<'_, 'd>,
        cell: Pin<&BrandCell<'d, D>>,
        token: &mut BrandToken<'d>,
        drain_window: Duration,
        shutdown: Option<&Cell<bool>>,
    ) -> io::Result<()> {
        let should_shutdown = match shutdown {
            Some(state) => !state.replace(true),
            None => true,
        };
        if should_shutdown {
            Dispatcher::shutdown(cell.borrow_pin_mut(token), &mut driver.reborrow());
        }
        Self::drain_loop(driver, cell, token, drain_window)
    }
}

impl<'a, 'scope, 'd: 'scope, S, D> AppSession<'a, 'scope, 'd, S, D>
where
    D: Dispatcher<'d>,
{
    #[doc(hidden)]
    pub fn driver(&self) -> DriverRef<'d> {
        self.session.driver()
    }

    #[doc(hidden)]
    pub fn block_on_with<R, T>(&mut self, root: R) -> io::Result<T>
    where
        R: RootTask<'d, T>,
    {
        let core = &mut *self.session.core;
        Session::<S>::block_on_parts(
            &mut core.driver,
            &mut core.token,
            self.cell,
            root,
            Some(self.shutdown),
        )
    }

    pub fn run(&mut self) -> io::Result<()> {
        let core = &mut *self.session.core;
        Session::<S>::run_parts(
            &mut core.driver,
            &mut core.token,
            self.cell,
            Some(self.shutdown),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::pin::{Pin, pin};
    use std::time::{Duration, Instant};

    use o3::cell::BrandCell;
    use pin_project::pin_project;

    use super::{DRAIN_BATCH, READY_DRAIN_ROUNDS, Session};
    use crate::driver::ready::ReadySlot;
    use crate::driver::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
    use crate::runtime::profile::Throughput;
    use crate::runtime::{Dispatcher, Executor, Idle};
    use crate::{Cqe, DriverContext, Event, driver};

    #[pin_project]
    struct CascadingReady<'d> {
        #[pin]
        ready: ReadySlot<'d>,
        polls: Cell<usize>,
        stop: usize,
    }

    #[pin_project]
    struct TurnClockProbe<'d> {
        #[pin]
        ready: ReadySlot<'d>,
        batch_times: Cell<Option<(Instant, Instant)>>,
        pre_park_time: Cell<Option<Instant>>,
    }

    impl<'d> Dispatcher<'d> for CascadingReady<'d> {
        fn dispatch(self: Pin<&mut Self>, _ev: Event, _driver: &mut DriverContext<'_, 'd>) {}

        fn activate(self: Pin<&mut Self>, _target: Token, _driver: &mut DriverContext<'_, 'd>) {
            let this = self.project();
            let polls = this.polls.get() + 1;
            this.polls.set(polls);
            if polls < *this.stop {
                this.ready.as_ref().activate();
            }
        }

        fn pre_park(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {}

        fn idle(self: Pin<&Self>) -> Idle {
            Idle::Park(None)
        }
    }

    impl<'d> Dispatcher<'d> for TurnClockProbe<'d> {
        fn dispatch(self: Pin<&mut Self>, _ev: Event, _driver: &mut DriverContext<'_, 'd>) {}

        fn activate(self: Pin<&mut Self>, _target: Token, driver: &mut DriverContext<'_, 'd>) {
            let start = driver.turn_now();
            std::thread::sleep(Duration::from_millis(20));
            self.project()
                .batch_times
                .set(Some((start, driver.turn_now())));
        }

        fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
            self.project().pre_park_time.set(Some(driver.turn_now()));
        }

        fn idle(self: Pin<&Self>) -> Idle {
            Idle::Park(None)
        }
    }

    #[test]
    fn ready_cascade_is_followed_but_bounded_per_driver_turn() {
        let config = driver::Config::for_tcp_profile::<Throughput>(1);
        Executor::new(config)
            .expect("executor")
            .enter(|mut session| {
                let target = Token::new(ROUTE_FRAMEWORK - 1, SlotIndex::new(0), Epoch::INITIAL);
                let ready = session.driver().make_ready_slot(target);
                let app = pin!(BrandCell::new(CascadingReady {
                    ready,
                    polls: Cell::new(0),
                    stop: READY_DRAIN_ROUNDS + 2,
                }));

                {
                    let (token, mut access) = session.token_and_driver();
                    app.as_ref()
                        .borrow_pin_mut(token)
                        .project()
                        .ready
                        .as_ref()
                        .activate();
                    let mut completions = [Cqe::ZERO; DRAIN_BATCH];
                    let _ = Session::<()>::drive_batch(
                        &mut access,
                        app.as_ref(),
                        token,
                        &mut completions,
                        |_| false,
                    );
                }

                assert_eq!(
                    app.as_ref()
                        .borrow_pin(session.token())
                        .project_ref()
                        .polls
                        .get(),
                    READY_DRAIN_ROUNDS
                );
                assert!(session.driver().has_ready());

                {
                    let (token, mut access) = session.token_and_driver();
                    let mut completions = [Cqe::ZERO; DRAIN_BATCH];
                    let _ = Session::<()>::drive_batch(
                        &mut access,
                        app.as_ref(),
                        token,
                        &mut completions,
                        |_| false,
                    );
                }

                assert_eq!(
                    app.as_ref()
                        .borrow_pin(session.token())
                        .project_ref()
                        .polls
                        .get(),
                    READY_DRAIN_ROUNDS + 2
                );
                assert!(!session.driver().has_ready());
            });
    }

    #[test]
    fn turn_clock_is_stable_during_callbacks_and_refreshed_before_park() {
        let config = driver::Config::for_tcp_profile::<Throughput>(1);
        Executor::new(config)
            .expect("executor")
            .enter(|mut session| {
                let target = Token::new(ROUTE_FRAMEWORK - 1, SlotIndex::new(0), Epoch::INITIAL);
                let ready = session.driver().make_ready_slot(target);
                let app = pin!(BrandCell::new(TurnClockProbe {
                    ready,
                    batch_times: Cell::new(None),
                    pre_park_time: Cell::new(None),
                }));

                {
                    let (token, mut access) = session.token_and_driver();
                    app.as_ref()
                        .borrow_pin_mut(token)
                        .project()
                        .ready
                        .as_ref()
                        .activate();
                    let mut completions = [Cqe::ZERO; DRAIN_BATCH];
                    let _ = Session::<()>::drive_batch(
                        &mut access,
                        app.as_ref(),
                        token,
                        &mut completions,
                        |_| false,
                    );
                    Session::<()>::prepare_park(&mut access, app.as_ref(), token);
                }

                let app = app.as_ref().borrow_pin(session.token());
                let (batch_start, batch_end) = app
                    .project_ref()
                    .batch_times
                    .get()
                    .expect("ready callback was not driven");
                let pre_park = app
                    .project_ref()
                    .pre_park_time
                    .get()
                    .expect("pre-park callback was not driven");
                assert_eq!(batch_start, batch_end);
                assert!(pre_park > batch_end);
            });
    }
}
