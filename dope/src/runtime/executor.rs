use std::io;
use std::pin::Pin;
use std::time::{Duration, Instant};

use crate::runtime::dispatcher::Idle;
use crate::{Dispatcher, Drive, Driver, DriverCfg, backend};

const PARK_CEILING: Duration = Duration::from_secs(1);
/// Per-tick CQE drain batch. Also the single source of truth for the
/// provided-buffer pool sizing in [`crate::backend`] (the held buffer HWM is
/// bounded by one drain batch — see MEMORY_DESIGN.md section 0).
pub(crate) const DRAIN_BATCH: usize = 256;

pub struct Executor {
    driver: Driver,
}

impl Executor {
    pub fn new(cfg: DriverCfg) -> io::Result<Self> {
        let driver = <backend::Default as backend::Backend>::new_driver(cfg)?;
        Ok(Self { driver })
    }

    pub fn driver_mut(&mut self) -> &mut Driver {
        &mut self.driver
    }

    pub fn run<D: Dispatcher>(&mut self, mut dispatcher: Pin<&mut D>) -> io::Result<()> {
        let driver = &mut self.driver;
        let mut buf = [backend::Cqe::ZERO; DRAIN_BATCH];
        let mut wake_buf: Vec<backend::token::Token> = Vec::with_capacity(64);
        loop {
            let (cq_saturated, shutdown_seen) =
                Self::tick(driver, dispatcher.as_mut(), &mut buf, &mut wake_buf);
            if shutdown_seen {
                Dispatcher::on_shutdown(dispatcher.as_mut(), driver);
                return Self::drain_loop(driver, dispatcher.as_mut(), D::SHUTDOWN_DRAIN);
            }
            let timeout = Self::park_timeout(driver, dispatcher.as_ref(), cq_saturated);
            let _ = driver.park(timeout);
        }
    }

    fn tick<D: Dispatcher>(
        driver: &mut Driver,
        mut dispatcher: Pin<&mut D>,
        buf: &mut [backend::Cqe; DRAIN_BATCH],
        wake_buf: &mut Vec<backend::token::Token>,
    ) -> (bool, bool) {
        let n = driver.drain(buf);
        let mut shutdown_seen = false;
        for cqe in &buf[..n] {
            if cqe.user_data == backend::token::SHUTDOWN.raw() {
                shutdown_seen = true;
                continue;
            }
            let Ok(ev) = backend::Event::try_from(*cqe) else {
                continue;
            };
            Dispatcher::dispatch(dispatcher.as_mut(), ev, driver);
        }
        let cq_saturated = n == buf.len();
        wake_buf.clear();
        backend::park::Parker::drain(driver, wake_buf);
        for target in wake_buf.iter() {
            Dispatcher::on_wake(dispatcher.as_mut(), *target, driver);
        }
        Dispatcher::pre_park(dispatcher.as_mut(), driver);
        (cq_saturated, shutdown_seen)
    }

    fn park_timeout<D: Dispatcher>(
        driver: &Driver,
        dispatcher: Pin<&D>,
        cq_saturated: bool,
    ) -> Duration {
        if cq_saturated || !backend::park::Parker::is_empty(driver) {
            return Duration::ZERO;
        }
        match Dispatcher::idle(dispatcher) {
            Idle::Busy => Duration::ZERO,
            Idle::Park(None) => PARK_CEILING,
            Idle::Park(Some(d)) => PARK_CEILING.min(d.saturating_duration_since(Instant::now())),
        }
    }

    fn drain_loop<D: Dispatcher>(
        driver: &mut Driver,
        mut dispatcher: Pin<&mut D>,
        drain_window: Duration,
    ) -> io::Result<()> {
        let deadline = Instant::now() + drain_window;
        let mut buf = [backend::Cqe::ZERO; DRAIN_BATCH];
        let mut wake_buf: Vec<backend::token::Token> = Vec::with_capacity(64);
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(());
            }
            let (cq_saturated, _) =
                Self::tick(driver, dispatcher.as_mut(), &mut buf, &mut wake_buf);
            if !cq_saturated
                && backend::park::Parker::is_empty(driver)
                && matches!(Dispatcher::idle(dispatcher.as_ref()), Idle::Park(None))
            {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(now);
            let timeout =
                Self::park_timeout(driver, dispatcher.as_ref(), cq_saturated).min(remaining);
            let _ = driver.park(timeout);
        }
    }
}
