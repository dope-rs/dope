use std::future::Future;
use std::pin::{Pin, pin};
use std::time::{Duration, Instant};

use dope::fiber::Fiber;
use dope::runtime::park::Parker;
use dope::runtime::token::Token;
use dope::{Cqe, Dispatcher, Drive, Executor, Idle};

use crate::OneShot;

const ONESHOT_ROUTE: u8 = 255;
const PARK_CEILING: Duration = Duration::from_secs(1);

pub fn block_on<D: Dispatcher, F: Future>(
    exec: &mut Executor,
    mut dispatcher: Pin<&mut D>,
    fiber: Fiber<'_, F>,
) -> F::Output {
    let driver = exec.driver_mut();
    let mut one_shot = pin!(OneShot::new(fiber, ONESHOT_ROUTE, driver));
    let mut buf = [Cqe::ZERO; 256];
    let mut wake_buf: Vec<Token> = Vec::with_capacity(64);
    loop {
        let n = driver.drain(&mut buf);
        for cqe in &buf[..n] {
            if cqe.route() != ONESHOT_ROUTE {
                let Ok(ev) = dope::Event::try_from(*cqe) else {
                    continue;
                };
                Dispatcher::dispatch(dispatcher.as_mut(), ev, driver);
            }
        }
        wake_buf.clear();
        Parker::drain(driver, &mut wake_buf);
        for target in &wake_buf {
            if target.route() != ONESHOT_ROUTE {
                Dispatcher::on_wake(dispatcher.as_mut(), *target, driver);
            }
        }
        one_shot.as_mut().pre_park(driver);
        Dispatcher::pre_park(dispatcher.as_mut(), driver);
        if one_shot.as_ref().is_done() {
            return one_shot
                .as_mut()
                .take_output()
                .expect("OneShot reported done");
        }
        let timeout = if n == buf.len() || !Parker::is_empty(driver) {
            Duration::ZERO
        } else {
            match Dispatcher::idle(dispatcher.as_ref()) {
                Idle::Busy => Duration::ZERO,
                Idle::Park(None) => PARK_CEILING,
                Idle::Park(Some(d)) => {
                    PARK_CEILING.min(d.saturating_duration_since(Instant::now()))
                }
            }
        };
        let _ = driver.park(timeout);
    }
}
