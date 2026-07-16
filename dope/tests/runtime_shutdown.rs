use std::cell::Cell;
use std::io;
use std::pin::Pin;
use std::rc::Rc;

use dope::driver;
use dope::runtime::profile::Throughput;
use dope::runtime::{Dispatcher, Executor, Idle, ShutdownTrigger};
use dope::{DriverContext, Event};
use dope_fiber::{AppSessionExt, Pending};

struct CountingDispatcher {
    shutdowns: Rc<Cell<usize>>,
}

impl<'d> Dispatcher<'d> for CountingDispatcher {
    fn dispatch(self: Pin<&mut Self>, _event: Event, _driver: &mut DriverContext<'_, 'd>) {}

    fn activate(
        self: Pin<&mut Self>,
        _target: dope::driver::token::Token,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
    }

    fn pre_park(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {}

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(None)
    }

    fn shutdown(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {
        let shutdowns = &self.as_ref().get_ref().shutdowns;
        shutdowns.set(shutdowns.get() + 1);
    }
}

fn executor() -> Executor {
    Executor::new(driver::Config::for_profile::<Throughput>()).expect("executor")
}

#[test]
fn app_run_dispatches_shutdown_exactly_once() {
    let shutdowns = Rc::new(Cell::new(0));
    executor()
        .enter(|mut session| -> io::Result<()> {
            let trigger = ShutdownTrigger::new()?;
            trigger.try_register(&mut session.driver_access())?;
            trigger.fire()?;
            session.with_app(
                CountingDispatcher {
                    shutdowns: Rc::clone(&shutdowns),
                },
                |mut app| app.run(),
            )
        })
        .expect("runtime shutdown");
    assert_eq!(shutdowns.get(), 1);
}

#[test]
fn app_block_on_is_interrupted_and_dispatches_shutdown_exactly_once() {
    let shutdowns = Rc::new(Cell::new(0));
    executor()
        .enter(|mut session| -> io::Result<()> {
            let trigger = ShutdownTrigger::new()?;
            trigger.try_register(&mut session.driver_access())?;
            trigger.fire()?;
            let error = session.with_app(
                CountingDispatcher {
                    shutdowns: Rc::clone(&shutdowns),
                },
                |mut app| app.block_on(Pending::<()>::default()).unwrap_err(),
            );
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            Ok(())
        })
        .expect("runtime shutdown");
    assert_eq!(shutdowns.get(), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn signal_shutdown_restores_the_calling_threads_mask() {
    fn membership(signal: libc::c_int) -> libc::c_int {
        let mut current = unsafe { std::mem::zeroed::<libc::sigset_t>() };
        let result =
            unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, std::ptr::null(), &mut current) };
        assert_eq!(result, 0);
        unsafe { libc::sigismember(&current, signal) }
    }

    let before = (membership(libc::SIGINT), membership(libc::SIGTERM));
    {
        let _signal = dope::runtime::SignalShutdown::new().expect("signal shutdown");
        assert_eq!(membership(libc::SIGINT), 1);
        assert_eq!(membership(libc::SIGTERM), 1);
    }
    assert_eq!(
        (membership(libc::SIGINT), membership(libc::SIGTERM)),
        before
    );
}
