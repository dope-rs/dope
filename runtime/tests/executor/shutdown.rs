//! Runtime shutdown integration coverage.

use std::{cell::Cell, io, pin, rc::Rc, task, time};

use dope_core::driver::{
    route,
    schedule::{ready::completion::Waker, timer::Registration},
    settings,
};
use dope_runtime::{
    executor::{self, Executor},
    shutdown,
};
use dope_test::dispatch;

struct CountingDispatcher {
    shutdowns: Rc<Cell<usize>>,
    finishes: Rc<Cell<usize>>,
    root_dropped: Option<Rc<Cell<bool>>>,
}

impl<'d> dispatch::Hooks<'d, ()> for CountingDispatcher {
    fn shutdown(&mut self, _ready: &mut ()) {
        if let Some(root_dropped) = &self.root_dropped {
            assert!(
                root_dropped.get(),
                "root must drop before the installed application shuts down"
            );
        }
        self.shutdowns.set(self.shutdowns.get() + 1);
    }

    fn finish(&mut self, _ready: &mut ()) {
        self.finishes.set(self.finishes.get() + 1);
    }
}

fn counting_probe<'d>(
    shutdowns: Rc<Cell<usize>>,
    finishes: Rc<Cell<usize>>,
    root_dropped: Option<Rc<Cell<bool>>>,
) -> dispatch::Probe<'d, (), CountingDispatcher, 0> {
    dispatch::Builder::new(CountingDispatcher {
        shutdowns,
        finishes,
        root_dropped,
    })
    .probe::<0>()
}

fn executor() -> Executor {
    Executor::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("executor")
}

#[test]
fn app_run_dispatches_shutdown_exactly_once() {
    let shutdowns = Rc::new(Cell::new(0));
    let finishes = Rc::new(Cell::new(0));
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    executor()
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            trigger.fire()?;
            session
                .with_app(
                    counting_probe(Rc::clone(&shutdowns), Rc::clone(&finishes), None),
                    |mut app| app.run(),
                )?
                .map(drop)
        })
        .expect("runtime shutdown");
    assert_eq!(shutdowns.get(), 1);
    assert_eq!(finishes.get(), 1);
}

#[test]
fn dropping_app_scope_finishes_exactly_once() {
    let shutdowns = Rc::new(Cell::new(0));
    let finishes = Rc::new(Cell::new(0));
    executor().enter(|mut session| {
        session
            .with_app(
                counting_probe(Rc::clone(&shutdowns), Rc::clone(&finishes), None),
                |_| {},
            )
            .expect("application teardown");
    });
    assert_eq!(shutdowns.get(), 1);
    assert_eq!(finishes.get(), 1);
}

#[test]
fn app_shutdown_does_not_wait_for_driver_global_timers() {
    let shutdowns = Rc::new(Cell::new(0));
    let finishes = Rc::new(Cell::new(0));
    executor().enter(|mut session| {
        let driver = session.driver_access().driver_ref();
        let target = driver
            .targets::<route::KeyTag<0>>()
            .bind(route::SlotIndex::ZERO, route::Epoch::INITIAL)
            .dispatch();
        let ready = driver.ready().make_ready_slot(target).expect("ready slot");
        let now = time::Instant::now();
        let timer = session.driver_access().timer();
        let pending = pin::pin!(Registration::with_deadline(
            timer,
            driver
                .scheduler()
                .deadline(now + time::Duration::from_secs(60)),
        ));
        assert_eq!(
            pending.as_ref().poll(
                driver.scheduler().deadline(now),
                Waker::from_ready(driver, ready.key()),
            ),
            task::Poll::Pending,
        );

        session
            .with_app(
                counting_probe(Rc::clone(&shutdowns), Rc::clone(&finishes), None),
                |_| {},
            )
            .expect("application teardown must ignore unrelated timers");

        assert!(pending.as_ref().cancel());
    });
    assert_eq!(shutdowns.get(), 1);
    assert_eq!(finishes.get(), 1);
}

struct RootDrop(Rc<Cell<bool>>);

impl Drop for RootDrop {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

impl<'d> executor::Root<'d> for RootDrop {
    type Output = ();

    fn poll(_context: executor::RootContext<'_, 'd, Self>) -> std::task::Poll<Self::Output> {
        std::task::Poll::Pending
    }
}

#[test]
fn terminal_block_on_drops_root_then_finishes_installed_owner_once() {
    let shutdowns = Rc::new(Cell::new(0));
    let finishes = Rc::new(Cell::new(0));
    let root_dropped = Rc::new(Cell::new(false));
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    trigger.fire().expect("fire shutdown");
    executor()
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            let error = session.with_app(
                counting_probe(
                    Rc::clone(&shutdowns),
                    Rc::clone(&finishes),
                    Some(Rc::clone(&root_dropped)),
                ),
                |mut app| app.drive(RootDrop(Rc::clone(&root_dropped))).unwrap_err(),
            )?;
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            Ok(())
        })
        .expect("terminal block_on");
    assert!(root_dropped.get());
    assert_eq!(shutdowns.get(), 1);
    assert_eq!(finishes.get(), 1);
}
