extern crate dope;

use std::cell::Cell;
use std::io;
use std::pin::Pin;
use std::sync::mpsc;

use dope::runtime::profile::Throughput;
use dope::runtime::{Dispatcher, Executor, Idle, Launcher, WorkerContext, WorkerEntry};
use dope::{DriverContext, Event, driver};

struct Probe;

impl WorkerEntry for Probe {
    type Input = Cell<u8>;

    fn run(_input: Self::Input, _context: WorkerContext) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn rejects_empty_cpu_set() {
    let result = Launcher::pinned(Vec::new());
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(io::ErrorKind::InvalidInput)
    );
}

#[test]
fn rejects_input_count_mismatch() {
    let result = Launcher::unbound(1)
        .expect("launcher")
        .run::<Probe>(Vec::new());
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(io::ErrorKind::InvalidInput)
    );
}

#[test]
fn rejects_duplicate_cpus() {
    let result = Launcher::pinned(vec![1, 1]);
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(io::ErrorKind::InvalidInput)
    );
}

#[test]
fn rejects_zero_worker_stack_size() {
    let result = Launcher::unbound(1).expect("launcher").worker_stack_size(0);
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(io::ErrorKind::InvalidInput)
    );
}

struct ReportMetadata;

impl WorkerEntry for ReportMetadata {
    type Input = mpsc::Sender<(usize, Option<u16>)>;

    fn run(output: Self::Input, context: WorkerContext) -> io::Result<()> {
        let _shutdown = context.shutdown_trigger()?;
        output
            .send((context.worker(), context.cpu()))
            .map_err(|_| io::Error::other("metadata receiver closed"))
    }
}

#[test]
fn unbound_workers_report_stable_metadata() {
    let (output, reports) = mpsc::channel();
    let launcher = Launcher::unbound(2).expect("launcher");
    assert_eq!(launcher.worker_count(), 2);
    launcher
        .run::<ReportMetadata>(vec![output.clone(), output])
        .expect("workers");
    let mut reports = reports.try_iter().collect::<Vec<_>>();
    reports.sort_unstable();
    assert_eq!(reports, vec![(0, None), (1, None)]);
}

struct Parked;

impl<'d> Dispatcher<'d> for Parked {
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
}

enum SupervisedInput {
    Fail,
    Wait,
}

struct Supervised;

impl WorkerEntry for Supervised {
    type Input = SupervisedInput;

    fn run(input: Self::Input, context: WorkerContext) -> io::Result<()> {
        match input {
            SupervisedInput::Fail => Err(io::Error::other("worker failed")),
            SupervisedInput::Wait => {
                Executor::with_seed(driver::Config::for_profile::<Throughput>(), context.seed())?
                    .enter(|mut session| {
                        context.try_register_shutdown(&mut session.driver_access())?;
                        let app = std::pin::pin!(o3::cell::BrandCell::new(Parked));
                        session.run(app.as_ref())
                    })
            }
        }
    }
}

#[test]
fn worker_failure_stops_other_cooperative_workers() {
    let error = Launcher::unbound(2)
        .expect("launcher")
        .run::<Supervised>(vec![SupervisedInput::Fail, SupervisedInput::Wait])
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
}
