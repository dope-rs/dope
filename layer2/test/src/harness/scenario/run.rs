use dope_core::platform::{thread, wake};
use dope_runtime::{process::STACK, shutdown};
use o3::alloc;

use crate::harness::scenario::process::deadline;

/// Failure before one owned worker starts.
#[derive(Debug)]
pub enum Start {
    Allocation(alloc::Error),
    Thread(thread::Error),
}

/// Runs the parent locally and returns the worker's output after its join.
pub trait Task: thread::Task + Sized {
    fn with<R>(self, run: impl FnOnce() -> R) -> Result<(R, Self::Output), Start> {
        scope(self, |join| (run(), join()))
    }
}

impl<T: thread::Task> Task for T {}

/// The borrowing join authority stays private to consuming harness views.
pub(crate) fn scope<T: thread::Task, R>(
    task: T,
    run: impl for<'scope> FnOnce(&'scope mut dyn FnMut() -> T::Output) -> R,
) -> Result<R, Start> {
    let mut group = thread::Builder::try_new(1).map_err(Start::Allocation)?;
    group.try_push(task, ()).map_err(Start::Allocation)?;
    group.scope(|mut workers| {
        workers
            .start(STACK)
            .map_err(|(_, error)| Start::Thread(error))?;
        let mut workers = Some(workers);
        let mut join = || {
            let Some(workers) = workers.take() else {
                unreachable!("joined")
            };
            match workers.finish().next() {
                Some(((), output)) => output,
                None => unreachable!("worker"),
            }
        };
        Ok(run(&mut join))
    })
}

/// An entry closure moved only across the worker creation and join boundary.
pub struct Entry<F>(pub F);

impl<F: FnOnce() -> O + Send, O: Send> thread::Task for Entry<F> {
    type Output = O;

    fn run(self) -> O {
        self.0()
    }
}

/// Exact failure boundary of a managed test scenario.
#[derive(Debug)]
pub enum Error<E> {
    Deadline(deadline::Error),
    Start(Start),
    ShutdownCreate(wake::CreateError),
    ShutdownFire(shutdown::FireError),
    Readiness(wake::WaitError),
    Server(E),
}
