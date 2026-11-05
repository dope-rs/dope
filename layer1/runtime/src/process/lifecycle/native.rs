use std::cell::Cell;

use dope_core::platform::thread;
use o3::alloc;

use crate::process::STACK;

/// Failure before one owned worker starts.
#[derive(Debug)]
pub enum Start {
    Allocation(alloc::Error),
    Thread(thread::Error),
}

/// Runs the parent locally and returns the worker's output after its join.
pub trait Task: thread::Task + Sized {
    fn with<R>(self, run: impl FnOnce() -> R) -> Result<(R, Self::Output), Start> {
        scope(self, |join| (run(), join.finish()))
    }
}

impl<T: thread::Task> Task for T {}

pub struct Join<'scope, O>(&'scope dyn Fn() -> O);

impl<O> Join<'_, O> {
    pub fn finish(self) -> O {
        self.0()
    }
}

/// Lends the worker's join authority to the local callback.
pub fn scope<T: thread::Task, R>(
    task: T,
    run: impl for<'scope> FnOnce(Join<'scope, T::Output>) -> R,
) -> Result<R, Start> {
    let mut group = thread::Builder::try_new(1).map_err(Start::Allocation)?;
    group.try_push(task, ()).map_err(Start::Allocation)?;
    group.scope(|mut workers| {
        workers
            .start(STACK)
            .map_err(|(_, error)| Start::Thread(error))?;
        let workers = Cell::new(Some(workers));
        let join = || {
            let Some(workers) = workers.take() else {
                unreachable!("joined")
            };
            match workers.finish().next() {
                Some(((), output)) => output,
                None => unreachable!("worker"),
            }
        };
        Ok(run(Join(&join)))
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
