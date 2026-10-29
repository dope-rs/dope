use std::convert;

use dope_core::{platform::signal, random};
use o3::mem::slot;

use crate::{
    process::{self, failure::execute, gate, layout, watch},
    shutdown,
};

pub(in crate::process) mod bus;
mod control;
pub(in crate::process) mod worker;

/// Owns process workers through cooperative shutdown and joining.
///
/// Enter on the original thread during single-threaded startup. Worker entries
/// must observe shutdown and return; every started worker is joined before these
/// methods return. A stalled worker can keep the caller waiting indefinitely.
/// Heartbeat callbacks do not interrupt worker execution.
pub struct Runtime<I> {
    pub(in crate::process) workers: layout::Set<I>,
}

impl<I> Runtime<I> {
    /// Runs workers whose application readiness is their executor-install edge.
    pub fn run<'r, E, X>(
        self,
        storage: slot::Vacant<'r, signal::Receiver>,
        entry: E,
    ) -> Result<(), execute::Error<'r, convert::Infallible, X>>
    where
        I: Send,
        E: for<'run> Fn(
                I,
                process::Context<'run>,
            ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X>
            + Clone
            + Send,
        X: Send,
    {
        let entry = worker::Plain::new(entry);
        let Self { workers } = self;
        signal::Source::scope(storage, move |signal| {
            run_waiting(workers, entry, control::Control::signal(signal))
        })
        .map_err(execute::Error::from)
    }

    /// Runs workers and supervises an explicit steady-state heartbeat.
    pub fn run_watched<'r, N, E, X>(
        self,
        storage: slot::Vacant<'r, signal::Receiver>,
        entry: E,
        notify: N,
    ) -> Result<(), execute::Error<'r, N::Error, X>>
    where
        I: Send,
        N: watch::Notify,
        E: for<'run> Fn(
                I,
                process::Context<'run>,
            ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X>
            + Clone
            + Send,
        X: Send,
    {
        self.watch_inner(storage, worker::Plain::new(entry), notify)
    }

    /// Runs gated workers and supervises an explicit steady-state heartbeat.
    pub fn watch_gated<'r, N, E, X>(
        self,
        storage: slot::Vacant<'r, signal::Receiver>,
        entry: E,
        notify: N,
    ) -> Result<(), execute::Error<'r, N::Error, X>>
    where
        I: Send,
        N: watch::Notify,
        E: for<'run> Fn(
                I,
                process::Context<'run>,
                gate::Closed<'run>,
            ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X>
            + Clone
            + Send,
        X: Send,
    {
        self.watch_inner(storage, worker::Gated::new(entry), notify)
    }

    fn watch_inner<'r, N, E, X>(
        self,
        storage: slot::Vacant<'r, signal::Receiver>,
        entry: E,
        notify: N,
    ) -> Result<(), execute::Error<'r, N::Error, X>>
    where
        I: Send,
        N: watch::Notify,
        E: worker::Entry<I, X>,
        X: Send,
    {
        let Self { workers } = self;
        signal::Source::scope(storage, move |signal| {
            run_waiting_watched(workers, entry, notify, control::Control::signal(signal))
        })
        .map_err(execute::Error::from)
    }
}

fn run_waiting<'r, I, E, X>(
    workers: layout::Set<I>,
    entry: worker::Plain<E>,
    control: control::Control<'_>,
) -> Result<(), execute::Error<'r, convert::Infallible, X>>
where
    I: Send,
    worker::Plain<E>: worker::Entry<I, X>,
    X: Send,
{
    let count = workers.len();
    let seed = random::Seed::acquire().map_err(execute::Cause::Entropy)?;
    worker::scope(workers, seed, entry, |running| {
        let outcome = control.ready(count, &running);
        let stopped = running.stop();
        match outcome {
            Ok(()) => stopped.clean(),
            Err(error) => Err(stopped.fail(error)),
        }
    })
}

fn run_waiting_watched<'r, I, N, E, X>(
    workers: layout::Set<I>,
    entry: E,
    notify: N,
    control: control::Control<'_>,
) -> Result<(), execute::Error<'r, N::Error, X>>
where
    I: Send,
    N: watch::Notify,
    E: worker::Entry<I, X>,
    X: Send,
{
    let count = workers.len();
    let seed = random::Seed::acquire().map_err(execute::Cause::Entropy)?;
    worker::scope(workers, seed, entry, |running| {
        let outcome = control.watch(count, notify, &running);
        let stopped = running.stop();
        let outcome = outcome.and_then(|clock| match clock {
            Some(clock) => {
                control::Finish::new(&stopped).map_or(Ok(()), |finish| finish.drain(count, clock))
            }
            None => Ok(()),
        });
        match outcome {
            Ok(()) => stopped.clean(),
            Err(error) => Err(stopped.fail(error)),
        }
    })
}
