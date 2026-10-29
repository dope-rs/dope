use std::{convert, mem};

use dope_core::{
    dope_sys::thread::signal::Signal as _,
    platform::{affinity, signal},
};

use crate::process::{
    failure::{
        bus::{Drain, DrainCause, Event, Shutdown, Startup, State},
        execute,
    },
    runtime::worker::{self, bound},
    watch,
};

mod clock;
pub(in crate::process) mod set;

use clock::Clock;

type Observation<N> = Result<
    Option<Clock<<N as watch::Notify>::Heartbeat>>,
    execute::Cause<<N as watch::Notify>::Error>,
>;

pub(super) struct Control<'process> {
    source: signal::Source<'process>,
}

#[repr(transparent)]
pub(super) struct Finish<'a, 'run, I, E, W> {
    running: &'a worker::Stopping<'run, I, E, W>,
}

impl<'process> Control<'process> {
    pub(super) const fn signal(source: signal::Source<'process>) -> Self {
        Self { source }
    }

    fn wait_ready<'signal, 'id, S, I, E, W>(
        workers: usize,
        running: &bound::Bound<'id, I, E, W>,
        poll: set::Set<'id, 'signal, set::Both<'signal, 'process>>,
    ) -> Result<Option<set::Set<'id, 'signal, set::Both<'signal, 'process>>>, execute::Cause<S>>
    where
        E: worker::Entry<I, W>,
    {
        let mut poll = poll;
        let mut installs = workers;
        let mut ready = if E::GATED { workers } else { 0 };
        while installs != 0 || ready != 0 {
            let (next, source) = poll.next()?;
            poll = next;
            let index = match source {
                set::Ready::Signal(source) => {
                    source.wait().map_err(execute::Cause::TerminationWait)?;
                    return Ok(None);
                }
                set::Ready::Worker(index) => index,
            };
            match execute::Cause::<S>::received(running.main(index).recv(), running.cpu(index))? {
                Event::Installed if installs != 0 => installs -= 1,
                Event::Ready if ready != 0 => ready -= 1,
                event => {
                    let cpu = running.cpu(index);
                    return Err(execute::Cause::BusState(State::StartupEvent(Startup {
                        cpu,
                        event,
                        workers,
                        installed: installs,
                        ready,
                        gated: E::GATED,
                    })));
                }
            }
        }
        Ok(Some(poll))
    }

    fn observe<'signal, 'id, S, I, E, W>(
        running: &bound::Bound<'id, I, E, W>,
        poll: set::Set<'id, 'signal, set::Both<'signal, 'process>>,
    ) -> Result<(), execute::Cause<S>> {
        let index = match poll.wait()? {
            set::Ready::Signal(source) => {
                source.wait().map_err(execute::Cause::TerminationWait)?;
                return Ok(());
            }
            set::Ready::Worker(index) => index,
        };
        let event = execute::Cause::<S>::received(running.main(index).recv(), running.cpu(index))?;
        Err(execute::Cause::BusState(State::RuntimeEvent {
            cpu: running.cpu(index),
            event,
        }))
    }

    fn drive<'signal, 'id, S, I, E, W, H>(
        clock: Clock<H>,
        running: &bound::Bound<'id, I, E, W>,
        poll: set::Set<'id, 'signal, set::Both<'signal, 'process>>,
    ) -> Result<Clock<H>, execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
    {
        let mut poll = poll;
        let mut clock = clock;
        loop {
            let (next, polled, ready) = clock.wait(poll, |ready| (ready, false))?;
            clock = next;
            poll = polled;
            let index = match ready {
                None => continue,
                Some(set::Ready::Signal(source)) => {
                    source.wait().map_err(execute::Cause::TerminationWait)?;
                    return Ok(clock);
                }
                Some(set::Ready::Worker(index)) => index,
            };
            let event =
                execute::Cause::<S>::received(running.main(index).recv(), running.cpu(index))?;
            return Err(execute::Cause::BusState(State::RuntimeEvent {
                cpu: running.cpu(index),
                event,
            }));
        }
    }

    pub(super) fn ready<I, E, W>(
        &self,
        workers: usize,
        running: &worker::Running<'_, I, E, W>,
    ) -> Result<(), execute::Cause<convert::Infallible>>
    where
        E: worker::Entry<I, W>,
    {
        running.scope(|running| {
            let poll = set::Set::both(&self.source, &running)?;
            match Self::wait_ready(workers, &running, poll)? {
                Some(poll) => Self::observe(&running, poll),
                None => Ok(()),
            }
        })
    }

    pub(super) fn watch<N, I, E, W>(
        &self,
        workers: usize,
        notify: N,
        running: &worker::Running<'_, I, E, W>,
    ) -> Observation<N>
    where
        N: watch::Notify,
        E: worker::Entry<I, W>,
    {
        running.scope(|running| {
            let poll = set::Set::both(&self.source, &running)?;
            let Some(poll) = Self::wait_ready(workers, &running, poll)? else {
                return Ok(None);
            };
            let beat = notify.ready().map_err(execute::Cause::WatchReady)?;
            let clock = Clock::new(beat)?;
            Self::drive(clock, &running, poll).map(Some)
        })
    }
}

#[derive(Default)]
struct Counts {
    drained: usize,
    done: usize,
}

impl Counts {
    fn record<S>(
        &mut self,
        event: Event,
        cpu: affinity::Cpu,
        workers: usize,
    ) -> Result<(), execute::Cause<S>> {
        match event {
            Event::Drain => self.drained += 1,
            Event::Done => self.done += 1,
            event => {
                return Err(execute::Cause::BusState(State::DrainEvent(Drain {
                    cause: DrainCause::Event { cpu, event },
                    workers,
                    drained: self.drained,
                    done: self.done,
                })));
            }
        }
        if self.done == workers && self.drained != workers {
            return Err(execute::Cause::BusState(State::DrainOrder(Drain {
                cause: DrainCause::Order,
                workers,
                drained: self.drained,
                done: self.done,
            })));
        }
        Ok(())
    }
}

impl<'a, 'run, I, E, W> Finish<'a, 'run, I, E, W> {
    pub(super) const fn new(running: &'a worker::Stopping<'run, I, E, W>) -> Option<Self> {
        if running.can_observe() {
            Some(Self { running })
        } else {
            None
        }
    }

    pub(super) fn drain<S, H>(
        self,
        workers: usize,
        mut clock: Clock<H>,
    ) -> Result<(), execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
        E: worker::Entry<I, W>,
    {
        // SAFETY: construction retains every live Main throughout this loan.
        unsafe {
            self.running.scope(|running| {
                let mut poll = set::Set::bus(&running)?;
                let mut count = Counts::default();
                while E::GATED && count.drained != workers {
                    let (next, polled, (index, event)) =
                        Self::next_watched(clock, poll, |index| {
                            let event = running.main(index).recv();
                            let retire = matches!(event, Ok(Event::Done));
                            ((index, event), retire)
                        })?;
                    clock = next;
                    poll = polled;
                    let cpu = running.cpu(index);
                    count.record(execute::Cause::<S>::received(event, cpu)?, cpu, workers)?;
                }
                clock = clock.draining()?;
                while count.done != workers {
                    let (next, polled, (index, event)) =
                        Self::next_watched(clock, poll, |index| {
                            let event = running.main(index).recv();
                            let retire = matches!(event, Ok(Event::Done));
                            ((index, event), retire)
                        })?;
                    clock = next;
                    poll = polled;
                    match execute::Cause::<S>::received(event, running.cpu(index))? {
                        Event::Done => count.done += 1,
                        event => {
                            return Err(execute::Cause::BusState(State::ShutdownEvent(Shutdown {
                                cpu: running.cpu(index),
                                event,
                                workers,
                                done: count.done,
                            })));
                        }
                    }
                }
                Ok(())
            })
        }
    }

    fn next_watched<'id, S, H, R>(
        clock: Clock<H>,
        poll: set::Set<'id, 'id, set::Workers>,
        mut receive: impl FnMut(set::Index<'id>) -> (R, bool),
    ) -> Result<(Clock<H>, set::Set<'id, 'id, set::Workers>, R), execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
    {
        let mut clock = clock;
        let mut poll = poll;
        loop {
            let (next, polled, ready) = clock.wait(poll, &mut receive)?;
            clock = next;
            poll = polled;
            match ready {
                None => {}
                Some(index) => return Ok((clock, poll, index)),
            }
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Finish<'static, 'static, (), (), ()>>()
            == mem::size_of::<&worker::Running<'static, (), (), ()>>()
    );
    assert!(
        mem::align_of::<Finish<'static, 'static, (), (), ()>>()
            == mem::align_of::<&worker::Running<'static, (), (), ()>>()
    );
};
