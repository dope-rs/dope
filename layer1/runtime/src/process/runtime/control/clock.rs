use std::{mem, time};

use o3::base::time::Positive;

use crate::{
    executor::drive::clock,
    process::{
        failure::{config, execute},
        runtime::control::set,
        watch,
    },
};

pub(in crate::process) struct Clock<H> {
    heartbeat: H,
    period: Positive,
    next: time::Instant,
}

impl<H> Clock<H> {
    pub(in crate::process) fn new<S>(beat: H) -> Result<Self, execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
    {
        let period = beat.period();
        let now = time::Instant::now();
        let next = clock::Range::try_add(now, period.get())
            .map_err(config::Error::HeartbeatDeadline)
            .map_err(execute::Cause::Config)?;
        Ok(Self {
            heartbeat: beat,
            period,
            next,
        })
    }

    pub(in crate::process) fn wait<'worker, 'signal, S, M, R>(
        mut self,
        poll: set::Set<'worker, 'signal, M>,
        receive: impl FnMut(M::Ready<'worker>) -> (R, bool),
    ) -> Result<(Self, set::Set<'worker, 'signal, M>, Option<R>), execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
        M: set::Mode,
    {
        let (poll, ready) = poll.wait_until(self.next, receive)?;
        if ready.is_none() {
            self.pulse(time::Instant::now())?;
        }
        Ok((self, poll, ready))
    }

    pub(in crate::process) fn draining<S>(self) -> Result<Self, execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
    {
        self.heartbeat
            .draining()
            .map_err(execute::Cause::WatchDrain)?;
        Ok(self)
    }

    fn pulse<S>(&mut self, now: time::Instant) -> Result<(), execute::Cause<S>>
    where
        H: watch::Heartbeat<S>,
    {
        let deadline = clock::Range::try_add(now, self.period.get())
            .map_err(config::Error::HeartbeatDeadline)
            .map_err(execute::Cause::Config)?;
        self.heartbeat.pulse().map_err(execute::Cause::WatchPulse)?;
        self.next = deadline;
        Ok(())
    }
}

const _: () = {
    assert!(
        mem::size_of::<Clock<()>>()
            == mem::size_of::<time::Duration>() + mem::size_of::<time::Instant>()
    );
};
