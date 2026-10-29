use std::{task::Poll, time::Duration};

use dope_fiber::{
    abi::{
        Fiber, Task,
        race::{Either, Race},
    },
    context::PollCall,
    task::time::sleep,
};

use crate::fibers::Delay;

pub struct Within<'d, F> {
    race: Task<Race<F, Delay<'d>>>,
}

impl<F> Within<'_, F> {
    pub(in crate::fibers) fn new(fiber: F, duration: Duration) -> Self {
        Self {
            race: Task::new(Race::new(fiber, Delay::new(duration))),
        }
    }
}

impl<'d, F: Fiber<'d>> Fiber<'d> for Within<'d, F> {
    type Output = Result<Option<F::Output>, sleep::Error>;
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        // SAFETY: the private race remains pinned and this owner has no Drop.
        let race = unsafe { this.map_unchecked_mut(|this| &mut this.race) };
        let (polled, _) = admit.poll(race);
        polled.map(|result| match result {
            Either::Left(output) => Ok(Some(output)),
            Either::Right(result) => result.map(|()| None),
        })
    }
}
