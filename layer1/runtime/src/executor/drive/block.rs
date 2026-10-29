use std::{mem, ops::ControlFlow, pin, task};

use dope_core::driver::{result::poll, route, route::indices, schedule, schedule::ready};
use drive::{clock, turn};
use schedule::clock::{Moment, Sample};
use scope::Roots as _;
use turn::{modes, routes, scope};

use crate::executor::{self, boot, drive, root};

/// Evidence that one root and its installed application reached a fixed point.
#[must_use = "fixed-point evidence and its root output must be observed"]
pub struct Fixed<'d, T> {
    output: T,
    now: Moment<'d>,
}

impl<'d, T> Fixed<'d, T> {
    pub(crate) const fn new(output: T, now: Moment<'d>) -> Self {
        Self { output, now }
    }

    pub const fn output(&self) -> &T {
        &self.output
    }

    pub fn into_output(self) -> T {
        self.output
    }

    pub const fn now(&self) -> Moment<'d> {
        self.now
    }
}

const _: () = {
    assert!(mem::size_of::<Fixed<'_, ()>>() == mem::size_of::<Moment<'_>>());
    assert!(mem::align_of::<Fixed<'_, ()>>() == mem::align_of::<Moment<'_>>());
};

pub(crate) struct Block<'d, T, C> {
    state: T,
    source: C,
    slot: ready::Slot<'d, routes::Wake>,
}

enum Attempt<R, O> {
    Complete(Result<O, drive::Error>),
    Cancel(R, drive::Error),
}

enum Outcome<O, P> {
    Complete(O, P),
    Shutdown(Option<O>),
}

impl<O, P> Outcome<O, P> {
    fn complete(self) -> Result<(O, P), drive::Error> {
        match self {
            Self::Complete(output, proof) => Ok((output, proof)),
            Self::Shutdown(_) => Err(drive::Error::Shutdown),
        }
    }
}

impl<R, O> Attempt<R, O> {
    fn finish<'d>(self) -> Result<O, drive::Error>
    where
        R: boot::Root<'d, Output = O>,
    {
        match self {
            Self::Complete(result) => result,
            Self::Cancel(root, source) => {
                R::cancel(root);
                Err(source)
            }
        }
    }
}

impl<'run, 'driver, 'd, C> Block<'d, scope::Boot<'run, 'driver, 'd>, C>
where
    C: clock::Source,
{
    pub(crate) fn run_boot<R>(
        start: Result<Self, drive::Error>,
        root: R,
    ) -> Result<R::Output, drive::Error>
    where
        R: boot::Root<'d>,
    {
        match start {
            Ok(block) => block.poll_boot(root),
            Err(source) => Attempt::Cancel(root, source),
        }
        .finish()
    }

    fn poll_boot<R>(mut self, mut root: R) -> Attempt<R, R::Output>
    where
        R: boot::Root<'d>,
    {
        let wake = self.slot.target();
        let (mut polled, output) = {
            let mut cause = boot::Cause::Start;
            loop {
                let tick = self.source.tick();
                let ready = match scope::Roots::next_root(&mut self.state, &self.slot, tick) {
                    Ok(ready) => ready,
                    Err(source) => return Attempt::Cancel(root, source.into()),
                };
                let polled = if matches!(cause, boot::Cause::Start) || ready.is_root() {
                    let (polled, output) = ready.poll_boot(root, wake, cause);
                    match output {
                        boot::Poll::Pending(next) => {
                            root = next;
                            polled
                        }
                        boot::Poll::Ready(output) => break (polled, output),
                    }
                } else {
                    ready.skip()
                };
                cause = boot::Cause::Wake;
                let parked = polled.park();
                match parked.status() {
                    turn::Status::Active => {}
                    turn::Status::Shutdown => return Attempt::Cancel(root, drive::Error::Shutdown),
                }
                let resolved = parked.resolve(self.source, |idle| {
                    let state = idle.progress();
                    Ok(turn::Resolved::Wait(idle, state))
                });
                if let Err(source) = resolved {
                    return Attempt::Cancel(root, source);
                }
            }
        };
        Attempt::Complete(loop {
            let parked = polled.park();
            match parked.status() {
                turn::Status::Active => {}
                turn::Status::Shutdown => break Err(drive::Error::Shutdown),
            }
            let drained = match parked.resolve(self.source, |idle| {
                let state = idle.progress();
                if !matches!(state, schedule::Progress::Quiescent) {
                    return Ok(turn::Resolved::Wait(idle, state));
                }
                Ok(match idle.sync()? {
                    poll::Commit::Drained => turn::Resolved::Done,
                    poll::Commit::Pending => turn::Resolved::Again,
                })
            }) {
                Ok(drained) => drained,
                Err(error) => break Err(error),
            };
            if drained {
                break Ok(output);
            }
            let tick = self.source.tick();
            polled = match self.state.next_root(&self.slot, tick) {
                Ok(ready) => ready.skip(),
                Err(error) => break Err(error.into()),
            };
        })
    }
}

impl<'d, T, C> Block<'d, T, C>
where
    T: scope::Roots<'d>,
{
    pub(crate) fn new(state: T, source: C) -> Result<Self, drive::Error> {
        let driver = state.driver_ref();
        let target = driver
            .targets::<routes::Wake>()
            .bind(indices::SlotIndex::ZERO, route::Epoch::INITIAL);
        let slot = driver.ready().make_slot(target.dispatch())?;
        Ok(Self {
            state,
            source,
            slot,
        })
    }
}

impl<'run, 'driver, 'app, 'd: 'app, D, S, C>
    Block<'d, scope::Scope<'run, 'driver, 'app, 'd, D, S>, C>
where
    D: executor::raw::App<'d>,
    C: clock::Time,
{
    pub(crate) fn run<R>(
        self,
        root: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> Result<R::Output, drive::Error>
    where
        R: root::Root<'d>,
    {
        let (output, ()) = self
            .drive(root, tick, |idle| {
                Ok(matches!(idle.sync()?, poll::Commit::Drained).then_some(()))
            })?
            .complete()?;
        Ok(output)
    }

    pub(crate) fn run_drain<R>(
        self,
        root: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> Result<ControlFlow<R::Output>, drive::Error>
    where
        R: root::Root<'d>,
    {
        Ok(
            match self.drive(root, tick, |idle| {
                Ok(matches!(idle.sync()?, poll::Commit::Drained).then_some(()))
            })? {
                Outcome::Complete(output, ()) | Outcome::Shutdown(Some(output)) => {
                    ControlFlow::Break(output)
                }
                Outcome::Shutdown(None) => ControlFlow::Continue(()),
            },
        )
    }

    pub(crate) fn run_fixed<R>(
        self,
        root: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
    ) -> Result<Fixed<'d, R::Output>, drive::Error>
    where
        R: root::Root<'d>,
    {
        let (output, now) = self
            .drive(root, tick, |idle| {
                let fixed = idle.is_fixed();
                let now = idle.now();
                let commit = idle.sync()?;
                Ok((fixed && matches!(commit, poll::Commit::Drained)).then_some(now))
            })?
            .complete()?;
        Ok(Fixed::new(output, now))
    }

    fn drive<R, P>(
        mut self,
        root: R,
        tick: impl Fn(C) -> Result<Sample, drive::Error>,
        complete: impl for<'turn> Fn(
            turn::Idle<'turn, 'd, modes::App<'turn, 'app, 'd, D, S>>,
        ) -> Result<Option<P>, drive::Error>,
    ) -> Result<Outcome<R::Output, P>, drive::Error>
    where
        R: root::Root<'d>,
    {
        let wake = self.slot.target();
        let (mut polled, output) = {
            let mut root = pin::pin!(root);
            let mut first = true;
            loop {
                let tick = tick(self.source)?;
                let ready = self.state.next_root(&self.slot, tick)?;
                let poll = first || ready.is_root();
                let (polled, output) = if poll {
                    ready.poll(root.as_mut(), wake)
                } else {
                    (ready.skip(), task::Poll::Pending)
                };
                first = false;
                match output {
                    task::Poll::Ready(output) => break (polled, output),
                    task::Poll::Pending => {
                        let parked = polled.park();
                        match parked.status() {
                            turn::Status::Active => {}
                            turn::Status::Shutdown => return Ok(Outcome::Shutdown(None)),
                        }
                        parked.resolve(self.source, |idle| {
                            let state = idle.progress();
                            Ok(turn::Resolved::Wait(idle, state))
                        })?;
                    }
                }
            }
        };
        loop {
            let parked = polled.park();
            match parked.status() {
                turn::Status::Active => {}
                turn::Status::Shutdown => return Ok(Outcome::Shutdown(Some(output))),
            }
            if let turn::Sync::Idle(idle) = parked.sync()?
                && let Some(proof) = complete(idle)?
            {
                return Ok(Outcome::Complete(output, proof));
            }
            let tick = tick(self.source)?;
            polled = self.state.next_root(&self.slot, tick)?.skip();
        }
    }
}
