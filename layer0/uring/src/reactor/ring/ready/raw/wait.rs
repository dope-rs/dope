use dope_sys::{
    io::{
        reactor::{
            complete::flags::{Flow, Meta as _},
            submit,
            submit::{Control as _, Ready as _},
        },
        socket::option::Kind,
    },
    kernel,
    probe::{self, Cause, Outcome as _, State, Stopped},
    ring::{Access as _, enter::Enter as _},
    table::token,
};

use crate::{
    Ring, Uring, error,
    reactor::{
        op::sockets,
        ring::{Checked, Identity, ProbeFault, wait::Waiting},
        sq,
    },
};

impl<S, T: token::Codec> probe::raw::Wait<S> for Waiting<S, T> {
    type Ring = Ring<T>;
    type Owned = sq::Owned<T>;
    type Target = sq::Fixed;
    type Probe = Identity<T>;
    type Outcome = Checked<T>;
    type Error = kernel::Errno<error::Enter>;

    fn submit(
        mut state: S,
        ring: impl for<'a> FnOnce(&'a mut S) -> &'a mut Ring<T>,
        entry: &sq::Owned<T>,
        probe: Identity<T>,
    ) -> Result<Self, Stopped<S, ProbeFault<T>>> {
        let pushed = Self::push(ring(&mut state), probe, |vacancy| {
            submit::raw::Vacancy::prepare(vacancy, entry).push();
        });
        let (state, ()) = Stopped::attach(state, pushed)?;
        Ok(Self { state, probe })
    }

    fn resume(
        stopped: Stopped<Self, ProbeFault<T>>,
        ring: impl for<'a> FnOnce(&'a mut S) -> &'a mut Ring<T>,
        mut report: impl FnMut(ProbeFault<T>),
    ) -> Result<Self, Stopped<Self, ProbeFault<T>>> {
        let Stopped { mut state, cause } = stopped;
        let unread = cause.unread();
        report(cause);
        let result = match unread {
            Some(observed) => Self::acknowledge(ring(&mut state.state), state.probe, observed),
            None => Ok(()),
        };
        Stopped::attach(state, result).map(|(state, ())| state)
    }

    fn finish(
        mut self,
        ring: impl for<'a> Fn(&'a mut S) -> &'a mut Ring<T>,
    ) -> Result<(S, Checked<T>), Stopped<S, ProbeFault<T>>> {
        let result = ring(&mut self.state)
            .enter()
            .try_wait(None)
            .map_err(|error| Cause::Enter {
                probe: self.probe,
                error,
            });
        let (mut waiting, ()) = Stopped::attach(self, result).map_err(Self::owner)?;
        let result = Self::pop(ring(&mut waiting.state), waiting.probe);
        Stopped::attach(waiting.state, result)
    }

    fn next(
        mut self,
        ring: impl for<'a> FnOnce(&'a mut S) -> &'a mut Ring<T>,
    ) -> Result<(State<S, Self>, Checked<T>), Stopped<State<S, Self>, ProbeFault<T>>> {
        let queue = ring(&mut self.state);
        let result = queue.enter().try_wait(None).map_err(|error| Cause::Enter {
            probe: self.probe,
            error,
        });
        let result = result.and_then(|()| Self::pop(queue, self.probe));
        let (waiting, done) = Stopped::attach(self, result).map_err(Stopped::active)?;
        let state = match done.meta().flow() {
            Flow::Last => State::Idle(waiting.state),
            Flow::More => State::Active(waiting),
        };
        Ok((state, done))
    }

    fn cancel(
        self,
        ring: impl for<'a> Fn(&'a mut S) -> &'a mut Ring<T>,
    ) -> Result<S, Stopped<State<S, Self>, ProbeFault<T>>> {
        let probe = self.probe;
        let cancel = Uring::cancel(T::probe(probe), T::cancel_probe(probe));
        let waiting = Waiting::submit(self, |waiting| ring(&mut waiting.state), &cancel, probe)
            .map_err(Stopped::active)?;
        let (state, done) = waiting.state.next(ring)?;
        match state {
            State::Idle(state) => {
                let (state, ()) = Stopped::attach(state, done.cancel().map_err(Cause::Check))
                    .map_err(Stopped::idle)?;
                Ok(state)
            }
            State::Active(state) => Err(Stopped {
                state: State::Active(state),
                cause: Cause::Check(done.nonterminal()),
            }),
        }
    }

    unsafe fn option(
        mut state: S,
        access: impl for<'a> FnOnce(&'a mut S) -> (&'a mut Ring<T>, sq::Fixed, &'a libc::c_int),
        probe: Identity<T>,
    ) -> Result<Self, Stopped<S, ProbeFault<T>>> {
        let (ring, slot, value) = access(&mut state);
        let entry = sockets::set_option(slot, Kind::KeepAlive, value).user_data(T::probe(probe));
        let pushed = Self::push(ring, probe, |vacancy| {
            // SAFETY: the caller proves the scalar remains stable in this retained
            // owner until terminal completion or ring quiescence.
            unsafe { submit::raw::Vacancy::borrow(vacancy, &entry) }.push();
        });
        let (state, ()) = Stopped::attach(state, pushed)?;
        Ok(Self { state, probe })
    }

    unsafe fn inspect<E>(
        mut self,
        check: impl FnOnce(&mut S) -> Result<(), E>,
    ) -> Result<Self, Stopped<Self, E>> {
        let result = check(&mut self.state);
        Stopped::attach(self, result).map(|(state, ())| state)
    }
}
