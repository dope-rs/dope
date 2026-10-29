pub(crate) use accepted::Accepted;
use dope_sys::{
    io::reactor::{
        Full,
        complete::{Head as _, Inspect as _, Record as _},
        submit::Queue as _,
    },
    probe::{Cause, Outcome as _, Stopped},
    ring::{Access as _, Complete as _},
    table::token,
};
pub(crate) use listener::Listener;
pub(crate) use range::Range;
pub(crate) use recover::Recover;
pub(crate) use released::Released;

use crate::{
    Observed, Ring,
    reactor::{
        ring::{Checked, Identity, ProbeFault},
        sq,
    },
};

mod accepted;
mod listener;
mod range;
mod recover;
mod released;

pub struct Waiting<S, T: token::Codec> {
    pub(super) state: S,
    pub(super) probe: Identity<T>,
}

impl<S, T: token::Codec> Waiting<S, T> {
    pub(super) fn owner(failed: Stopped<Self, ProbeFault<T>>) -> Stopped<S, ProbeFault<T>> {
        failed.map(|waiting| waiting.state)
    }

    pub(super) fn push(
        ring: &mut Ring<T>,
        probe: Identity<T>,
        fill: impl FnOnce(sq::raw::Vacancy<'_, T>),
    ) -> Result<(), ProbeFault<T>> {
        let vacancy = match ring.sq().try_acquire() {
            Ok(vacancy) => vacancy,
            Err(Full) => {
                let submit = ring.sq().len();
                let complete = ring.completion().available();
                return Err(Cause::Residual {
                    probe,
                    submit,
                    complete,
                });
            }
        };
        fill(vacancy);
        Ok(())
    }

    pub(super) fn acknowledge(
        ring: &mut Ring<T>,
        probe: Identity<T>,
        reported: Observed<T>,
    ) -> Result<(), ProbeFault<T>> {
        let mut queue = ring.completion();
        let head = queue.front().ok_or(Cause::Missing(probe))?;
        let parts = head.parts();
        let observed = Observed::new(parts.token, parts.outcome, parts.flags);
        if observed != reported {
            return Err(Cause::Unexpected { probe, observed });
        }
        head.commit();
        Ok(())
    }

    pub(super) fn pop(ring: &mut Ring<T>, probe: Identity<T>) -> Result<Checked<T>, ProbeFault<T>> {
        let mut queue = ring.completion();
        let head = queue.front().ok_or(Cause::Missing(probe))?;
        let parts = head.parts();
        if parts.token != T::probe(probe) {
            let observed = Observed::new(parts.token, parts.outcome, parts.flags);
            return Err(Cause::Unexpected { probe, observed });
        }
        let done = Checked::new(parts, probe).map_err(Cause::Check)?;
        head.commit();
        Ok(done)
    }
}
