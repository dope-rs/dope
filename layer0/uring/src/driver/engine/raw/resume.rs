use dope_sys::{
    engine::{self, life, ring},
    probe::Stopped,
    ring::Source,
};

use crate::{Engine, Uring, driver::tuning::Progress};

impl<'a, C, R> engine::raw::Resume<'a, R> for Uring
where
    C: ring::Context + 'a,
    C::Tuning: Progress<C::Token>,
    R: life::Resume<Backend = Engine<C>> + 'a,
{
    unsafe fn try_resume(
        resolver: R,
    ) -> Result<R, Stopped<R, life::Error<<Self as Source>::Poll, R::Fault>>> {
        // SAFETY: the caller retains the exact terminal source continuously
        // through the next quiescence or destruction, including on failure.
        unsafe { Engine::try_resume(resolver) }
    }
}
