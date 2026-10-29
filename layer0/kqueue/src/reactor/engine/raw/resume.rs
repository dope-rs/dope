use dope_sys::{engine, engine::life, io::reactor::ready, probe::Stopped};

use crate::{Kqueue, reactor::Engine};

impl<'a, C, R> engine::raw::Resume<'a, R> for Kqueue
where
    C: ready::Driver<Context: ready::raw::Decode + 'a>,
    <Engine<C> as ready::Backend<C>>::Failure: Copy + Eq,
    R: life::Resume<Backend = Engine<C>> + 'a,
{
    /// The shutdown filter remains installed while application tables quiesce.
    unsafe fn try_resume(
        resolver: R,
    ) -> Result<R, Stopped<R, life::Error<<Engine<C> as engine::raw::Poll>::Error, R::Fault>>> {
        <Self as life::Finish<'a, R>>::try_finish(resolver).map(life::Quiesced::into_inner)
    }
}
