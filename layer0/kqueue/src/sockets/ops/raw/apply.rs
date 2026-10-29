use dope_sys::io::reactor::ready::{Backend as _, store::State as _};

use crate::sockets::ops::{Context, Engine, Operation, change, device, ready, tuning};

impl<C, H, const N: usize> tuning::raw::Apply<Engine<C>, H, N> for Operation<H>
where
    C: ready::Driver,
    H: device::raw::Host<
            Engine<C>,
            N,
            Slot = <<C::Context as Context>::Slots as change::raw::Slots>::Physical,
        >,
{
    unsafe fn try_apply(self) -> Result<H::Output, H::Error> {
        self.host.direct(|backend, fd, slot, options| {
            match backend.state().set(slot, options.map(Into::into)) {
                Ok(()) => Ok(fd),
                Err(error) => Err((fd, error)),
            }
        })
    }
}
