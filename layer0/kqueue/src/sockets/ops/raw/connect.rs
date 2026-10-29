use crate::sockets::ops::{
    Command, Engine, Handle, Key, Kqueue, Operation, Retained, Slot, admit, device, request,
    submit, tuning,
};

impl<'r, C, H, const N: usize> tuning::raw::Connect<Engine<C>, H, Command<Retained<'r, Slot<C>>>, N>
    for Operation<H>
where
    C: submit::Context<Handle = Handle> + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Inputs: request::Context<Target = Slot<C>, Outbound = C::Outbound>,
    H: device::raw::Connect<Engine<C>, Command<Retained<'r, Slot<C>>>, N, Slot = Slot<C>>,
    H::Reserve: device::Reserve<Key = Key<C>>,
{
    unsafe fn try_connect(self) -> Result<H::Output, H::Error> {
        use device::{Failure, Reserve as _};
        self.host.direct(|backend, fd, options, reserve, last| {
            let vacant = match reserve.try_vacancy() {
                Ok(vacant) => vacant,
                Err(error) => return Err(Failure::Reserve(tuning::Error::Reserve(fd, error))),
            };
            let permit = match admit::Admit::<C>::try_admit(backend) {
                Ok(permit) => permit,
                Err(error) => return Err(Failure::Reserve(tuning::Error::Full(fd, error))),
            };
            if let Some((slot, options)) = options
                && let Err(error) = admit::Options::<C>::set(&permit, slot, Some(options.into()))
            {
                return Err(Failure::Option(fd, error));
            }
            let (key, target) = H::Reserve::reserve(vacant);
            // SAFETY: the host retains this exact terminal's captured address
            // through completion or quiescence, as required by Connect.
            unsafe { admit::raw::Retain::<C>::retain(permit, last, key) };
            Ok((fd, target))
        })
    }
}
