use crate::sockets::ops::{
    Context, Engine, Index, Key, Operation, Table, Uring, admit, device, sq, tuning,
};

impl<'r, C, H, const N: usize> tuning::raw::Connect<Engine<C>, H, sq::Borrowed<'r>, N>
    for Operation<H>
where
    C: Context<Tuning = Table<Key<C>, N>> + admit::Context<Native = Uring, Key = Key<C>>,
    H: device::raw::Connect<Engine<C>, sq::Borrowed<'r>, N, Slot = sq::Fixed, Index = Index<C>>,
    H::Reserve: device::Reserve<Key = Key<C>>,
{
    unsafe fn try_connect(self) -> Result<H::Output, H::Error> {
        use device::{Failure, Options as _, Reserve as _};

        use crate::driver::tuning::raw::Vacant;
        self.host.connect(
            |backend, fd, reserve, last| {
                let vacant = match reserve.try_vacancy() {
                    Ok(vacant) => vacant,
                    Err(error) => {
                        return Err(Failure::Reserve(tuning::Error::Reserve(fd, error)));
                    }
                };
                let permit = match admit::Admit::<C>::try_admit(backend) {
                    Ok(permit) => permit,
                    Err(error) => return Err(Failure::Reserve(tuning::Error::Full(fd, error))),
                };
                let (key, target) = H::Reserve::reserve(vacant);
                // SAFETY: Connect retains this terminal's exact captured address
                // through completion or queue quiescence.
                unsafe { admit::raw::Retain::<C>::retain(permit, last, key) };
                Ok((fd, target))
            },
            |admitted, options, reserve, last| {
                let (count, options) = options.split();
                let (queue, vacant) = Vacant::<C::Token, _, N>::admit(admitted);
                // SAFETY: the host retains the exact options, option count
                // and captured address for this descriptor's driver. Its pinned
                // table survives the terminal; native capacity precedes reservation.
                unsafe {
                    vacant.try_submit(queue, count, options, Some(last), || reserve.try_reserve())
                }
                .map_err(Failure::Reserve)
            },
        )
    }
}
