use crate::sockets::ops::{Context, Engine, Index, Key, Operation, Table, device, sq, tuning};

impl<C, H, const N: usize> tuning::raw::Apply<Engine<C>, H, N> for Operation<H>
where
    C: Context<Tuning = Table<Key<C>, N>>,
    H: device::raw::Host<Engine<C>, N, Slot = sq::Fixed, Index = Index<C>, Permit = ()>,
    H::Reserve: device::Reserve<Key = Key<C>>,
{
    unsafe fn try_apply(self) -> Result<H::Output, H::Error> {
        use device::{Failure, Finish, Options as _, Reserve as _};

        use crate::driver::tuning::raw::Vacant;
        self.host.apply(|admitted, options, reserve| {
            let Some(options) = options else {
                return Ok(Finish::Applied(admitted.into_file()));
            };
            let (count, options) = options.split();
            let (queue, vacant) = Vacant::<C::Token, _, N>::admit(admitted);
            // SAFETY: Apply retains the exact nonzero count and state token;
            // the descriptor and pinned state belong to this driver. The table
            // survives completion; native capacity precedes the host flight.
            let (fd, target) =
                unsafe { vacant.try_submit(queue, count, options, None, || reserve.try_reserve()) }
                    .map_err(Failure::Reserve)?;
            Ok(Finish::Pending(fd, target, ()))
        })
    }
}
