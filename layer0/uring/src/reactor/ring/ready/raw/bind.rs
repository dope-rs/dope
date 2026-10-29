use dope_sys::{
    memory::buffer::provided::Context as _,
    ring::create::{Build as _, Retain as _},
};

use crate::reactor::ring::{
    Ready,
    ready::{Error, Ring, buffers, memory, ready, ring, token},
};

impl<T, L, P> ready::raw::Bind<P> for Ready<T, L>
where
    T: token::Codec,
    L: memory::raw::Canary,
    P: Copy + From<crate::Primary> + From<memory::Error>,
{
    type Cleanup = ring::Resources<T, L, P>;

    unsafe fn bind(
        ring: Ring<T>,
        layout: L,
    ) -> Result<(Self, buffers::raw::Context<L>), Result<P, Self::Cleanup>> {
        let context = match buffers::raw::Context::new(layout) {
            Ok(context) => context,
            Err(state) => return Err(Self::Cleanup::open(ring, state).into_result()),
        };
        // SAFETY: the caller retains this exact Context beside success. Every
        // failed registration below moves it into its complete cleanup owner.
        let error = match unsafe { <Self as ready::raw::Create>::new(ring, &context) } {
            Ok(ring) => return Ok((ring, context)),
            Err(Error::Open { ring, error }) => Self::Cleanup::open(ring, error),
            Err(Error::Registration { error, producer }) => {
                Self::Cleanup::registration(error, producer)
            }
            Err(Error::Provided {
                error,
                ring,
                producer,
            }) => Self::Cleanup::provided(error, ring, producer),
        };
        Err(error.close(context))
    }
}
