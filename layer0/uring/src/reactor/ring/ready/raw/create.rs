use dope_sys::{
    memory,
    memory::buffer::provided::Producer as _,
    ring::{Access as _, Owner as _},
};

use crate::reactor::ring::{
    Ready,
    ready::{Error, Failure, GROUP, Producer, Ring, buffers, complete, ready, register},
};

impl<T: complete::raw::Token, L: memory::raw::Receive> ready::raw::Create for Ready<T, L> {
    unsafe fn new(
        ring: Ring<T>,
        context: &buffers::raw::Context<L>,
    ) -> Result<Self, Failure<T, L>> {
        let producer = match Producer::new(context) {
            Ok(producer) => producer,
            Err(error) => return Err(Error::Open { ring, error }),
        };
        let (address, entries) = producer.registration();
        // SAFETY: the producer owns these stable entries and the caller keeps
        // their exact Context live until the ring and all returned owners close.
        let buffer = unsafe { register::raw::Buffer::<_, GROUP>::new(producer, address, entries) };
        let mut ring = match ring.try_register() {
            Ok(ring) => ring,
            Err(error) => {
                return Err(Error::Registration {
                    error,
                    producer: buffer.into_storage(),
                });
            }
        };
        let error = match ring
            .registry()
            .buffer(buffer.address, buffer.entries, GROUP)
        {
            Ok(()) => {
                return Ok(Self {
                    owner: buffer.bind(ring),
                });
            }
            Err(error) => error,
        };
        Err(Error::Provided {
            error,
            ring,
            producer: buffer.into_storage(),
        })
    }
}
