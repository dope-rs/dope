use dope_sys::{
    engine::shutdown::{self, Cleanup as _, Shutdown},
    io::{Close, Retry as _},
};
use o3::buffer::storage::provided;

use crate::{
    Engine,
    driver::{
        self,
        life::{domain, native::create},
        retained::flight,
        storage::resident::Arena,
    },
};

pub(crate) struct Runtime {
    pub(crate) backend: Engine,
    pub(crate) resident: Arena,
    pub(crate) flights: flight::Arena,
    pub(crate) shared: provided::Arena<driver::Buffer, driver::Shared, ()>,
}

pub(crate) struct Closing<B: Shutdown = Engine>(Option<B::State>);

impl Runtime {
    pub(in crate::driver) fn new(
        backend: Engine,
        resident: Arena,
        shared: provided::Arena<driver::Buffer, driver::Shared, ()>,
    ) -> Self {
        Self {
            backend,
            flights: flight::Arena::new(),
            resident,
            shared,
        }
    }
    pub(in crate::driver) fn close(self) -> Closing {
        let Self {
            backend, shared, ..
        } = self;
        let buffer = shared.into_context().into_buffer();
        Closing(backend.try_shutdown(buffer).err())
    }
}

impl<B: Shutdown> Closing<B>
where
    create::Kernel: From<<B::State as shutdown::Cleanup>::Error>,
{
    pub(super) const fn closed() -> Self {
        Self(None)
    }

    pub(super) fn failure(&self) -> Option<domain::Unmap> {
        self.0
            .as_ref()
            .map(|state| domain::Unmap::native(state.failure()))
    }

    /// Retries the retained owner, returning the exact failure of the attempt it rejects.
    pub(super) fn try_close(
        &mut self,
        mut report: impl FnMut(domain::Unmap),
    ) -> Result<(), (&mut Self, domain::Unmap)> {
        let Some(state) = self.0.take() else {
            return Ok(());
        };
        let (owner, error) = state.recover().into_parts();
        let error: shutdown::Failure<<B::State as shutdown::Cleanup>::Error> = error.into();
        report(domain::Unmap::native(error));
        match owner.try_close() {
            Ok(()) => Ok(()),
            Err(state) => {
                let failure = domain::Unmap::native(state.failure());
                self.0 = Some(state);
                Err((self, failure))
            }
        }
    }
}
