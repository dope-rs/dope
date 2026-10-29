use crate::{
    Released, RemoveSlot,
    driver::{
        life::native::create,
        result::{IoOperation, Kernel},
        storage,
        storage::cleanup,
    },
};

type Primary = ((IoOperation, Kernel), RemoveSlot);

/// A failed fixed-file removal whose backend resources still require cleanup.
pub struct Release(cleanup::State<Released, Primary>);

impl Release {
    pub(in crate::driver) fn new<S: Into<Released>>(state: S) -> Self {
        Self(cleanup::State::new(state))
    }

    /// Retries in place while preserving the exact failed removal and native owner.
    pub fn try_close(
        &mut self,
        report: impl FnMut(storage::Mapping),
    ) -> Result<storage::Removal, storage::Mapping> {
        self.0.try_close(report, storage::Removal::native, |error| {
            create::Mapping::from(error).0
        })
    }

    pub fn primary(&self) -> storage::Removal {
        self.0.primary(storage::Removal::native)
    }

    /// No pending mapping failure remains after terminal cleanup.
    pub fn cleanup(&self) -> Option<storage::Mapping> {
        self.0.failure(|error| create::Mapping::from(error).0)
    }
}
