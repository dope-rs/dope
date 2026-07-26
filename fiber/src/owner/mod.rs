use o3::buffer::Shared;

use crate::Fiber;
use crate::raw::owners::SplitStorage;

/// Builds a fiber from an immutable split-byte view.
///
/// Its higher-ranked lifetime prevents a safe implementation from leaking the view.
pub trait SplitTask<'d> {
    type Input;
    type State: ?Sized;
    type Context: ?Sized;
    type Output: 'static;
    type Error: 'static;

    fn build<'req>(
        view: SplitView<'req>,
        input: Self::Input,
        state: &'req Self::State,
        context: &'req Self::Context,
    ) -> Result<impl Fiber<'d, Output = Self::Output> + 'req, Self::Error>
    where
        'd: 'req,
        Self::State: 'req,
        Self::Context: 'req;
}

/// Immutable bytes split into head and body views over stable `Shared` storage.
pub struct SplitBytes {
    storage: SplitStorage,
}

impl SplitBytes {
    pub fn new(request: Shared, body: Option<Shared>, split: usize) -> Self {
        debug_assert!(split <= request.len());
        Self {
            storage: SplitStorage::new(request, body, split),
        }
    }

    /// Builds a task borrowing this storage without exposing the self-reference.
    ///
    /// Construction performs no allocation and introduces no polling state.
    pub fn try_into_task<'req, 'd, T>(
        self,
        input: T::Input,
        state: &'req T::State,
        context: &'req T::Context,
    ) -> Result<impl Fiber<'d, Output = T::Output> + 'req, T::Error>
    where
        T: SplitTask<'d>,
        'd: 'req,
        T::State: 'req,
        T::Context: 'req,
    {
        self.storage.try_into_task::<T>(input, state, context)
    }
}

#[derive(Clone, Copy)]
pub struct SplitView<'a> {
    head: &'a [u8],
    body: &'a [u8],
}

impl<'a> SplitView<'a> {
    pub(crate) fn from_parts(head: &'a [u8], body: &'a [u8]) -> Self {
        Self { head, body }
    }

    pub fn into_parts(self) -> (&'a [u8], &'a [u8]) {
        (self.head, self.body)
    }
}
