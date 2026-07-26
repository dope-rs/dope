use core::pin::Pin;
use core::task::Poll;

use o3::buffer::Shared;
use pin_project::pin_project;

use crate::owner::{SplitTask, SplitView};
use crate::{Context, Fiber};

pub(crate) struct SplitStorage {
    request: Shared,
    body: Option<Shared>,
    split: usize,
}

impl SplitStorage {
    pub(crate) fn new(request: Shared, body: Option<Shared>, split: usize) -> Self {
        Self {
            request,
            body,
            split,
        }
    }

    pub(crate) fn try_into_task<'req, 'd, T>(
        self,
        input: T::Input,
        state: &'req T::State,
        context: &'req T::Context,
    ) -> Result<OwnerFiber<impl Fiber<'d, Output = T::Output> + 'req, Self>, T::Error>
    where
        T: SplitTask<'d>,
        'd: 'req,
        T::State: 'req,
        T::Context: 'req,
    {
        let head = &self.request.as_slice()[..self.split];
        let body = match &self.body {
            Some(body) => body.as_slice(),
            None => &self.request.as_slice()[self.split..],
        };
        // SAFETY: Shared allocations are immutable and stable across handle
        // moves. OwnerFiber places `self` after the returned fiber, so the
        // fiber is dropped before both backing handles.
        // SplitTask's higher-ranked build method and owned output/error types
        // prevent a safe implementation from leaking either extended view.
        let view =
            unsafe { SplitView::from_parts(&*(head as *const [u8]), &*(body as *const [u8])) };
        let fiber = T::build(view, input, state, context)?;
        Ok(OwnerFiber {
            fiber,
            _owner: self,
        })
    }
}

/// A fiber followed by its backing storage, in guaranteed drop order.
#[pin_project]
pub(crate) struct OwnerFiber<F, O> {
    #[pin]
    fiber: F,
    _owner: O,
}

impl<'d, F, O> Fiber<'d> for OwnerFiber<F, O>
where
    F: Fiber<'d>,
    F::Output: 'static,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Fiber::poll(self.project().fiber, context)
    }
}
