mod core;
mod domain;
mod sealed;

use ::core::{pin, task};
pub use domain::Domain;
use o3::collections::{self, fixed::array};
pub(crate) use sealed::{Index, PinnedArray, Queue};

use crate::{abi, context};

enum DomainTag {}

const POLL_BUDGET: usize = 32;

struct Task<'a, 'd> {
    context: pin::Pin<&'a crate::raw::Binding<'d>>,
}

impl<'a, 'd> Task<'a, 'd> {
    const fn new(context: pin::Pin<&'a crate::raw::Binding<'d>>) -> Self {
        Self { context }
    }
}

#[pin_project::pin_project(PinnedDrop)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Batch<'domain, 'd, F, O, const N: usize> {
    #[pin]
    core: core::Core<'d, F, O, N>,
    domain: &'domain mut Domain<'d, N>,
}

impl<'domain, 'd, F, O, const N: usize> Batch<'domain, 'd, F, O, N> {
    /// Builds an empty fixed-capacity batch.
    /// Fiber slots and bindings stay inline. Only a ready bitmap larger than
    /// its one-word representation requires allocation.
    pub fn try_empty(
        domain: &'domain mut Domain<'d, N>,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            core: core::Core::try_empty()?,
            domain,
        })
    }

    /// Builds a batch containing every fiber in array order.
    /// Ready storage is reserved before the fibers enter their pinned slots,
    /// so allocation failure drops the input array without partially binding it.
    pub fn try_from_array(
        domain: &'domain mut Domain<'d, N>,
        fibers: [F; N],
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            core: core::Core::try_from_array(fibers)?,
            domain,
        })
    }

    pub fn try_push(&mut self, fiber: F) -> Result<(), F> {
        self.core.try_push(fiber)
    }
}

impl<'domain, 'd, F, O, const N: usize> abi::Fiber<'d> for Batch<'domain, 'd, F, O, N>
where
    F: abi::Fiber<'d, Output = O>,
{
    type Output = array::IntoIter<O, N>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use ::core::task::Poll;
        let (this, mut context) = call.into_parts();
        let mut this = this.project();

        match this.core.as_mut().drive(context.as_mut(), this.domain) {
            Poll::Ready(()) => Poll::Ready(this.core.take_output()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pin_project::pinned_drop]
impl<F, O, const N: usize> PinnedDrop for Batch<'_, '_, F, O, N> {
    fn drop(self: pin::Pin<&mut Self>) {
        let this = self.project();
        this.core.cancel(this.domain);
    }
}
