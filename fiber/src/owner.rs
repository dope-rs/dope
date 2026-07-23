use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;

use dope::DriverRef;
use o3::buffer::Shared;
use pin_project::pin_project;

use crate::{Context, Fiber};

/// Builds a fiber from an immutable split-byte view without allowing the view
/// to escape its owner.
///
/// The method is generic over the request lifetime. A safe implementation
/// therefore cannot store the view in longer-lived state supplied through
/// [`Self::Input`], [`Self::State`], or [`Self::Context`]. The owned output and
/// error types close the remaining ordinary escape paths.
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

/// A zero-sized proof of a generative driver lifetime.
///
/// The private invariant marker prevents safe fabrication. Driver scopes are
/// introduced by higher-ranked runtime entry points, so their lifetime cannot
/// be selected as `'static` or escape the runtime closure.
#[derive(Clone, Copy)]
pub struct FiberScope<'d> {
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> FiberScope<'d> {
    pub fn from_driver(_driver: DriverRef<'d>) -> Self {
        Self {
            driver: PhantomData,
        }
    }
}

/// A fiber and the immutable storage backing its borrows.
///
/// The fiber field is deliberately declared first so it is always dropped
/// before the owner. Construction from an owner is available only for owner
/// types whose backing allocation remains stable when the handle is moved.
#[pin_project]
pub struct OwnerFiber<F, O> {
    #[pin]
    fiber: F,
    owner: O,
}

impl<F, O> OwnerFiber<F, O> {
    pub fn from_parts(fiber: F, owner: O) -> Self {
        Self { fiber, owner }
    }
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

/// Immutable request-like storage split into a head and body view.
///
/// `Shared` keeps its allocation stable across handle moves. If `body` is
/// present it backs the body view; otherwise the tail of `request` does.
pub struct SplitBytes {
    request: Shared,
    body: Option<Shared>,
    split: usize,
}

impl SplitBytes {
    pub fn new(request: Shared, body: Option<Shared>, split: usize) -> Self {
        debug_assert!(split <= request.len());
        Self {
            request,
            body,
            split,
        }
    }

    pub fn view(&self) -> SplitView<'_> {
        let head = &self.request.as_slice()[..self.split];
        let body = match &self.body {
            Some(body) => body.as_slice(),
            None => &self.request.as_slice()[self.split..],
        };
        SplitView { head, body }
    }

    /// Extends a view to the driver domain owned by an [`OwnerFiber`].
    ///
    /// # Safety
    /// The returned view must be retained only by a value dropped before this
    /// owner. It must not escape through that value's output.
    unsafe fn domain_view<'d>(&self) -> SplitView<'d> {
        let SplitView { head, body } = self.view();
        // SAFETY: Shared allocations are immutable and stable across moves.
        // OwnerFiber keeps both handles alive until after its fiber is dropped.
        unsafe {
            SplitView {
                head: &*(head as *const [u8]),
                body: &*(body as *const [u8]),
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct SplitView<'a> {
    head: &'a [u8],
    body: &'a [u8],
}

impl<'a> SplitView<'a> {
    pub fn head(self) -> &'a [u8] {
        self.head
    }

    pub fn body(self) -> &'a [u8] {
        self.body
    }

    pub fn into_parts(self) -> (&'a [u8], &'a [u8]) {
        (self.head, self.body)
    }
}

impl<F> OwnerFiber<F, SplitBytes> {
    /// Builds a fiber borrowing immutable views of its owned bytes.
    ///
    /// # Safety
    ///
    /// `build` must not let the supplied view escape except inside the returned
    /// fiber. In particular, it must not store the view through interior
    /// mutability or another side channel. The returned fiber is dropped before
    /// its owner, while owned output and error types close the two ordinary
    /// return paths.
    pub unsafe fn try_from_split<'d, E>(
        owner: SplitBytes,
        _scope: FiberScope<'d>,
        build: impl FnOnce(SplitView<'d>) -> Result<F, E>,
    ) -> Result<Self, E>
    where
        F: Fiber<'d>,
        F::Output: 'static,
        E: 'static,
    {
        // SAFETY: the caller proves that the view escapes only in `F`.
        let view = unsafe { owner.domain_view() };
        let fiber = build(view)?;
        Ok(Self { fiber, owner })
    }
}

/// Builds an owner-backed task through a lifetime-generic safe factory.
///
/// The returned fiber is layout-equivalent to an [`OwnerFiber`] containing
/// the factory's fiber and the byte owner. Construction performs no allocation
/// and introduces no polling state machine.
pub fn try_from_split_task<'req, 'd, T>(
    owner: SplitBytes,
    input: T::Input,
    state: &'req T::State,
    context: &'req T::Context,
) -> Result<OwnerFiber<impl Fiber<'d, Output = T::Output> + 'req, SplitBytes>, T::Error>
where
    T: SplitTask<'d>,
    'd: 'req,
    T::State: 'req,
    T::Context: 'req,
{
    // SAFETY: `SplitTask::build` is generic over `'req`, so a safe
    // implementation cannot move this view into state with a longer lifetime.
    // Its output and error are owned, and OwnerFiber drops the returned fiber
    // before the allocation handles backing the view.
    let view = unsafe { owner.domain_view() };
    let fiber = T::build(view, input, state, context)?;
    Ok(OwnerFiber { fiber, owner })
}
