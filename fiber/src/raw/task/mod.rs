pub(crate) mod bridges;
pub(crate) mod queue;

use core::cell::Cell;
use core::marker::PhantomPinned;
use core::pin::Pin;
use core::ptr::NonNull;

use dope::driver::ready::{CompletionCallback, CompletionWaker, ReadyKey};
pub use dope::driver::ready::{CompletionRegistrar, CompletionRegistrarWithRegion};
use dope::{DriverContext, DriverRef};
use o3::cell::RegionToken;
use o3::collections::BatchSet;
use o3::marker::ThreadBound;
use pin_project::{pin_project, pinned_drop};
use queue::Queue;

use crate::raw::link::{PinnedLink, StableLinkSource};

struct BindingSource<T>(NonNull<T>);

// SAFETY: this private source is constructed only while installing or
// traversing a binding whose two endpoint drops revoke every stored link.
unsafe impl<T> StableLinkSource<T> for BindingSource<T> {
    fn pointer(self) -> NonNull<T> {
        self.0
    }
}

struct Node<'d> {
    binding: Cell<Option<NodeBinding<'d>>>,
    _pin: PhantomPinned,
    _thread: ThreadBound,
}

#[derive(Clone, Copy)]
struct NodeBinding<'d> {
    ready: PinnedLink<BatchSet>,
    index: usize,
    parent: WakeTarget<'d>,
}

impl<'d> Node<'d> {
    const fn new() -> Self {
        Self {
            binding: Cell::new(None),
            _pin: PhantomPinned,
            _thread: ThreadBound::NEW,
        }
    }

    fn install(
        self: Pin<&Self>,
        ready: PinnedLink<BatchSet>,
        index: usize,
        parent: WakeTarget<'d>,
    ) {
        debug_assert!(
            index < ready.get().capacity(),
            "dope-fiber: task index out of bounds"
        );
        debug_assert!(
            self.binding.get().is_none(),
            "dope-fiber: task node already bound"
        );
        self.binding.set(Some(NodeBinding {
            ready,
            index,
            parent,
        }));
    }

    fn unbind(self: Pin<&Self>) -> Option<usize> {
        let binding = self.binding.take()?;
        binding.ready.get().remove(binding.index);
        Some(binding.index)
    }

    fn is_bound(&self) -> bool {
        self.binding.get().is_some()
    }

    fn wake(self: Pin<&Self>) {
        let mut next = self.binding.get();
        loop {
            let Some(binding) = next else {
                return;
            };
            if !binding.ready.get().insert(binding.index) {
                return;
            }
            match binding.parent {
                WakeTarget::Node(parent) => next = parent.get().binding.get(),
                WakeTarget::Ready(root) => {
                    root.wake();
                    return;
                }
            }
        }
    }
}

#[pin_project(PinnedDrop)]
#[repr(C)]
pub(crate) struct TaskContext<'d, T: Copy = usize> {
    #[pin]
    node: Node<'d>,
    queue: Cell<Option<PinnedLink<Queue<T>>>>,
}

/// An owner proof for a task context retained by a queue binding.
/// # Safety
/// The context, supplied queue, and parent target stay pinned until unbound,
/// or an endpoint's Drop revokes the binding first.
pub(crate) unsafe trait StableTaskSource<'a, 'd, T: Copy> {
    fn context(self) -> Pin<&'a TaskContext<'d, T>>;
}

/// A queue implementation that may retain a ready-set link.
/// # Safety
/// Every returned link must name pinned storage covered by the binding's
/// teardown.
pub(crate) unsafe trait BindingQueue<T: Copy> {
    type Input;

    fn attach(self: Pin<&Self>, index: usize, input: Self::Input) -> usize;
    fn ready(&self) -> &BatchSet;

    fn ready_link(self: Pin<&Self>) -> PinnedLink<BatchSet> {
        PinnedLink::from_stable(BindingSource(NonNull::from(self.ready())))
    }

    fn recycle_link(self: Pin<&Self>) -> Option<PinnedLink<Queue<T>>> {
        None
    }
}

impl<'d, T: Copy> TaskContext<'d, T> {
    pub(crate) const fn new() -> Self {
        Self {
            node: Node::new(),
            queue: Cell::new(None),
        }
    }

    pub(crate) fn bind<'a, Q>(
        source: impl StableTaskSource<'a, 'd, T>,
        queue: Pin<&Q>,
        index: usize,
        input: Q::Input,
        parent: Waker<'d>,
    ) -> Waker<'d>
    where
        Q: BindingQueue<T>,
        'd: 'a,
        T: 'a,
    {
        let context = source.context();
        let node = context.project_ref().node;
        debug_assert!(!node.is_bound(), "task context already bound");
        let index = queue.attach(index, input);
        let ready = queue.ready_link();
        context.queue.set(queue.recycle_link());
        node.install(ready, index, parent.target);
        Waker::from_node(PinnedLink::from_stable(BindingSource(NonNull::from(
            node.get_ref(),
        ))))
    }

    pub(crate) fn unbind(self: Pin<&Self>) {
        let node = self.project_ref().node;
        let Some(index) = node.unbind() else {
            return;
        };
        if let Some(queue) = self.queue.replace(None) {
            queue.get().clear(index);
        }
    }

    pub(crate) fn is_bound(&self) -> bool {
        self.node.is_bound()
    }

    pub(crate) fn waker<'a>(source: impl StableTaskSource<'a, 'd, T>) -> Waker<'d>
    where
        'd: 'a,
        T: 'a,
    {
        let node = source.context().project_ref().node;
        debug_assert!(node.is_bound(), "task context not bound");
        Waker::from_node(PinnedLink::from_stable(BindingSource(NonNull::from(
            node.get_ref(),
        ))))
    }

    pub(crate) fn wake(self: Pin<&Self>) {
        let node = self.project_ref().node;
        node.wake();
    }
}

#[pinned_drop]
impl<T: Copy> PinnedDrop for TaskContext<'_, T> {
    fn drop(self: Pin<&mut Self>) {
        self.as_ref().unbind();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WakeTarget<'d> {
    Node(PinnedLink<Node<'d>>),
    Ready(RootWaker<'d>),
}

struct NodeCompletion<'d> {
    node: PinnedLink<Node<'d>>,
}

// SAFETY: NodeCompletion always pairs a Node pointer with wake_node. Bound
// nodes stay pinned, and registrar teardown makes stored handles unobservable
// before the node is unbound.
unsafe impl<'d> CompletionCallback<'d> for NodeCompletion<'d> {
    #[inline(always)]
    fn into_raw_parts(self) -> (NonNull<()>, unsafe fn(NonNull<()>)) {
        (self.node.pointer().cast(), wake_node)
    }
}

/// A local proof carrier for registrar inputs composed from multiple values.
#[repr(transparent)]
pub(crate) struct CompletionOwner<T>(pub(crate) T);

#[pin_project]
pub struct Context<'poll, 'd> {
    wake: Waker<'d>,
    driver: DriverContext<'poll, 'd>,
    #[pin]
    _pin: PhantomPinned,
}

impl<'poll, 'd> Context<'poll, 'd> {
    pub(crate) fn from_waker(wake: Waker<'d>, driver: DriverContext<'poll, 'd>) -> Self {
        Self {
            wake,
            driver,
            _pin: PhantomPinned,
        }
    }

    #[doc(hidden)]
    pub fn from_ready(
        reference: DriverRef<'d>,
        key: ReadyKey<'d>,
        driver: DriverContext<'poll, 'd>,
    ) -> Self {
        Self::from_waker(Waker::from_ready(reference, key), driver)
    }

    pub fn waker(&self) -> WakerRef<'_, 'd> {
        WakerRef { waker: &self.wake }
    }

    /// Delivers a retainable completion handle directly to an owner that
    /// proves its teardown.
    #[doc(hidden)]
    #[inline(always)]
    pub fn register_completion<R>(self: Pin<&Self>, registrar: R) -> R::Output
    where
        R: CompletionRegistrar<'d>,
    {
        let this = self.get_ref();
        this.wake
            .register_completion(this.driver.region_token_ref(), registrar)
    }

    /// Delivers a retainable completion handle and mutable region access to an
    /// owner that proves both lifetimes.
    #[doc(hidden)]
    #[inline(always)]
    pub fn register_completion_with_region<R>(mut self: Pin<&mut Self>, registrar: R) -> R::Output
    where
        R: CompletionRegistrarWithRegion<'d>,
    {
        let this = self.as_mut().project();
        this.wake
            .register_completion_with_region(this.driver.region_token(), registrar)
    }

    pub fn driver_access(self: Pin<&mut Self>) -> DriverContext<'_, 'd> {
        self.project().driver.reborrow()
    }

    pub fn region_token(self: Pin<&mut Self>) -> &mut RegionToken<'d> {
        self.project().driver.region_token()
    }

    pub(crate) fn parent_waker(&self) -> Waker<'d> {
        self.wake
    }

    pub fn wake(&self) {
        self.wake.wake();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Waker<'d> {
    target: WakeTarget<'d>,
}

impl<'d> Waker<'d> {
    fn from_node(node: PinnedLink<Node<'d>>) -> Self {
        Self {
            target: WakeTarget::Node(node),
        }
    }

    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self {
            target: WakeTarget::Ready(RootWaker::from_ready(driver, key)),
        }
    }

    #[inline(always)]
    fn register_completion<R>(self, region: &RegionToken<'d>, registrar: R) -> R::Output
    where
        R: CompletionRegistrar<'d>,
    {
        match self.target {
            WakeTarget::Node(node) => {
                CompletionWaker::register_callback(NodeCompletion { node }, region, registrar)
            }
            WakeTarget::Ready(root) => registrar.register(root.completion()),
        }
    }

    #[inline(always)]
    fn register_completion_with_region<R>(
        self,
        region: &mut RegionToken<'d>,
        registrar: R,
    ) -> R::Output
    where
        R: CompletionRegistrarWithRegion<'d>,
    {
        match self.target {
            WakeTarget::Node(node) => CompletionWaker::register_callback_with_region(
                NodeCompletion { node },
                region,
                registrar,
            ),
            WakeTarget::Ready(root) => registrar.register(root.completion(), region),
        }
    }

    pub fn wake(self) {
        match self.target {
            WakeTarget::Node(node) => node.get().wake(),
            WakeTarget::Ready(root) => root.wake(),
        }
    }
}

pub struct WakerRef<'a, 'd> {
    waker: &'a Waker<'d>,
}

impl WakerRef<'_, '_> {
    pub fn wake(&self) {
        self.waker.wake();
    }
}

/// A driver-owned wake target that may outlive the object which exposed it.
///
/// Ready keys are generational, so waking after their slot has been released
/// is a safe no-op. Unlike [`Waker`], this type can never name a task node.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RootWaker<'d> {
    driver: DriverRef<'d>,
    key: ReadyKey<'d>,
}

impl<'d> RootWaker<'d> {
    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self { driver, key }
    }

    pub fn completion(self) -> CompletionWaker<'d> {
        CompletionWaker::from_ready(self.driver, self.key)
    }

    pub fn wake(self) {
        self.driver.activate_ready(self.key);
    }
}

unsafe fn wake_node(target: NonNull<()>) {
    // SAFETY: CompletionCallback invokes this while its pinned node is live.
    let node = unsafe { PinnedLink::from_raw(target.cast::<Node<'static>>()) };
    node.get().wake();
}

impl<'d> From<RootWaker<'d>> for Waker<'d> {
    fn from(root: RootWaker<'d>) -> Self {
        Self {
            target: WakeTarget::Ready(root),
        }
    }
}
