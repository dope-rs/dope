pub(crate) mod bridges;
pub mod queue;

use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::transmute;
use core::pin::Pin;
use core::ptr::NonNull;

use dope::driver::ready::{CompletionCallback, CompletionWaker, ReadyKey};
pub use dope::driver::ready::{CompletionRegistrar, CompletionRegistrarWithRegion};
use dope::{DriverContext, DriverRef};
use o3::cell::RegionToken;
use o3::collections::BatchSet;
use o3::marker::ThreadBound;

use crate::raw::link::{PinnedLink, StableLinkSource};
use pin_project::{pin_project, pinned_drop};
use queue::TaskQueue;

struct BindingSource<T>(NonNull<T>);

// SAFETY: this private source is constructed only while installing or
// traversing a binding whose two endpoint drops revoke every stored link.
unsafe impl<T> StableLinkSource<T> for BindingSource<T> {
    fn pointer(self) -> NonNull<T> {
        self.0
    }
}

struct Node {
    binding: Cell<Option<NodeBinding>>,
    _pin: PhantomPinned,
    _thread: ThreadBound,
}

#[derive(Clone, Copy)]
struct NodeBinding {
    ready: PinnedLink<BatchSet>,
    index: usize,
    parent: WakeTarget<'static>,
}

impl Node {
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
        parent: WakeTarget<'static>,
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

    fn index(&self) -> Option<usize> {
        self.binding.get().map(|binding| binding.index)
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
pub(crate) struct TaskContext<T: Copy = usize> {
    #[pin]
    node: Node,
    queue: Cell<Option<PinnedLink<TaskQueue<T>>>>,
}

/// An owner proof for a task context retained by a queue binding.
/// # Safety
/// The context, supplied queue, and parent target stay pinned until unbound,
/// or an endpoint's Drop revokes the binding first.
pub(crate) unsafe trait StableTaskSource<'a, 'd, T: Copy> {
    fn context(self) -> Pin<&'a TaskContext<T>>;
}

/// A queue implementation that may retain task and ready-set links.
/// # Safety
/// `attach` may retain `task` only until recycle or queue teardown, and every
/// returned link must name pinned storage covered by the binding's teardown.
pub(crate) unsafe trait BindingQueue<T: Copy> {
    type Input;

    fn attach(self: Pin<&Self>, input: Self::Input, task: PinnedLink<TaskContext<T>>) -> usize;
    fn ready(&self) -> &BatchSet;

    fn ready_link(self: Pin<&Self>) -> PinnedLink<BatchSet> {
        PinnedLink::from_stable(BindingSource(NonNull::from(self.ready())))
    }

    fn recycle_link(self: Pin<&Self>) -> Option<PinnedLink<TaskQueue<T>>> {
        None
    }
}

impl<T: Copy> TaskContext<T> {
    pub(crate) const fn new() -> Self {
        Self {
            node: Node::new(),
            queue: Cell::new(None),
        }
    }

    pub(crate) fn bind<'a, 'd, Q>(
        source: impl StableTaskSource<'a, 'd, T>,
        queue: Pin<&Q>,
        input: Q::Input,
        parent: Waker<'d>,
    ) -> Waker<'d>
    where
        Q: BindingQueue<T>,
        T: 'a,
    {
        let context = source.context();
        let node = context.project_ref().node;
        debug_assert!(!node.is_bound(), "task context already bound");
        let task = PinnedLink::from_stable(BindingSource(NonNull::from(context.get_ref())));
        let index = queue.attach(input, task);
        let ready = queue.ready_link();
        context.queue.set(queue.recycle_link());
        // SAFETY: StableTaskSource guarantees this binding is revoked before
        // the parent brand can expire; the erased target stays inside Node.
        let parent = unsafe { transmute::<WakeTarget<'_>, WakeTarget<'static>>(parent.target) };
        node.install(ready, index, parent);
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
            queue.get().recycle(index);
        }
    }

    /// Detaches a node while its queue is being dropped.
    pub(super) fn detach_queue(self: Pin<&Self>, queue: PinnedLink<TaskQueue<T>>, index: usize) {
        let node = self.project_ref().node;
        if self.queue.get() != Some(queue) || node.index() != Some(index) {
            return;
        }
        node.unbind();
        self.queue.set(None);
    }

    pub(crate) fn is_bound(&self) -> bool {
        self.node.is_bound()
    }

    pub(crate) fn waker<'a, 'd>(source: impl StableTaskSource<'a, 'd, T>) -> Waker<'d>
    where
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
impl<T: Copy> PinnedDrop for TaskContext<T> {
    fn drop(self: Pin<&mut Self>) {
        self.as_ref().unbind();
    }
}

type TaskBrand<'d> = PhantomData<(&'d Cell<()>, fn(&'d ()) -> &'d ())>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum WakeTarget<'d> {
    Node(PinnedLink<Node>),
    Ready(RootWaker<'d>),
}

struct NodeCompletion<'d> {
    node: PinnedLink<Node>,
    _brand: TaskBrand<'d>,
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

    pub fn waker(&self) -> Waker<'_> {
        self.wake.shorten()
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
    brand: TaskBrand<'d>,
}

impl<'d> Waker<'d> {
    fn from_node(node: PinnedLink<Node>) -> Self {
        Self {
            target: WakeTarget::Node(node),
            brand: PhantomData,
        }
    }

    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self {
            target: WakeTarget::Ready(RootWaker::from_ready(driver, key)),
            brand: PhantomData,
        }
    }

    pub(crate) fn shorten<'a>(self) -> Waker<'a>
    where
        'd: 'a,
    {
        unsafe { transmute(self) }
    }

    #[inline(always)]
    fn register_completion<R>(self, region: &RegionToken<'d>, registrar: R) -> R::Output
    where
        R: CompletionRegistrar<'d>,
    {
        match self.target {
            WakeTarget::Node(node) => CompletionWaker::register_callback(
                NodeCompletion {
                    node,
                    _brand: PhantomData,
                },
                region,
                registrar,
            ),
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
                NodeCompletion {
                    node,
                    _brand: PhantomData,
                },
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
    let node = unsafe { PinnedLink::from_raw(target.cast::<Node>()) };
    node.get().wake();
}

impl<'d> From<RootWaker<'d>> for Waker<'d> {
    fn from(root: RootWaker<'d>) -> Self {
        Self {
            target: WakeTarget::Ready(root),
            brand: PhantomData,
        }
    }
}
