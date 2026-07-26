pub(crate) mod bridges;
pub mod queue;

use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::transmute;
use core::pin::Pin;
use core::ptr::NonNull;

use dope::driver::ready::{CompletionWaker, ReadyKey};
use dope::{DriverContext, DriverRef};
use o3::cell::RegionToken;
use o3::collections::BatchSet;
use o3::marker::ThreadBound;

use pin_project::pin_project;
use queue::{IndexQueue, TaskQueue};

struct Node {
    ready: Cell<Option<NonNull<BatchSet>>>,
    index: Cell<usize>,
    parent: Cell<Option<NonNull<Node>>>,
    root_driver: Cell<Option<DriverRef<'static>>>,
    root_key: Cell<ReadyKey<'static>>,
    _pin: PhantomPinned,
    _thread: ThreadBound,
}

impl Node {
    const fn new() -> Self {
        Self {
            ready: Cell::new(None),
            index: Cell::new(usize::MAX),
            parent: Cell::new(None),
            root_driver: Cell::new(None),
            root_key: Cell::new(ReadyKey::NONE),
            _pin: PhantomPinned,
            _thread: ThreadBound::NEW,
        }
    }

    unsafe fn bind(&self, ready: &BatchSet, index: usize, parent: Option<Waker<'_>>) {
        assert!(
            index < ready.capacity(),
            "dope-fiber: task index out of bounds"
        );
        assert!(
            self.ready.get().is_none(),
            "dope-fiber: task node already bound"
        );
        self.index.set(index);
        self.ready.set(Some(NonNull::from(ready)));
        match parent.map(|waker| waker.target) {
            Some(WakeTarget::Node(node)) => {
                let parent = unsafe { node.as_ref() };
                self.parent.set(Some(node));
                self.root_driver.set(parent.root_driver.get());
                self.root_key.set(parent.root_key.get());
            }
            Some(WakeTarget::Ready(driver, key)) => {
                self.parent.set(None);
                self.root_driver.set(Some(unsafe {
                    transmute::<DriverRef<'_>, DriverRef<'static>>(driver)
                }));
                self.root_key
                    .set(unsafe { transmute::<ReadyKey<'_>, ReadyKey<'static>>(key) });
            }
            None => {
                self.parent.set(None);
                self.root_driver.set(None);
                self.root_key.set(ReadyKey::NONE);
            }
        }
    }

    unsafe fn unbind(self: Pin<&Self>) {
        if let Some(ready) = self.ready.replace(None) {
            unsafe { ready.as_ref() }.remove(self.index.get());
        }
        self.parent.set(None);
        self.root_driver.set(None);
        self.root_key.set(ReadyKey::NONE);
        self.index.set(usize::MAX);
    }

    fn wake(self: Pin<&Self>) {
        let mut node = NonNull::from(self.get_ref());
        loop {
            let current = unsafe { node.as_ref() };
            let Some(ready) = current.ready.get() else {
                return;
            };
            if !unsafe { ready.as_ref() }.insert(current.index.get()) {
                return;
            }
            if let Some(parent) = current.parent.get() {
                node = parent;
                continue;
            }
            if let Some(driver) = current.root_driver.get() {
                driver.activate_ready(current.root_key.get());
            }
            return;
        }
    }
}

#[repr(C)]
pub(crate) struct TaskContext<T: Copy = usize> {
    node: Node,
    queue: Cell<Option<NonNull<TaskQueue<T>>>>,
    index: Cell<usize>,
}

impl<T: Copy> TaskContext<T> {
    pub(crate) const fn new() -> Self {
        Self {
            node: Node::new(),
            queue: Cell::new(None),
            index: Cell::new(usize::MAX),
        }
    }

    pub(crate) unsafe fn bind_inner<'d>(
        self: Pin<&Self>,
        queue: Pin<&TaskQueue<T>>,
        target: T,
        parent: Option<Waker<'d>>,
    ) -> Waker<'d> {
        assert!(self.index.get() == usize::MAX, "task context already bound");
        let index = queue.allocate(target, NonNull::from(self.get_ref()));
        self.queue.set(Some(NonNull::from(queue.get_ref())));
        self.index.set(index);
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        unsafe { node.bind(&queue.ready, index, parent) };
        Waker::from_node(NonNull::from(node.get_ref()))
    }

    /// # Safety
    /// The task, queue, and parent stay pinned and live until unbound, and no
    /// other task is bound to `index` in this queue during that interval.
    pub(crate) unsafe fn bind_index<'d>(
        self: Pin<&Self>,
        queue: Pin<&IndexQueue>,
        index: usize,
        parent: Waker<'d>,
    ) -> Waker<'d> {
        assert!(self.index.get() == usize::MAX, "task context already bound");
        self.index.set(index);
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        unsafe { node.bind(&queue.ready, index, Some(parent)) };
        Waker::from_node(NonNull::from(node.get_ref()))
    }

    /// # Safety
    /// No context or waker for this task may be used after this call.
    pub(crate) unsafe fn unbind(self: Pin<&Self>) {
        let index = self.index.replace(usize::MAX);
        if index == usize::MAX {
            return;
        }
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        unsafe { node.unbind() };
        if let Some(queue) = self.queue.replace(None) {
            unsafe { queue.as_ref() }.release(index);
        }
    }

    /// Detaches a node while its queue is being dropped.
    ///
    /// # Safety
    /// `queue` is the live queue currently recorded by this pinned task, and
    /// `index` is the queue slot that points back to it.
    pub(super) unsafe fn detach_queue(
        self: Pin<&Self>,
        queue: NonNull<TaskQueue<T>>,
        index: usize,
    ) {
        if self.queue.get() != Some(queue) || self.index.get() != index {
            return;
        }
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        unsafe { node.unbind() };
        self.queue.set(None);
        self.index.set(usize::MAX);
    }

    pub(crate) fn is_bound(&self) -> bool {
        self.index.get() != usize::MAX
    }

    /// # Safety
    /// The bound task and queue stay pinned and live for `'d`.
    pub(crate) unsafe fn context_unchecked<'d>(self: Pin<&Self>) -> Waker<'d> {
        assert!(self.index.get() != usize::MAX, "task context not bound");
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        Waker::from_node(NonNull::from(node.get_ref()))
    }

    pub(crate) fn wake(self: Pin<&Self>) {
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        node.wake();
    }
}

impl<T: Copy> Drop for TaskContext<T> {
    fn drop(&mut self) {
        if self.index.get() == usize::MAX {
            return;
        }
        // SAFETY: a task can only become bound after it has been pinned. Drop
        // runs at that stable address and no waker may be used after its owner
        // has dropped the task context.
        unsafe { Pin::new_unchecked(&*self).unbind() };
    }
}

type TaskBrand<'d> = PhantomData<(&'d Cell<()>, fn(&'d ()) -> &'d ())>;

#[derive(Clone, Copy)]
enum WakeTarget<'d> {
    Node(NonNull<Node>),
    Ready(DriverRef<'d>, ReadyKey<'d>),
}

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

    #[doc(hidden)]
    pub fn completion_waker(&self) -> CompletionWaker<'d> {
        self.wake.completion()
    }

    pub fn driver_access(self: Pin<&mut Self>) -> DriverContext<'_, 'd> {
        self.project().driver.reborrow()
    }

    pub fn region_token(self: Pin<&mut Self>) -> &mut RegionToken<'d> {
        self.project().driver.region_token()
    }

    /// # Safety
    /// The wake target stays live for `'a`.
    pub unsafe fn waker_unchecked<'a>(&self) -> Waker<'a> {
        unsafe { transmute(self.wake) }
    }

    pub fn wake(&self) {
        self.wake.wake();
    }
}

#[derive(Clone, Copy)]
pub struct Waker<'d> {
    target: WakeTarget<'d>,
    brand: TaskBrand<'d>,
}

impl<'d> Waker<'d> {
    fn from_node(node: NonNull<Node>) -> Self {
        Self {
            target: WakeTarget::Node(node),
            brand: PhantomData,
        }
    }

    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self {
            target: WakeTarget::Ready(driver, key),
            brand: PhantomData,
        }
    }

    pub(crate) fn shorten<'a>(self) -> Waker<'a>
    where
        'd: 'a,
    {
        unsafe { transmute(self) }
    }

    /// Converts this task waker into the type-erased handle stored by Dope's
    /// completion tables.
    pub fn completion(self) -> CompletionWaker<'d> {
        match self.target {
            WakeTarget::Node(node) => unsafe {
                CompletionWaker::from_callback(node.cast(), wake_node)
            },
            WakeTarget::Ready(driver, key) => CompletionWaker::from_ready(driver, key),
        }
    }

    pub fn wake(self) {
        match self.target {
            WakeTarget::Node(node) => unsafe { Pin::new_unchecked(node.as_ref()) }.wake(),
            WakeTarget::Ready(driver, key) => driver.activate_ready(key),
        }
    }
}

/// A driver-owned wake target that may outlive the object which exposed it.
///
/// Ready keys are generational, so waking after their slot has been released
/// is a safe no-op. Unlike [`Waker`], this type can never name a task node.
#[derive(Clone, Copy)]
pub struct RootWaker<'d> {
    driver: DriverRef<'d>,
    key: ReadyKey<'d>,
}

impl<'d> RootWaker<'d> {
    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self { driver, key }
    }
}

unsafe fn wake_node(target: NonNull<()>) {
    let node = target.cast::<Node>();
    unsafe { Pin::new_unchecked(node.as_ref()) }.wake();
}

impl<'d> From<RootWaker<'d>> for Waker<'d> {
    fn from(root: RootWaker<'d>) -> Self {
        Self::from_ready(root.driver, root.key)
    }
}

impl PartialEq for Waker<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.target, other.target) {
            (WakeTarget::Node(left), WakeTarget::Node(right)) => left == right,
            (
                WakeTarget::Ready(left_driver, left_key),
                WakeTarget::Ready(right_driver, right_key),
            ) => left_driver == right_driver && left_key == right_key,
            _ => false,
        }
    }
}

impl Eq for Waker<'_> {}
