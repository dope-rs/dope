mod queue;

use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem;
use core::pin::Pin;
use core::ptr::NonNull;

use dope::driver::ready::{CompletionWaker, ReadyKey};
use dope::{DriverContext, DriverRef};
use o3::collections::BatchSet;
use o3::marker::ThreadBound;

pub(crate) use queue::IndexQueue;
pub use queue::TaskQueue;

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
                    mem::transmute::<DriverRef<'_>, DriverRef<'static>>(driver)
                }));
                self.root_key
                    .set(unsafe { mem::transmute::<ReadyKey<'_>, ReadyKey<'static>>(key) });
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

    fn ready_key(&self) -> ReadyKey<'static> {
        self.root_key.get()
    }
}

#[repr(C)]
pub struct TaskContext<T: Copy = usize> {
    node: Node,
    queue: Cell<Option<NonNull<TaskQueue<T>>>>,
    index: Cell<usize>,
    target: Cell<T>,
}

impl TaskContext<usize> {
    pub const fn new() -> Self {
        Self::with_target(0)
    }
}

impl<T: Copy> TaskContext<T> {
    pub const fn with_target(target: T) -> Self {
        Self {
            node: Node::new(),
            queue: Cell::new(None),
            index: Cell::new(usize::MAX),
            target: Cell::new(target),
        }
    }

    /// # Safety
    /// The task and queue stay pinned and live until unbound.
    pub unsafe fn bind<'task, 'parent>(
        self: Pin<&'task Self>,
        queue: Pin<&'task TaskQueue<T>>,
        target: T,
        parent: Option<Waker<'parent>>,
    ) -> Waker<'task>
    where
        'parent: 'task,
    {
        let parent: Option<Waker<'task>> = parent.map(|waker| unsafe { mem::transmute(waker) });
        unsafe { self.bind_inner(queue, target, parent) }
    }

    /// # Safety
    /// The task, queue, and parent stay pinned and live until unbound.
    pub unsafe fn bind_child<'d>(
        self: Pin<&Self>,
        queue: Pin<&TaskQueue<T>>,
        target: T,
        parent: Waker<'d>,
    ) -> Waker<'d> {
        unsafe { self.bind_inner(queue, target, Some(parent)) }
    }

    unsafe fn bind_inner<'d>(
        self: Pin<&Self>,
        queue: Pin<&TaskQueue<T>>,
        target: T,
        parent: Option<Waker<'d>>,
    ) -> Waker<'d> {
        assert!(self.index.get() == usize::MAX, "task context already bound");
        let index = queue.allocate(target);
        self.queue.set(Some(NonNull::from(queue.get_ref())));
        self.index.set(index);
        self.target.set(target);
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
        target: T,
        parent: Waker<'d>,
    ) -> Waker<'d> {
        assert!(self.index.get() == usize::MAX, "task context already bound");
        self.index.set(index);
        self.target.set(target);
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        unsafe { node.bind(&queue.ready, index, Some(parent)) };
        Waker::from_node(NonNull::from(node.get_ref()))
    }

    /// # Safety
    /// No context or waker for this task may be used after this call.
    pub unsafe fn unbind(self: Pin<&Self>) {
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

    pub fn set_target(self: Pin<&Self>, target: T) {
        self.target.set(target);
        if let Some(queue) = self.queue.get() {
            unsafe { queue.as_ref() }.set_target(self.index.get(), target);
        }
    }

    pub fn target(&self) -> T {
        self.target.get()
    }

    /// # Safety
    /// The bound task and queue stay pinned and live for `'d`.
    pub unsafe fn context_unchecked<'d>(self: Pin<&Self>) -> Waker<'d> {
        assert!(self.index.get() != usize::MAX, "task context not bound");
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        Waker::from_node(NonNull::from(node.get_ref()))
    }

    pub fn wake(self: Pin<&Self>) {
        let node = unsafe { self.map_unchecked(|task| &task.node) };
        node.wake();
    }
}

impl Default for TaskContext<usize> {
    fn default() -> Self {
        Self::new()
    }
}

type TaskBrand<'d> = PhantomData<(&'d Cell<()>, fn(&'d ()) -> &'d ())>;

#[derive(Clone, Copy)]
enum WakeTarget<'d> {
    Node(NonNull<Node>),
    Ready(DriverRef<'d>, ReadyKey<'d>),
}

pub struct Context<'poll, 'd> {
    wake: Waker<'d>,
    driver: DriverContext<'poll, 'd>,
    _pin: PhantomPinned,
}

impl<'poll, 'd> Context<'poll, 'd> {
    pub fn from_waker(wake: Waker<'d>, driver: DriverContext<'poll, 'd>) -> Self {
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

    #[doc(hidden)]
    pub fn ready_key(&self) -> ReadyKey<'d> {
        self.wake.ready_key()
    }

    pub fn raw_task(self: Pin<&mut Self>) -> *mut () {
        unsafe { self.get_unchecked_mut() as *mut Self }.cast()
    }

    /// # Safety
    /// `task` names a live `Context<'_, 'd>` for the duration of the current poll.
    pub unsafe fn from_raw_task(task: *mut ()) -> Context<'poll, 'd>
    where
        'd: 'poll,
    {
        let context = unsafe { &mut *task.cast::<Context<'_, 'd>>() };
        Context::from_waker(context.wake, context.driver.reborrow())
    }

    /// # Safety
    /// `task` names a live `Context` for the duration of the current poll.
    pub unsafe fn wake_raw(task: *const ()) {
        let context = unsafe { &*task.cast::<Context<'_, '_>>() };
        context.wake.wake();
    }

    #[inline]
    pub fn waker(&self) -> Waker<'_> {
        unsafe { mem::transmute(self.wake) }
    }

    #[doc(hidden)]
    pub fn completion_waker(&self) -> CompletionWaker<'d> {
        self.wake.completion()
    }

    pub fn into_waker(self) -> Waker<'d> {
        self.wake
    }

    pub fn driver_access(self: Pin<&mut Self>) -> DriverContext<'_, 'd> {
        unsafe { self.get_unchecked_mut() }.driver.reborrow()
    }

    pub(crate) fn child(self: Pin<&mut Self>, wake: Waker<'d>) -> Context<'_, 'd> {
        Context::from_waker(wake, self.driver_access())
    }

    /// # Safety
    /// The wake target stays live for `'a`.
    pub unsafe fn waker_unchecked<'a>(&self) -> Waker<'a> {
        unsafe { mem::transmute(self.wake) }
    }

    #[inline]
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

    pub fn shorten<'a>(self) -> Waker<'a>
    where
        'd: 'a,
    {
        unsafe { mem::transmute(self) }
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

    #[doc(hidden)]
    pub fn ready_key(self) -> ReadyKey<'d> {
        match self.target {
            WakeTarget::Node(node) => unsafe { node.as_ref() }.ready_key(),
            WakeTarget::Ready(_, key) => key,
        }
    }

    #[inline]
    pub fn wake(self) {
        match self.target {
            WakeTarget::Node(node) => unsafe { Pin::new_unchecked(node.as_ref()) }.wake(),
            WakeTarget::Ready(driver, key) => driver.activate_ready(key),
        }
    }
}

unsafe fn wake_node(target: NonNull<()>) {
    let node = target.cast::<Node>();
    unsafe { Pin::new_unchecked(node.as_ref()) }.wake();
}

impl PartialEq for Waker<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.target, other.target) {
            (WakeTarget::Node(left), WakeTarget::Node(right)) => left == right,
            (WakeTarget::Ready(_, left_key), WakeTarget::Ready(_, right_key)) => {
                left_key == right_key
            }
            _ => false,
        }
    }
}

impl Eq for Waker<'_> {}
