use core::pin;

use dope::core::driver::schedule::ready::task;
use o3::collections::batch::set;

use crate::context;

#[pin_project::pin_project(PinnedDrop)]
#[repr(transparent)]
struct Core<'d> {
    #[pin]
    node: task::Node<'d>,
}

#[pin_project::pin_project]
#[repr(transparent)]
pub(crate) struct Binding<'d> {
    #[pin]
    core: Core<'d>,
}

#[pin_project::pin_project]
#[repr(transparent)]
pub(crate) struct RootBinding<'d> {
    #[pin]
    core: Core<'d>,
}

/// Owner proof for a task context retained by its wake chain.
/// # Safety
/// The returned context must remain pinned until unbind or Drop revokes every retained link.
pub(crate) unsafe trait StableBindingSource<'a, 'd> {
    fn context(self) -> pin::Pin<&'a Binding<'d>>;
}

/// Owner proof for a binding admitted directly beneath a driver root.
/// # Safety
/// The returned context must remain pinned until unbind or Drop revokes every retained link.
pub(crate) unsafe trait StableRootBindingSource<'a, 'd> {
    fn context(self) -> pin::Pin<&'a RootBinding<'d>>;
}

/// Pinned queue retained by every installed task binding.
/// # Safety
/// The set stays pinned and `attach` returns a valid erased index until unbind.
pub(crate) unsafe trait BindingQueue {
    type Input;

    fn attach(self: pin::Pin<&Self>, index: usize, input: Self::Input) -> usize;
    fn ready(self: pin::Pin<&Self>) -> pin::Pin<&set::Set<usize>>;
}

impl<'d> Core<'d> {
    const fn new() -> Self {
        use dope::core::driver::schedule::ready::task::Node;

        Self { node: Node::new() }
    }

    pub(crate) fn unbind(self: pin::Pin<&Self>) {
        let node = self.project_ref().node;
        let _ = task::raw::Binding::unbind(node);
    }

    pub(crate) fn is_bound(self: pin::Pin<&Self>) -> bool {
        let node = self.project_ref().node;
        task::raw::Binding::is_bound(node)
    }

    fn node(self: pin::Pin<&Self>) -> pin::Pin<&task::Node<'d>> {
        self.project_ref().node
    }
}

impl<'d> Binding<'d> {
    pub(crate) const fn new() -> Self {
        Self { core: Core::new() }
    }

    pub(crate) fn bind_domain<'a, Q, Tag, const N: usize>(
        source: impl StableBindingSource<'a, 'd>,
        queue: pin::Pin<&Q>,
        index: usize,
        input: Q::Input,
        domain: &mut task::Domain<'d, Tag, N>,
    ) -> Option<context::Waker<'d>>
    where
        Q: crate::raw::BindingQueue,
        'd: 'a,
    {
        let node = source.context().project_ref().core.node();
        debug_assert!(!task::raw::Binding::is_bound(node));
        let admission = domain.admit(node)?;
        let index = queue.attach(index, input);
        // SAFETY: the source and queue contracts retain both pinned endpoints.
        let wake = unsafe { task::raw::Binding::bind_leased(admission, queue.ready(), index) };
        Some(context::Waker::from_wake(wake))
    }

    pub(crate) fn reclaim_domain<'a, Tag, const N: usize>(
        source: impl StableBindingSource<'a, 'd>,
        index: usize,
        domain: &mut task::Domain<'d, Tag, N>,
    ) -> bool
    where
        'd: 'a,
    {
        let node = source.context().project_ref().core.node();
        // SAFETY: the only safe constructor binds this structurally-owned node
        // through the same borrowed domain, and every completion reclaims once.
        (unsafe { task::raw::Binding::reclaim_domain(domain, node) }) == Some(index)
    }

    pub(crate) fn waker<'a>(source: impl StableBindingSource<'a, 'd>) -> Option<context::Waker<'d>>
    where
        'd: 'a,
    {
        let node = source.context().project_ref().core.node();
        task::raw::Binding::waker(node).map(context::Waker::from_wake)
    }

    pub(crate) fn is_bound(self: pin::Pin<&Self>) -> bool {
        self.project_ref().core.is_bound()
    }
}

impl<'d> RootBinding<'d> {
    pub(crate) const fn new() -> Self {
        Self { core: Core::new() }
    }

    pub(crate) fn bind_root<'a, Q>(
        source: impl StableRootBindingSource<'a, 'd>,
        queue: pin::Pin<&Q>,
        index: usize,
        input: Q::Input,
        parent: context::RootWaker<'d>,
    ) -> Option<context::Waker<'d>>
    where
        Q: crate::raw::BindingQueue,
        'd: 'a,
    {
        let node = source.context().project_ref().core.node();
        debug_assert!(!task::raw::Binding::is_bound(node));
        let parent: context::Waker<'d> = parent.into();
        let admission = task::raw::Binding::admit(parent.0, node)?;
        let index = queue.attach(index, input);
        // SAFETY: RootBinding and queue retain both pinned endpoints, and the
        // nominal RootBinding exposes no non-root binding operation.
        let wake = unsafe { task::raw::Binding::bind(admission, queue.ready(), index) };
        Some(context::Waker::from_wake(wake))
    }

    pub(crate) fn root_poll<'a>(
        source: impl StableRootBindingSource<'a, 'd>,
    ) -> Option<(context::RootWaker<'d>, context::Waker<'d>)>
    where
        'd: 'a,
    {
        let node = source.context().project_ref().core.node();
        // SAFETY: RootBinding can only be installed through bind_root, whose
        // parent type cannot name a task node. Stale generations stay no-op.
        unsafe { task::raw::Binding::root_poll_unchecked(node) }.map(|(parent, wake)| {
            (
                context::RootWaker::from(parent),
                context::Waker::from_wake(wake),
            )
        })
    }

    pub(crate) fn root_parent<'a>(
        source: impl StableRootBindingSource<'a, 'd>,
    ) -> Option<context::RootWaker<'d>>
    where
        'd: 'a,
    {
        let node = source.context().project_ref().core.node();
        // SAFETY: RootBinding can only be installed through bind_root, whose
        // parent type cannot name a task node. Stale generations stay no-op.
        unsafe { task::raw::Binding::root_parent_unchecked(node) }.map(context::RootWaker::from)
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Core<'_> {
    fn drop(self: pin::Pin<&mut Self>) {
        self.as_ref().unbind();
    }
}

const _: () = assert!(
    core::mem::size_of::<Binding<'static>>() == core::mem::size_of::<RootBinding<'static>>()
);
const _: () = assert!(
    core::mem::align_of::<Binding<'static>>() == core::mem::align_of::<RootBinding<'static>>()
);
