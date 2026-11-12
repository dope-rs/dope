use std::{marker, mem, pin, ptr};

use crate::executor;
use dope_core::driver;

mod forward;
mod owner;
mod project;
mod resume;

pub use owner::Owner;

/// Lifecycle storage borrowing the exact pinned provider.
pub(crate) type Borrowed<'a, T> = Owner<pin::Pin<&'a mut T>, T>;
pub use project::Project;
pub use resume::Resume;

// SAFETY: the owned pair forwards each lifecycle operation to both exact pinned
// roots. Each root independently permits resume after stopped native quiescence;
// the pair adds no operations or cleanup awaiting dispatch between them.
unsafe impl<'d, A, B> Resume<'d> for crate::client::Pair<'d, A, B>
where
    A: crate::topology::lending::Root<'d> + Resume<'d>,
    B: crate::topology::lending::Root<'d> + Resume<'d>,
{
}

/// A dependent application retained by its pinned provider owner.
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = crate, runtime = crate, driver = 'd, lending = true)]
pub struct Bound<'app, 'd, H, T> {
    #[dispatcher(state)]
    driver: marker::PhantomData<driver::Reference<'d>>,
    #[pin]
    #[app]
    head: Borrowed<'app, H>,
    #[pin]
    #[app]
    #[drain]
    tail: Owner<mem::ManuallyDrop<T>, T>,
}

impl<'app, 'd, H, T> Bound<'app, 'd, H, T> {
    pub(crate) const fn new(head: pin::Pin<&'app mut H>, tail: T) -> Self {
        const {
            assert!(mem::size_of::<Self>() == mem::size_of::<(pin::Pin<&'app mut H>, T)>());
            assert!(mem::align_of::<Self>() == mem::align_of::<(pin::Pin<&'app mut H>, T)>());
            assert!(!mem::needs_drop::<Self>());
        }
        Self {
            driver: marker::PhantomData,
            head: Owner::new(head),
            tail: Owner::new(mem::ManuallyDrop::new(tail)),
        }
    }

    pub fn tail(self: pin::Pin<&Self>) -> pin::Pin<&T> {
        Project::shared(self.project_ref().tail)
    }
}

impl<'parent, 'd, H, T> Bound<'parent, 'd, H, T>
where
    H: executor::raw::App<'d>,
    T: executor::raw::App<'d>,
{
    /// # Safety
    /// Consume this exact native-quiesced owner; never project or finish its backing again.
    pub(crate) unsafe fn finish_tail<'call, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'call, 'finish, 'owner, 'epoch, 'd, Self>,
        plan: Option<<Self as executor::raw::Layout<'d>>::ParkPlan>,
    ) -> (
        Borrowed<'parent, H>,
        Option<<H as executor::raw::Layout<'d>>::ParkPlan>,
    ) {
        let (mut app, install, finish) = call.into_parts();
        let tail = app.as_mut().project().tail;
        let call = executor::raw::Finish::new(tail, install, finish);
        let _ = <Owner<mem::ManuallyDrop<T>, T> as executor::raw::Finishes<'d>>::call(call);
        let head = app.as_mut().project().head;
        // SAFETY: the consumed owner makes its old backing inaccessible. Moving
        // this Pin handle preserves the actual provider's address and full loan.
        let head = unsafe { ptr::read(head.get_unchecked_mut()) };
        (head, plan.and_then(|plan| plan.3))
    }
}

const _: () = {
    assert!(mem::size_of::<Bound<'_, '_, (), ()>>() == mem::size_of::<&mut ()>());
    assert!(mem::align_of::<Bound<'_, '_, (), ()>>() == mem::align_of::<&mut ()>());
    assert!(!mem::needs_drop::<Bound<'_, '_, (), ()>>());
};
