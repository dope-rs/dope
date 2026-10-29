use std::{marker, mem, pin};

use dope_core::driver;

mod bound;
mod forward;
mod owner;
mod project;
mod resume;

pub use bound::Bound;
pub use owner::Owner;

/// Lifecycle storage borrowing the exact pinned provider.
pub(crate) type Borrowed<'a, T> = Owner<pin::Pin<&'a mut T>, T>;
pub use project::Project;
pub use resume::Resume;

/// Operational ordering remains identical to an ordinary head-first application.
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = crate, runtime = crate, driver = 'd)]
pub struct Pair<'app, 'd, H, T> {
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

impl<'app, 'd, H, T> Pair<'app, 'd, H, T> {
    pub(super) const fn new(head: pin::Pin<&'app mut H>, tail: T) -> Self {
        Self {
            driver: marker::PhantomData,
            head: Owner::new(head),
            tail: Owner::new(mem::ManuallyDrop::new(tail)),
        }
    }

    pub(super) fn tail(self: pin::Pin<&Self>) -> pin::Pin<&T> {
        Project::shared(self.project_ref().tail)
    }
}
