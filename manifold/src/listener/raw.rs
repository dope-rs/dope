use std::pin;

use crate::listener::{self, handler};

/// Restricted coordinate view for an installed listener application.
/// # Safety
/// The view cannot move, replace, or drop driver-branded retained storage.
pub unsafe trait ControlHandler<'d, const ID: u8>: handler::Application<'d, ID> {
    type Control<'step>
    where
        Self: 'step,
        'd: 'step;

    /// # Safety
    /// `application` must be the handler pinned beneath its installed listener
    /// and no listener lifecycle phase may overlap the returned control.
    unsafe fn control<'step>(application: pin::Pin<&'step mut Self>) -> Self::Control<'step>
    where
        'd: 'step;
}

pub(super) struct Installed<'a, A> {
    application: pin::Pin<&'a mut A>,
}

impl<'a, A> Installed<'a, A> {
    pub(super) fn new<'step, 'd, const ID: u8, E>(
        control: &'a mut listener::Control<'step, 'd, ID, A, E>,
    ) -> Self
    where
        'd: 'step,
        A: handler::Application<'d, ID>,
        E: crate::Env<Wire = A::Wire>,
    {
        Self {
            application: control.inner.as_mut().project().app,
        }
    }

    pub(super) fn control<'d, const ID: u8>(self) -> <A as ControlHandler<'d, ID>>::Control<'a>
    where
        'd: 'a,
        A: ControlHandler<'d, ID>,
    {
        // SAFETY: `Control` exclusively owns the exact installed listener and
        // the returned view is bounded by this borrow.
        unsafe { A::control(self.application) }
    }
}
