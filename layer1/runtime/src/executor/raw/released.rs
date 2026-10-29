use std::{mem, pin};

use crate::executor;

/// Ended external client access to one exact pinned application.
#[must_use]
#[repr(transparent)]
pub struct Released<'app, R> {
    app: pin::Pin<&'app mut R>,
}

impl<'app, R> Released<'app, R> {
    pub(crate) const fn new(app: pin::Pin<&'app mut R>) -> Self {
        Self { app }
    }

    pub fn owner(&mut self) -> pin::Pin<&mut R> {
        self.app.as_mut()
    }

    /// # Safety
    /// Project an unchanged pinned owned field of this exact root, with no remaining external clients.
    pub unsafe fn visit<'d, T: executor::raw::App<'d>>(
        mut self,
        project: impl for<'a> FnOnce(pin::Pin<&'a mut R>) -> pin::Pin<&'a mut T>,
    ) -> Self {
        let app = project(self.app.as_mut());
        let _ = T::released(Released::new(app));
        self
    }

    /// # Safety
    /// Project the unchanged optional pinned owned field of this exact root, with no remaining external clients.
    pub unsafe fn optional<'d, T: executor::raw::App<'d>>(
        mut self,
        project: impl for<'a> FnOnce(pin::Pin<&'a mut R>) -> Option<pin::Pin<&'a mut T>>,
    ) -> Self {
        if let Some(app) = project(self.app.as_mut()) {
            let _ = T::released(Released::new(app));
        }
        self
    }
}

const _: () = assert!(mem::size_of::<Released<'_, ()>>() == mem::size_of::<usize>());
