use std::{ops, time};

use dope_core::driver::schedule::clock;

use crate::executor::{self, drive, root, session};

/// Installed application scope whose protocol time is caller-controlled.
#[repr(transparent)]
pub struct Manual<'app, 'life: 'app, 'd: 'life, D: executor::raw::Layout<'d>, Q = ()> {
    pub(super) access: session::access::Access<'app, 'life, 'd, D, Q>,
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> Manual<'app, 'life, 'd, D, Q>
where
    D: executor::raw::App<'d>,
{
    pub fn drive<R>(
        &mut self,
        now: clock::Deadline<'d>,
        root: R,
    ) -> Result<session::Fixed<'d, R::Output>, drive::Error>
    where
        R: root::Root<'d>,
    {
        use drive::clock::at;

        self.access.core.check_time(now.into_raw())?;
        self.access
            .reborrow()
            .drive_fixed(at(now), root, drive::tick)
    }

    pub fn drive_for<R>(
        &mut self,
        now: clock::Deadline<'d>,
        timeout: time::Duration,
        root: R,
    ) -> Result<session::Fixed<'d, R::Output>, drive::Error>
    where
        R: root::Root<'d>,
    {
        use drive::clock::{Bound, at};

        self.access.core.check_time(now.into_raw())?;
        let source = Bound::until(at(now), timeout)?;
        self.access
            .reborrow()
            .drive_fixed(source, root, drive::timeout)
    }
}

impl<'app, 'life: 'app, 'd: 'life, D, Q> ops::Deref for Manual<'app, 'life, 'd, D, Q>
where
    D: executor::raw::Layout<'d>,
{
    type Target = session::access::Access<'app, 'life, 'd, D, Q>;

    fn deref(&self) -> &Self::Target {
        &self.access
    }
}

impl<'d, D: executor::raw::Layout<'d>, Q> ops::DerefMut for Manual<'_, '_, 'd, D, Q> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.access
    }
}
