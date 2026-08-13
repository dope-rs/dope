use crate::{
    backend::{fixed, uring::engine::lifecycle},
    driver,
};

#[repr(transparent)]
pub(in crate::backend::uring) struct RetireWork(lifecycle::CloseWork);

const _: () =
    assert!(std::mem::size_of::<RetireWork>() == std::mem::size_of::<lifecycle::CloseWork>());

impl RetireWork {
    pub(super) fn new(work: lifecycle::CloseWork) -> Self {
        debug_assert!(work.retires_slot());
        Self(work)
    }

    pub(in crate::backend::uring) fn into_retirement<'d>(
        self,
        driver: driver::Reference<'d>,
    ) -> fixed::Retirement<'d> {
        // SAFETY: retire work is affine and can only originate by consuming a
        // fixed slot or by restoring one kernel completion in the raw boundary.
        let retired = unsafe { fixed::raw::Retirement::from_deferred(self.0.slot()) };
        retired.bind(driver)
    }
}
