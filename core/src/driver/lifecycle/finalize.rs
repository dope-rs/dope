use crate::driver::lifecycle::{quiesce, routing};

/// Post-quiescence access to driver resource finalization.
///
/// File-table mutation is deliberately absent: descriptors remain affine
/// fields of their owner and are reclaimed only after that owner is dropped.
///
/// ```compile_fail,E0599
/// use dope_core::driver::lifecycle::Finalize;
///
/// fn cannot_reopen_file_authority(finish: &mut Finalize<'_, '_>) {
///     let _ = finish.files();
/// }
/// ```
#[must_use]
#[repr(transparent)]
pub struct Finalize<'a, 'd> {
    finalization: quiesce::Final<'a, 'd>,
}

impl<'a, 'd> Finalize<'a, 'd> {
    #[doc(hidden)]
    pub fn new(finalization: quiesce::Final<'a, 'd>) -> Self {
        Self { finalization }
    }

    #[doc(hidden)]
    pub fn reborrow(&mut self) -> Finalize<'_, 'd> {
        Finalize {
            finalization: self.finalization.reborrow(),
        }
    }

    pub fn retire_route<const ID: u8>(&mut self, route: &routing::Route<'d, ID>) {
        route.finish(self.finalization.context());
    }

    /// Stages a quiesced storage route for a later application installation.
    ///
    /// ```compile_fail
    /// use dope_core::driver::lifecycle::{Finalize, routing::Route};
    ///
    /// fn stage_application<'a, 'd>(
    ///     finish: &mut Finalize<'a, 'd>,
    ///     route: &Route<'d, 7>,
    /// ) {
    ///     finish.stage_route(route);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use dope_core::driver::lifecycle::{Finalize, routing::StorageRoute};
    ///
    /// fn retire_storage<'a, 'd>(
    ///     finish: &mut Finalize<'a, 'd>,
    ///     route: &StorageRoute<'d, 7>,
    /// ) {
    ///     finish.retire_route(route);
    /// }
    /// ```
    pub fn stage_route<const ID: u8>(&mut self, route: &routing::StorageRoute<'d, ID>) {
        route.stage();
    }
}

const _: () = assert!(
    std::mem::size_of::<Finalize<'static, 'static>>()
        == std::mem::size_of::<quiesce::Final<'static, 'static>>()
);
