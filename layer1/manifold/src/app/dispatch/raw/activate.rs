use std::pin;

use dope_core::driver::{route, schedule};

use crate::app::dispatch;

/// An owner that semantically consumes activations for one exact route.
///
/// # Safety
/// The pinned owner stays installed and the target has its canonical tag.
pub unsafe trait Activate<'d, R: route::App> {
    type Access;

    /// # Safety
    /// The owner is installed and the target carries its canonical route tag.
    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        target: route::Target<'d, R>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    );
}
