use std::pin::Pin;

use dope::manifold::dispatch::raw::Manifold;

use crate::tests::Dummy;

// SAFETY: Dummy retains no driver-visible owner and participates in the full
// generated shutdown sequence exercised by this test crate.
unsafe impl<'d> Manifold<'d> for Dummy {
    const ID: u8 = 0;

    type Dispatch = dope::manifold::dispatch::raw::Plain;
    type Activate = dope::manifold::dispatch::raw::Plain;
    type PrePark = dope::manifold::dispatch::raw::Plain;
    type Shutdown = dope::manifold::dispatch::raw::Plain;

    unsafe fn pre_park<'turn>(
        self: Pin<&mut Self>,
        _turn: dope::core::driver::schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let _ = self;
    }

    fn shutdown<'turn>(
        self: Pin<&mut Self>,
        _turn: dope::core::driver::schedule::Turn<'turn, 'd>,
        _driver: &mut dope::manifold::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        let _ = self;
    }
}
