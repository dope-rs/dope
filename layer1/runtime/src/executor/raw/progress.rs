use dope_core::driver::schedule::clock;
use o3::base::cell::region;

/// Read-only progress authority for one exact driver turn.
#[derive(Clone, Copy)]
#[must_use]
pub struct Progress<'region, 'turn, 'd> {
    region: &'region (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ),
    clock: clock::Clock<'turn, 'd>,
}

impl<'region, 'turn, 'd> Progress<'region, 'turn, 'd> {
    pub(crate) const fn new(
        region: &'region (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        clock: clock::Clock<'turn, 'd>,
    ) -> Self {
        Self { region, clock }
    }

    pub const fn regions(
        self,
    ) -> &'region (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ) {
        self.region
    }

    pub const fn clock(self) -> clock::Clock<'turn, 'd> {
        self.clock
    }
}

const _: () = {
    assert!(std::mem::size_of::<Progress<'_, '_, '_>>() == 2 * std::mem::size_of::<usize>());
    assert!(std::mem::align_of::<Progress<'_, '_, '_>>() == std::mem::align_of::<usize>());
};
