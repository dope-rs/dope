use std::marker;

use dope_sys::Errno;
use o3::base::{cell::region, marker::ThreadBound};

use crate::{
    driver::{self, life::Final, receive::credit, schedule::clock},
    io::event::net::recv,
};

/// Post-quiescence access to driver resource finalization.
///
/// File-table mutation is deliberately absent: descriptors remain affine
/// fields of their owner and are reclaimed only after that owner is dropped.
///
#[must_use]
pub struct Finish<'a, 'd> {
    last: Final<'a, 'd>,
    forced: bool,
    disarm: Option<Errno>,
}

impl<'a, 'd> Finish<'a, 'd> {
    pub(super) fn new(last: Final<'a, 'd>, forced: bool) -> Self {
        Self {
            last,
            forced,
            disarm: None,
        }
    }

    pub(super) fn resume(self) {
        self.last.resume();
    }

    /// Returns whether cooperative draining ended before owner quiescence. The
    /// backend is already fully quiesced in either case.
    pub fn is_forced(&self) -> bool {
        self.forced
    }

    /// Records the first native timer-disarm failure while finalization continues.
    pub fn fail_disarm(&mut self, cause: Errno) {
        if self.disarm.is_none() {
            self.disarm = Some(cause);
        }
    }

    /// Returns the first timer-disarm failure; this does not prove timer deletion.
    pub fn disarm_error(&self) -> Option<Errno> {
        self.disarm
    }

    /// Monotonic authority captured for the exact turn that reached finalization.
    pub fn clock<'f>(&'f self) -> clock::Clock<'f, 'd> {
        self.last.context().driver_ref().clock()
    }

    /// Returns an observed receive to its quiescent driver and reports its result.
    pub fn reclaim(&mut self, done: recv::Completion<'d>) {
        self.last.context_mut().reclaim(done);
    }

    /// Region authority paired with this exact post-quiescence proof.
    pub fn region_token<'f>(&'f mut self) -> &'f mut region::Token<region::Identity<'d>> {
        self.last.context_mut().region_token()
    }

    pub fn state_token<'f>(&'f mut self) -> &'f mut region::Token<region::Identity<'d, 1>> {
        self.last.context_mut().state_token()
    }

    pub(in crate::driver) fn brand(&self) -> driver::Invariant<'d> {
        self.last.context().driver_ref().brand
    }

    /// Binds retained-credit finalization to this exact quiescent turn.
    pub fn credit<'f>(&'f mut self) -> Credit<'f, 'd> {
        Credit::new(self.brand())
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Finish<'static, 'static>>()
            == std::mem::size_of::<(Final<'static, 'static>, bool)>()
    );
};

/// Quiescent authority over one retained-credit owner.
#[repr(transparent)]
pub struct Credit<'a, 'd> {
    borrow: marker::PhantomData<(&'a mut (), driver::Invariant<'d>, ThreadBound)>,
}

impl<'a, 'd> Credit<'a, 'd> {
    const fn new(_brand: driver::Invariant<'d>) -> Self {
        Self {
            borrow: marker::PhantomData,
        }
    }

    /// Detaches every slot proved active by the quiescent storage borrow.
    pub fn detach(self, storage: &credit::Storage<'d>) {
        for slot in storage.entries.iter() {
            slot.detach();
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Credit<'static, 'static>>() == 0);
};
