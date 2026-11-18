use std::{cell::UnsafeCell, pin::Pin};

use dope_core::driver::route::Identity;
use dope_net::link::egress::data;
use o3::buffer::resident;

use super::{Frame, release};

pub(in crate::network::listener) struct Home<T>(pub(super) UnsafeCell<T>);

pub(in crate::network::listener) struct DirectSlot<
    'p,
    's,
    'd,
    ID: Identity,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
>(Backing<'p, 's, 'd, ID, Frame<'d, B>>);

enum Backing<'p, 's, 'd, ID: Identity, T> {
    Fixed(&'p Home<T>),
    Shared(release::Lease<'p, 's, 'd, T, ID>),
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> DirectSlot<'p, 's, 'd, ID, B> {
    pub(super) fn shared(lease: release::Lease<'p, 's, 'd, Frame<'d, B>, ID>) -> Self {
        Self(Backing::Shared(lease))
    }

    /// # Safety
    /// The original clear generation exclusively lends this home to one output.
    /// Publication transfers it to that generation's native send until retirement.
    pub(in crate::network::listener) unsafe fn fixed(home: &'p Home<Frame<'d, B>>) -> Self {
        Self(Backing::Fixed(home))
    }

    pub(in crate::network::listener::writer) fn home(&self) -> Option<&'p Home<Frame<'d, B>>> {
        match &self.0 {
            Backing::Fixed(home) => Some(*home),
            Backing::Shared(_) => None,
        }
    }

    pub(in crate::network::listener::writer) fn get_mut(&mut self) -> Pin<&mut Frame<'d, B>> {
        match &mut self.0 {
            // SAFETY: fixed admission bound this immobile cell to the original
            // exclusive output or active send. Neither can lend a second writer.
            Backing::Fixed(home) => unsafe { Pin::new_unchecked(&mut *home.0.get()) },
            Backing::Shared(lease) => lease.get_mut(),
        }
    }
}
