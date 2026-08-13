use std::{marker, ops};

use dope_core::io::recv;
use o3::{buffer::resident, cell::region};

type Turn<'turn> = marker::PhantomData<fn(&'turn ()) -> &'turn ()>;

#[repr(transparent)]
pub struct Packet<'turn, 'd> {
    view: recv::View<'d>,
    turn: Turn<'turn>,
}

#[repr(transparent)]
pub struct Split<'turn, 'd> {
    view: recv::Unique<'d>,
    turn: Turn<'turn>,
}

#[repr(transparent)]
pub struct Frozen<'turn, 'd> {
    view: recv::Shared<'d>,
    turn: marker::PhantomData<&'turn ()>,
}

pub struct Retained<'d>(recv::Retained<'d>);

#[repr(transparent)]
pub(super) struct Retention<'d>(Option<resident::Budget<'d>>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Retainer<'turn, 'd> {
    retention: &'turn Retention<'d>,
}

impl<'turn, 'd> Packet<'turn, 'd> {
    pub(super) fn new(view: recv::View<'d>) -> Self {
        Self {
            view,
            turn: marker::PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.view.len()
    }

    pub fn is_empty(&self) -> bool {
        self.view.is_empty()
    }

    pub fn freeze(self) -> Frozen<'turn, 'd> {
        Frozen {
            view: self.view.into_shared(),
            turn: marker::PhantomData,
        }
    }

    pub fn into_split(self) -> Split<'turn, 'd> {
        Split {
            view: self.view.into_unique(),
            turn: marker::PhantomData,
        }
    }

    pub(super) fn into_view(self) -> recv::View<'d> {
        self.view
    }
}

impl<'turn, 'd> Split<'turn, 'd> {
    pub fn len(&self) -> usize {
        self.view.len()
    }

    pub fn is_empty(&self) -> bool {
        self.view.is_empty()
    }

    pub fn split_at(self, mid: usize) -> Result<(Self, Self), Self> {
        match self.view.split_at(mid) {
            Ok((head, tail)) => Ok((
                Self {
                    view: head,
                    turn: marker::PhantomData,
                },
                Self {
                    view: tail,
                    turn: marker::PhantomData,
                },
            )),
            Err(view) => Err(Self {
                view,
                turn: marker::PhantomData,
            }),
        }
    }

    pub fn freeze(self) -> Frozen<'turn, 'd> {
        Frozen {
            view: self.view.into_shared(),
            turn: marker::PhantomData,
        }
    }
}

impl Frozen<'_, '_> {
    pub fn len(&self) -> usize {
        self.view.len()
    }

    pub fn is_empty(&self) -> bool {
        self.view.is_empty()
    }

    pub fn resident_bytes(&self) -> usize {
        self.view.resident_bytes()
    }
}

impl<'d> Retained<'d> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn resident_bytes(&self) -> usize {
        self.0.resident_bytes()
    }

    pub fn get(&self, range: ops::Range<usize>) -> Option<Self> {
        self.0.get(range).map(Self)
    }

    pub fn into_range(self, range: ops::Range<usize>) -> Result<Self, Self> {
        self.0.into_range(range).map(Self).map_err(Self)
    }

    pub(super) fn into_inner(self) -> recv::Retained<'d> {
        self.0
    }

    pub(super) fn from_inner(inner: recv::Retained<'d>) -> Self {
        Self(inner)
    }
}

impl<'d> Retention<'d> {
    pub(super) fn new(capacity: usize, token: &region::Token<'d>) -> Self {
        Self((capacity != 0).then(|| resident::Budget::new(capacity, token)))
    }

    pub(super) fn retain<'turn>(
        &self,
        packet: Packet<'turn, 'd>,
    ) -> Result<Retained<'d>, Packet<'turn, 'd>> {
        let Some(budget) = &self.0 else {
            return Err(packet);
        };
        packet
            .view
            .try_into_retained(budget)
            .map(Retained)
            .map_err(Packet::new)
    }
}

impl<'turn, 'd> Retainer<'turn, 'd> {
    pub(super) fn new(retention: &'turn Retention<'d>) -> Self {
        Self { retention }
    }

    pub fn retain<'packet>(
        &self,
        packet: &Frozen<'packet, 'd>,
        range: ops::Range<usize>,
    ) -> Option<Retained<'d>> {
        let budget = self.retention.0.as_ref()?;
        packet.view.accounted(range, budget).map(Retained)
    }
}

impl AsRef<[u8]> for Packet<'_, '_> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl AsMut<[u8]> for Packet<'_, '_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.view.as_mut()
    }
}

impl AsRef<[u8]> for Split<'_, '_> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl AsMut<[u8]> for Split<'_, '_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.view.as_mut()
    }
}

impl AsRef<[u8]> for Frozen<'_, '_> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl AsRef<[u8]> for Retained<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
