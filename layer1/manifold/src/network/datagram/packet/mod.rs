use std::{cell, marker, num::NonZero};

use dope_core::io::{datagram, recv};
use o3::{
    base::marker::ThreadBound,
    buffer::{self, bytes::bounded, resident, storage::provided, write::consume},
};
use provided::{Frozen as _, account};

#[repr(transparent)]
pub struct Packet<'turn, 'd, T = recv::Lease<'d>> {
    view: T,
    turn: marker::PhantomData<(cell::Cell<&'turn ()>, &'d (), ThreadBound)>,
}

/// A removed prefix borrowing the original packet and its turn.
#[repr(transparent)]
pub struct Head<'a, 'turn, 'd, T = bounded::Value<provided::Prefix<'a, 'd>>> {
    view: T,
    turn: marker::PhantomData<&'a mut Packet<'turn, 'd>>,
}

#[repr(transparent)]
pub struct Frozen<'turn, 'd, T = recv::Shared<'d>> {
    view: T,
    turn: marker::PhantomData<(cell::Cell<&'turn ()>, &'d (), ThreadBound)>,
}

pub trait Backing {
    type Storage: buffer::Residency + ?Sized;

    fn backing(&self) -> &Self::Storage;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    Disabled,
    Account(account::Cause),
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Retainer<'d> {
    budget: Option<resident::Budget<'d>>,
}

impl<'turn, 'd> Packet<'turn, 'd> {
    pub(super) fn new(view: recv::Lease<'d>) -> Self {
        Self {
            view,
            turn: marker::PhantomData,
        }
    }

    pub(super) fn into_lease(self) -> recv::Lease<'d> {
        self.view
    }
}

impl<'a, 'd> Packet<'a, 'd, provided::Prefix<'a, 'd>> {
    /// Binds a provided segment to the original owner's borrow and driver.
    pub fn from_prefix(prefix: provided::Prefix<'a, 'd>) -> Self {
        Self {
            view: prefix,
            turn: marker::PhantomData,
        }
    }
}

impl<'turn, 'd, T: provided::Mutable<'d>> Packet<'turn, 'd, T> {
    pub fn len(&self) -> usize {
        self.view.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.view.as_ref().is_empty()
    }

    pub fn freeze(self) -> Frozen<'turn, 'd, T::Frozen> {
        let Self { view, turn } = self;
        Frozen {
            view: view.freeze(),
            turn,
        }
    }

    pub fn try_take(
        &mut self,
        amount: NonZero<usize>,
    ) -> Result<Head<'_, 'turn, 'd, T::Prefix<'_>>, consume::Error> {
        let view = self.view.try_take(amount)?;
        Ok(Head {
            view,
            turn: marker::PhantomData,
        })
    }
}

impl<'a, 'turn, 'd, T: provided::Mutable<'d>> Head<'a, 'turn, 'd, T> {
    pub fn freeze(self) -> Head<'a, 'turn, 'd, T::Frozen> {
        let Self { view, turn } = self;
        Head {
            view: view.freeze(),
            turn,
        }
    }
}

impl<'d> Retainer<'d> {
    pub(super) const fn new(budget: Option<resident::Budget<'d>>) -> Self {
        Self { budget }
    }

    pub(super) fn residency(&self) -> Option<datagram::Residency> {
        self.budget.as_ref().map(datagram::Residency::from)
    }

    /// Binds an external packet provider to one driver-branded budget.
    pub fn from_budget(budget: resident::Budget<'d>) -> Self {
        Self {
            budget: Some(budget),
        }
    }

    pub fn try_retain<P: Backing>(
        &self,
        packet: &P,
    ) -> Result<<P::Storage as provided::Frozen<'d>>::Account, Cause>
    where
        P::Storage: provided::Frozen<'d>,
    {
        let Some(budget) = self.budget else {
            return Err(Cause::Disabled);
        };
        packet
            .backing()
            .try_account(&budget)
            .map_err(Cause::Account)
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Packet<'_, '_, T> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl<T: AsMut<[u8]>> AsMut<[u8]> for Packet<'_, '_, T> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.view.as_mut()
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Head<'_, '_, '_, T> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl<T: AsMut<[u8]>> AsMut<[u8]> for Head<'_, '_, '_, T> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.view.as_mut()
    }
}

impl<T: buffer::Residency> Backing for Head<'_, '_, '_, T> {
    type Storage = T;

    fn backing(&self) -> &Self::Storage {
        &self.view
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Frozen<'_, '_, T> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl<T: buffer::Residency> Backing for Frozen<'_, '_, T> {
    type Storage = T;

    fn backing(&self) -> &Self::Storage {
        &self.view
    }
}

const _: () = {
    let word = size_of::<usize>();
    assert!(size_of::<Packet<'static, 'static>>() == 3 * word);
    assert!(size_of::<Packet<'static, 'static, provided::Prefix<'static, 'static>>>() == 3 * word);
    assert!(size_of::<Frozen<'static, 'static>>() == 3 * word);
    assert!(
        size_of::<Frozen<'static, 'static, provided::Prefix<'static, 'static, &'static [u8]>>>()
            == 3 * word
    );
    assert!(size_of::<Head<'static, 'static, 'static>>() == 3 * word);
    assert!(
        size_of::<
            Head<
                'static,
                'static,
                'static,
                bounded::Value<provided::Prefix<'static, 'static, &'static [u8]>>,
            >,
        >() == 3 * word
    );
    assert!(
        size_of::<Result<Head<'static, 'static, 'static>, consume::Error>>()
            == size_of::<Head<'static, 'static, 'static>>()
    );
    assert!(size_of::<Retainer<'static>>() == word);
    assert!(size_of::<Cause>() == 3 * word);
    assert!(size_of::<Option<Cause>>() == size_of::<Cause>());
};

mod raw;
