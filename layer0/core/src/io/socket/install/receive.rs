use std::{marker, mem};

use crate::{driver, driver::settings, io::transfer};

/// Datagram receive capacity proved for one invariant driver generation.
pub(crate) struct Capacity<'d>(marker::PhantomData<driver::Reference<'d>>);

/// Datagram receive storage cannot hold one framed payload byte.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Error {
    actual: transfer::NonZero,
    required: transfer::NonZero,
}

impl<'d> Capacity<'d> {
    pub(in crate::io::socket::install) fn try_new(
        driver: driver::Reference<'d>,
    ) -> Result<Self, Error> {
        let actual = driver.receive().layout().buffer_len();
        let required = settings::Receive::MIN_DATAGRAM_SLOT;
        if actual.into_nonzero() < required.into_nonzero() {
            return Err(Error { actual, required });
        }
        Ok(Self(marker::PhantomData))
    }
}

impl Error {
    pub const fn actual(&self) -> transfer::NonZero {
        self.actual
    }

    pub const fn required(&self) -> transfer::NonZero {
        self.required
    }
}

const _: () = {
    assert!(mem::size_of::<Capacity<'static>>() == 0);
    assert!(mem::align_of::<Capacity<'static>>() == 1);
    assert!(mem::size_of::<Result<Capacity<'static>, Error>>() == mem::size_of::<Error>());
};
