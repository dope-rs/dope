use std::mem;

use dope_core::driver::route::table;

use crate::network::datagram::plan::Invalid;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::network::datagram) struct Pending(table::Capacity);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::network::datagram) struct Flights(table::Capacity);

impl Pending {
    pub(in crate::network::datagram) const fn into_table(self) -> table::Capacity {
        self.0
    }
}

impl Flights {
    pub(in crate::network::datagram) const fn into_table(self) -> table::Capacity {
        self.0
    }
}

fn validate(actual: usize) -> Result<table::Capacity, Invalid> {
    let capacity = table::Capacity::try_from(actual).map_err(Invalid::Route)?;
    if capacity.count().get() == 0 {
        return Err(Invalid::Zero);
    }
    Ok(capacity)
}

impl TryFrom<usize> for Pending {
    type Error = Invalid;

    fn try_from(actual: usize) -> Result<Self, Self::Error> {
        validate(actual).map(Self)
    }
}

impl TryFrom<usize> for Flights {
    type Error = Invalid;

    fn try_from(actual: usize) -> Result<Self, Self::Error> {
        validate(actual).map(Self)
    }
}

const _: () = {
    assert!(mem::size_of::<Pending>() == mem::size_of::<table::Capacity>());
    assert!(mem::align_of::<Pending>() == mem::align_of::<table::Capacity>());
    assert!(mem::size_of::<Flights>() == mem::size_of::<table::Capacity>());
    assert!(mem::align_of::<Flights>() == mem::align_of::<table::Capacity>());
};
