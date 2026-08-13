use std::{fmt, net};

use dope::manifold::service;
use o3::collections::fixed::array;

pub(crate) struct AddressCount<const N: usize>;

impl<const N: usize> AddressCount<N> {
    pub(crate) const VALID: () = {
        assert!(N != 0, "DNS address capacity must be nonzero");
        assert!(
            N <= service::MAX_ENDPOINTS,
            "DNS address capacity exceeds the service endpoint ceiling"
        );
    };
}

#[repr(transparent)]
pub(crate) struct Addresses<const N: usize> {
    values: array::CopyInline<net::IpAddr, N>,
}

impl<const N: usize> Addresses<N> {
    pub(crate) fn new() -> Self {
        Self {
            values: array::CopyInline::new(),
        }
    }

    pub(crate) fn singleton(value: net::IpAddr) -> Self {
        let () = AddressCount::<N>::VALID;
        Self {
            values: array::CopyInline::from_fn(1, |_| value),
        }
    }

    pub(crate) fn try_insert_unique(&mut self, value: net::IpAddr) -> Result<bool, net::IpAddr> {
        if self.values.as_slice().contains(&value) {
            return Ok(false);
        }
        self.values.push(value).map(|()| true)
    }

    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn as_slice(&self) -> &[net::IpAddr] {
        self.values.as_slice()
    }
}

impl<const N: usize> fmt::Debug for Addresses<N> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.as_slice()).finish()
    }
}

impl<const N: usize> IntoIterator for Addresses<N> {
    type Item = net::IpAddr;
    type IntoIter = array::IntoIter<net::IpAddr, N>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}
