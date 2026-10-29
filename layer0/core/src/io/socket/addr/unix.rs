use std::{os::unix, path};

use dope_sys::io::socket::{Domain, addr};

use crate::io::socket::{Addr, Error, Prepared, StreamSpec, addr::prepared};

/// A compact, validated Unix-domain socket address.
#[derive(Clone, Copy)]
pub struct Unix {
    native: addr::Unix,
}

impl Unix {
    pub fn from_path(path: &path::Path) -> Result<Self, Error> {
        let bytes = unix::ffi::OsStrExt::as_bytes(path.as_os_str());
        Ok(Self {
            native: addr::Unix::from_path::<crate::Backend>(bytes).map_err(Error::from_path)?,
        })
    }
}

impl prepared::Sealed for Unix {}

impl Prepared for Unix {
    fn view(&self) -> prepared::View<'_> {
        prepared::View(self.native.as_addr())
    }

    fn stream_spec(&self) -> StreamSpec {
        StreamSpec(Domain::Unix)
    }
}

const _: () = {
    assert!(size_of::<addr::Unix>() == size_of::<Unix>());
    assert!(size_of::<Unix>() < size_of::<Addr>());
};
