use std::{io, mem, path::Path};

use dope_core::{
    driver,
    driver::{retained, retained::flight, route, route::kind},
    io::{fs, socket::msg},
};
use dope_sys::io::socket::msg::{Header, Iovec};
use o3::depot::array;

use crate::common::contract::submit::Error;

pub struct Identity {
    device: u128,
    inode: u128,
}

impl Identity {
    pub fn try_new(path: &Path) -> io::Result<Self> {
        use std::os::unix::ffi::OsStrExt as _;

        let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: path remains terminated and borrowed, and stat is writable.
        if unsafe { libc::stat(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful stat initialized the complete value.
        let stat = unsafe { stat.assume_init() };
        Ok(Self {
            device: stat.st_dev as u128,
            inode: stat.st_ino as u128,
        })
    }
}

impl Inspect for Identity {
    fn contains(&self, fd: libc::c_int) -> bool {
        let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: stat points to writable storage and fstat initializes it on success.
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
            return false;
        }
        // SAFETY: successful fstat initialized the complete value.
        let stat = unsafe { stat.assume_init() };
        stat.st_dev as u128 == self.device && stat.st_ino as u128 == self.inode
    }
}

use dope_core::Errno;
use dope_test::harness::support::file::{Directory, Remove};

#[derive(Debug, PartialEq, Eq)]
pub struct Failure {
    pub errno: Errno,
}

impl From<Errno> for Failure {
    fn from(errno: Errno) -> Self {
        Self { errno }
    }
}

pub trait Retain<'a, 'd> {
    fn retained(self) -> retained::Context<'a, 'd, 'd>;
}

impl<'a, 'd> Retain<'a, 'd> for driver::Context<'a, 'd> {
    fn retained(self) -> retained::Context<'a, 'd, 'd> {
        let owner = retained::raw::Owner::from_driver(self.driver_ref());
        retained::Context::new(self, owner)
    }
}

pub trait Submit<'owner, 'd: 'owner> {
    /// # Safety
    /// The exact owner must retain every submitted resource through quiescence.
    unsafe fn submit<'borrow, Tag: route::Tag, C: kind::Flight>(
        &'borrow mut self,
        op: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<(), Error>
    where
        'owner: 'borrow;
}

impl<'owner, 'd: 'owner> Submit<'owner, 'd> for retained::Context<'_, 'owner, 'd> {
    unsafe fn submit<'borrow, Tag: route::Tag, C: kind::Flight>(
        &'borrow mut self,
        op: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<(), Error>
    where
        'owner: 'borrow,
    {
        let flights = self
            .storage
            .reborrow()
            .try_flights(flight::Need::<Tag>::new(flight::Capacity::fixed::<1>()))
            .map_err(Error::Allocation)?;
        let slots = flights.slots();
        // SAFETY: the caller retains every submitted resource under this exact
        // owner through quiescence, including returned failures.
        let flight = unsafe { retained::raw::Submission::try_file(self.reborrow(), &slots, op) }
            .map_err(Error::Submit)?;
        drop(flight);
        Ok(())
    }
}

pub trait NativePath {
    fn c_path(&self) -> Result<std::ffi::CString, std::ffi::NulError>;
}

impl<F: FnOnce(Result<(), Remove>)> NativePath for Directory<F> {
    fn c_path(&self) -> Result<std::ffi::CString, std::ffi::NulError> {
        use std::os::unix::ffi::OsStrExt as _;

        std::ffi::CString::new(self.path().as_os_str().as_bytes())
    }
}

use std::ops::Range;

/// A consuming fixture supplies its exact descriptor identity predicate.
pub trait Inspect {
    fn contains(&self, fd: std::ffi::c_int) -> bool;

    fn collect(&self, bounds: Range<std::ffi::c_int>) -> Vec<std::ffi::c_int> {
        bounds.filter(|fd| self.contains(*fd)).collect()
    }
}

pub struct Message {
    header: Header,
    iovecs: [Iovec; 5],
}

impl Default for Message {
    fn default() -> Self {
        Self {
            header: Header::new(),
            iovecs: [
                Iovec::empty(),
                Iovec::from_slice(b"ab"),
                Iovec::empty(),
                Iovec::from_slice(b"cdef"),
                Iovec::empty(),
            ],
        }
    }
}

impl Message {
    pub fn view(&mut self) -> msg::Vectored<'_, 5> {
        use dope_core::io::{socket::msg::raw::Vectored as _, transfer};

        // SAFETY: the five owned descriptors point to static bytes whose sum
        // is exactly six; this borrow fixes their header and storage in place.
        unsafe {
            (
                &mut self.header,
                &self.iovecs,
                array::Len::MAX,
                transfer::Len::fixed::<6>(),
            )
                .retain()
        }
    }
}
