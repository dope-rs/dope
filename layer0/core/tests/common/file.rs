use std::{mem, path::Path};

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

#[derive(Debug)]
pub enum IdentityError {
    Path(fs::path::CopyError),
    Syscall(Errno),
}

impl Identity {
    pub fn try_new(path: &Path) -> Result<Self, IdentityError> {
        use fs::path::CStrExt as _;

        let path = Box::try_copy(path).map_err(IdentityError::Path)?;
        let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: path remains terminated and borrowed, and stat is writable.
        let result = unsafe { libc::stat(path.as_ptr(), stat.as_mut_ptr()) };
        // SAFETY: stat returns zero or -1 with this thread's unchanged errno.
        unsafe {
            dope_sys::kernel::raw::Status::new(result).try_zero::<Errno>(|| {
                dope_sys::kernel::raw::Code::last::<dope_core::Backend>().into()
            })
        }
        .map_err(IdentityError::Syscall)?;
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

pub trait Submit<'d> {
    /// # Safety
    /// The exact owner must retain every submitted resource through quiescence.
    unsafe fn submit<'borrow, Tag: route::Tag, C: kind::Flight>(
        &'borrow mut self,
        op: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<(), Error>
    where
        'd: 'borrow;
}

impl<'d> Submit<'d> for driver::Context<'_, 'd> {
    unsafe fn submit<'borrow, Tag: route::Tag, C: kind::Flight>(
        &'borrow mut self,
        op: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<(), Error>
    where
        'd: 'borrow,
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

pub fn cleanup(result: Result<(), dope_test::harness::support::file::Error>) {
    result.expect("fixture directory cleanup");
}
