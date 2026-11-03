mod budget;
pub mod cancel;
pub mod event;
mod failure;
pub mod slots;
pub mod socket;

pub use budget::Budget;
use dope_kqueue::Kqueue;
use dope_sys::{io::reactor::ready, table::Source};
pub use failure::Failure;

use crate::common::slots::Slots;

pub type Poll = <Kqueue as ready::Source>::Poll<Slots, Failure>;
pub type Files = <Kqueue as Source>::Files<Slots>;

pub extern "C" fn caught(_: libc::c_int) {}

#[derive(Debug)]
pub enum DirectoryError {
    Path(std::ffi::NulError),
    Syscall(dope_sys::Errno),
}

pub struct Directory(std::ffi::CString, o3::base::marker::ThreadBound);

impl Directory {
    pub fn try_new(base: &std::path::Path, tag: &str, nonce: u128) -> Result<Self, DirectoryError> {
        use dope_sys::io::file::{File as _, Mode};
        use std::os::unix::ffi::OsStrExt as _;

        let path = base.join(format!("dk-{tag}-{}-{nonce}", std::process::id()));
        let encoded =
            std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(DirectoryError::Path)?;
        dope_kqueue::Kqueue::mkdir::<dope_sys::Errno>(&encoded, Mode::new(0o700))
            .map_err(DirectoryError::Syscall)?;
        Ok(Self(encoded, o3::base::marker::ThreadBound::NEW))
    }

    pub fn path(&self) -> &std::path::Path {
        use std::os::unix::ffi::OsStrExt as _;

        std::path::Path::new(std::ffi::OsStr::from_bytes(self.0.to_bytes()))
    }

    pub fn c_path(&self) -> &std::ffi::CStr {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        use dope_sys::io::file::File as _;

        dope_kqueue::Kqueue::remove_dir_at::<dope_sys::Errno>(None, &self.0)
            .expect("fixture directory cleanup");
    }
}
