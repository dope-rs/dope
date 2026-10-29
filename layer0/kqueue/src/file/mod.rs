use std::os::fd;

use dope_sys::{
    Errno,
    io::{file, file::File as _},
    kernel,
};
use fd::AsRawFd as _;
pub(crate) use request::Request;

use crate::Kqueue;

mod request;

#[derive(Debug)]
pub struct Options {
    flags: file::Open,
    mode: file::Mode,
}

unsafe extern "C" {
    #[link_name = "fdatasync"]
    fn sync_data(file: libc::c_int) -> libc::c_int;
}

fn sync(file: fd::BorrowedFd<'_>, mode: file::Sync) -> Result<(), Errno> {
    match mode {
        file::Sync::Data => {
            // SAFETY: the borrowed descriptor remains live; fdatasync returns zero or -1.
            unsafe {
                kernel::raw::Status::new(sync_data(file.as_raw_fd()))
                    .try_zero(|| kernel::raw::Code::last::<Kqueue>().into())
            }
        }
        file::Sync::All => Kqueue::sync(file),
    }
}
