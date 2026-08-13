use std::os::fd;

use crate::file;

/// A confined descriptor proven to name a regular file, together with metadata
/// read from that exact descriptor.
///
/// The private fields make the proof non-forgeable outside the file manifold:
///
/// ```compile_fail
/// use dope_manifold::file::{Metadata, Regular};
///
/// fn forge(fd: std::os::fd::OwnedFd, metadata: Metadata) -> Regular {
///     Regular { fd, metadata }
/// }
/// ```
#[derive(Debug)]
pub struct Regular {
    fd: fd::OwnedFd,
    metadata: file::Metadata,
}

impl Regular {
    pub(in crate::file) const fn verified(fd: fd::OwnedFd, metadata: file::Metadata) -> Self {
        Self { fd, metadata }
    }

    pub const fn metadata(&self) -> file::Metadata {
        self.metadata
    }
}

impl fd::AsFd for Regular {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.fd.as_fd()
    }
}
