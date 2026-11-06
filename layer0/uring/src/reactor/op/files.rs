use crate::reactor::{op, ring};

pub(crate) const OPEN: op::Code = op::Code::new(ring::ffi::OPENAT2);

pub(crate) const METADATA: op::Code = op::Code::new(ring::ffi::STATX);

pub(crate) const READ: op::Code = op::Code::new(ring::ffi::READ);

pub(crate) const WRITE: op::Code = op::Code::new(ring::ffi::WRITE);

pub(crate) const SYNC: op::Code = op::Code::new(ring::ffi::FSYNC);

pub(crate) const RENAME: op::Code = op::Code::new(ring::ffi::RENAMEAT);

pub(crate) const UNLINK: op::Code = op::Code::new(ring::ffi::UNLINKAT);

pub(crate) const CLOSE: op::Code = op::Code::new(ring::ffi::CLOSE);
