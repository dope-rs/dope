use crate::reactor::{op, ring};

pub(crate) const RECEIVE: op::Code = op::Code::new(ring::ffi::RECVMSG);

pub(crate) const SEND: op::Code = op::Code::new(ring::ffi::SENDMSG);
