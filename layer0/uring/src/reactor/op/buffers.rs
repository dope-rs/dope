use crate::reactor::{op, ring};

pub(crate) const RECEIVE: op::Code = op::Code::new(ring::ffi::RECV);

pub(crate) const SEND: op::Code = op::Code::new(ring::ffi::SEND);
