use crate::io::{
    fd::handles,
    socket::{self, msg},
    transfer,
};

pub(crate) struct Send<'a> {
    pub(crate) slot: &'a handles::FixedSlot,
    pub(crate) buffer: &'a [u8],
    pub(crate) len: transfer::Len,
}

pub(crate) struct SendMsg<'a> {
    pub(crate) slot: &'a handles::FixedSlot,
    pub(crate) message: msg::Message<'a>,
}

pub(crate) struct AcceptOneshot<'a> {
    pub(crate) listener: &'a handles::FixedSlot,
    pub(crate) peer: &'a mut socket::raw::Addr,
}

pub(crate) struct AcceptMultishot<'a> {
    pub(crate) listener: &'a handles::FixedSlot,
}

pub(crate) struct Recv<'a> {
    pub(crate) slot: &'a handles::FixedSlot,
}

pub(crate) struct RecvMsgMulti<'a> {
    pub(crate) slot: &'a handles::FixedSlot,
}

pub(crate) struct Connect<'a> {
    pub(crate) slot: handles::FixedSlot,
    pub(crate) addr: &'a socket::Addr,
}

pub(crate) struct Socket<'a> {
    pub(crate) slot: &'a handles::FixedSlot,
    pub(crate) socket: socket::StreamSpec,
}
