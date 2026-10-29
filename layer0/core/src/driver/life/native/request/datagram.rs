use std::{marker, mem};

use crate::{
    driver::life::native::{request, request::bound},
    io::fd::handles,
};

#[must_use]
#[repr(transparent)]
pub(crate) struct Datagram<'d> {
    raw: request::Lowered,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Datagram<'d> {
    pub(crate) fn new(fd: &handles::Datagram<'d>) -> Self {
        let raw = request::Lowered::recv_msg(fd);
        Self {
            raw,
            driver: marker::PhantomData,
        }
    }

    pub(in crate::driver::life::native::request) fn into_inner(self) -> request::Lowered {
        self.raw
    }
}

const _: () = {
    assert!(mem::size_of::<Datagram<'static>>() == mem::size_of::<request::Lowered>());
    assert!(mem::align_of::<Datagram<'static>>() == mem::align_of::<request::Lowered>());
    assert!(
        mem::size_of::<bound::Bound<'static, 'static, Datagram<'static>>>()
            == mem::size_of::<bound::Bound<'static, 'static>>()
    );
    assert!(
        mem::align_of::<bound::Bound<'static, 'static, Datagram<'static>>>()
            == mem::align_of::<bound::Bound<'static, 'static>>()
    );
};
