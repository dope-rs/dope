use std::{marker, os::fd};

use dope_sys::{
    Errno,
    io::{
        reactor::ready::delivery::{entry, entry::Event as _},
        request,
        socket::Prepare,
    },
    memory::buffer,
};
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

use crate::{
    driver::{
        life::native::host::{Done, Received, poll::events},
        retained::flight,
    },
    io::transfer,
};

#[derive(Debug)]
pub(crate) struct File<N>(flight::raw::Echo, marker::PhantomData<N>);

impl<N> File<N> {
    pub(crate) const fn new(key: flight::raw::Echo) -> Self {
        Self(key, marker::PhantomData)
    }
}

impl<N: buffer::Source + entry::Source + Prepare> request::Complete for File<N> {
    type Output = Done<N>;

    fn opened(self, result: Result<fd::OwnedFd, Errno>) -> Self::Output {
        <Done<N> as entry::Event<events::Context<N>, Received<N>>>::opened(self.0, result)
    }

    fn write(self, result: Result<bounded::U32<0, { MAX as u32 }>, Errno>) -> Self::Output {
        let result = result.map(transfer::Len::from_bound);
        Done::<N>::written(self.0, result)
    }

    fn sync(self, result: Result<(), Errno>) -> Self::Output {
        Done::<N>::synced(self.0, result)
    }
}
