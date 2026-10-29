use libc::socklen_t;

use crate::{
    Errno,
    io::reactor::{
        complete,
        read::{Arm, Call, Complete, Context, Family},
    },
    memory,
    memory::buffer,
    table::registry,
};

/// Native read records retaining the original descriptor and routed operation.
pub trait Entry<C: Context>:
    registry::Entry<
        Echo = C::Key,
        Family = Family,
        Call: Call<Key = C::Key, File = <Self as registry::Entry>::File>,
    >
{
    fn accept(
        file: Self::File,
        key: C::Key,
        addr: *mut libc::sockaddr_storage,
        len: *mut socklen_t,
        flow: C::Flow,
    ) -> Self;
    fn recv(file: Self::File, key: C::Key) -> Self;
    fn message(file: Self::File, key: C::Key) -> Self;
}

/// Drives one registered read while preserving its storage and completion borrows.
pub trait Poll<'a, C: Context, L: memory::raw::Layout, E: Complete<Key = C::Key, Flow = C::Flow>, P>:
    Sized
{
    type Arm: Arm<'a, C, E, P>;
    type Pool: buffer::Pool<Layout = L, Buffer = E::Buffer>;

    fn new(
        arm: Self::Arm,
        pool: &'a Self::Pool,
        query: registry::Query<<Self::Arm as Arm<'a, C, E, P>>::Entry>,
    ) -> Option<Self>;

    /// # Safety
    /// The registered read retains its submitted storage and admitted datagram layout.
    unsafe fn dispatch(
        self,
        filter: i16,
        error: Option<Errno>,
        vacant: impl complete::Vacancy<Event = E>,
    );

    /// # Safety
    /// The registered read retains its submitted storage and admitted datagram layout.
    unsafe fn resume(self, vacant: impl complete::Vacancy<Event = E>);
}
