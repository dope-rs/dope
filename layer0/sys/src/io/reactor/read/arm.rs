use libc::socklen_t;

use crate::{
    io::{
        reactor::{
            complete,
            read::{Complete, Context, Enabled, Family},
        },
        request::Datagram,
    },
    table,
    table::{change, registry},
};

/// Registers and retires typed reads while borrowing their native owners.
pub trait Arm<'a, C: Context, E: Complete<Key = C::Key, Flow = C::Flow>, P>: Sized {
    type Files: table::Files<C::Slots>;
    type Entry: super::raw::Entry<
            C,
            File = <<Self::Files as table::Files<C::Slots>>::Changes as change::Changes>::Id,
        >;
    type Registry: registry::State<C::Slots, Self::Entry, Files = Self::Files>;
    type Failure;
    type Prepared: complete::Cancellation<Event = E>;

    fn new(files: &'a mut Self::Files, reads: &'a mut Self::Registry, poll: &'a mut P) -> Self;
    fn accept_one(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        addr: &mut libc::sockaddr_storage,
        len: &mut socklen_t,
        vacant: impl complete::Vacancy<Event = E>,
    );
    fn accept_many(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        vacant: impl complete::Vacancy<Event = E>,
    );
    fn recv(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        vacant: impl complete::Vacancy<Event = E>,
    );
    fn message(
        &mut self,
        key: C::Key,
        message: impl Datagram<Target = <C::Slots as change::raw::Slots>::Physical>,
        vacant: impl complete::Vacancy<Event = E>,
    );
    fn prepare(self, key: C::Key, family: Family) -> Result<Option<Self::Prepared>, Self::Failure>;
    fn enable(&mut self, file: <Self::Entry as registry::Entry>::File, key: C::Key) -> Enabled;
    fn finish(&mut self, id: <Self::Entry as registry::Entry>::Id);
}
