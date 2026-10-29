use crate::{
    io::{
        reactor::ready::delivery::entry::{Effects, Key, Physical, Slots},
        socket,
    },
    table,
    table::change,
};

/// Host descriptor-domain effects used by native socket completion policy.
pub trait Socket<'q, 'd, B>: Effects<'q, 'd, B> {
    fn creation(driver: Self::Driver, target: Self::Target) -> Option<Self::Creation>;
    fn reject(
        key: Key<Self>,
        queued: Physical<Self>,
        resolved: Option<Physical<Self>>,
        target: Self::Target,
    ) -> Self::Error;
    fn bad_install(
        key: Key<Self>,
        slot: Physical<Self>,
        target: Self::Target,
        cause: table::Install<<Slots<Self> as change::raw::Slots>::Index>,
    ) -> Self::Error;
    fn created(
        driver: Self::Driver,
        target: Self::Target,
        creation: Self::Creation,
    ) -> Self::Output;
    fn failed(
        driver: Self::Driver,
        target: Self::Target,
        creation: Self::Creation,
        error: socket::Create,
    ) -> Self::Output;
    fn fail_create(driver: Self::Driver, creation: Self::Creation);
    fn complete(driver: Self::Driver, creation: Self::Creation) -> Option<Self::Outbound>;
    fn discard(driver: Self::Driver, slot: Self::Outbound);
}
