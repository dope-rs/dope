use std::mem;

use dope_core::io::Event;

mod idle;
pub(super) mod modes;
mod parked;
pub(crate) mod ready;
pub(crate) mod routes;
pub(crate) mod scope;

pub(crate) use idle::Idle;
pub(crate) use parked::{Parked, Status};

const _: () = {
    assert!(mem::size_of::<Option<Event<'static>>>() == mem::size_of::<Event<'static>>());
    assert!(mem::align_of::<Option<Event<'static>>>() == mem::align_of::<Event<'static>>());
};

pub(crate) fn shutdown(source: &impl crate::executor::Startup) -> impl FnMut() -> Status {
    move || {
        source.observed(crate::shutdown::Requested::new());
        Status::Shutdown
    }
}
