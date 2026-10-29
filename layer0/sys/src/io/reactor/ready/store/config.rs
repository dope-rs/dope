use o3::{depot, depot::map::Plan};

use crate::{
    io::reactor::{
        complete,
        ready::{Driver, store::Context},
    },
    memory::buffer,
    table,
    table::permit,
};

/// Exact admitted input proofs consumed by readiness runtime construction.
pub struct Build<C: Driver + ?Sized> {
    pub receive: <<C::Context as Context>::Pool as buffer::Pool>::Layout,
    pub slots: <<C::Context as Context>::Slots as table::raw::Slots>::Layout,
    pub plan: Plan,
    pub capacity: <C::Queue as complete::Queue>::Capacity,
}

/// Projects host settings without changing their native layout proofs.
pub trait Config<G>: Driver {
    type Key: permit::raw::Key;
    type Index: depot::raw::Compact;

    fn config(config: &G) -> Build<Self>;
}
