use crate::{
    io::reactor::{
        complete,
        ready::store::{Context, Exec, State},
    },
    table::change,
};

/// Host completion representation retained by a readiness backend owner.
pub trait Driver {
    type Map: complete::Map;
    type Observation: Copy + Eq;
    type Context: Exec;
    type State: State<Self::Context>;
    type Queue: complete::Queue<Event = <Self::Context as Exec>::Event>;

    fn shutdown() -> <<Self::Context as Context>::Slots as change::raw::Slots>::Data;
}
