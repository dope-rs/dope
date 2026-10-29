use crate::{
    io::reactor::{ready, retry},
    memory::buffer,
    table,
    table::{change, registry},
};

/// Exact native owners composing one readiness engine.
pub trait Context {
    type Slots: table::raw::Slots;
    type Files: table::Files<Self::Slots>;
    type Read: registry::Entry<
            File = <<Self::Files as table::Files<Self::Slots>>::Changes as change::Changes>::Id,
        >;
    type Reads: registry::State<Self::Slots, Self::Read, Files = Self::Files>;
    type Pending: retry::Entry<
            File = <<Self::Files as table::Files<Self::Slots>>::Changes as change::Changes>::Id,
        >;
    type Writes: retry::State<Self::Pending>;
    type Poll: ready::Poll<Self::Slots, Files = Self::Files>;
    type Pool: buffer::Pool;
}
