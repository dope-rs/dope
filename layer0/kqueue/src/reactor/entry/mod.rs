mod create;
pub(super) mod event;

use dope_sys::{
    io::reactor::{read, ready::delivery::entry, retry},
    table,
};

type Key<C> = <<C as entry::Context>::Reader as read::Context>::Key;
type Flow<C> = <<C as entry::Context>::Reader as read::Context>::Flow;
type Slots<C> = <<C as entry::Context>::Reader as read::Context>::Slots;
type Accepted<C> = <Slots<C> as table::raw::Slots>::Accepted;
type Bytes<C> = <<C as entry::Context>::Writer as retry::Context>::Outcome;
type Unit<C> = <<C as entry::Context>::Writer as retry::Context>::Unit;
