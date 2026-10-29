use std::mem;

use o3::buffer::resident;

use crate::app::receive::{
    Borrowed, Coupled, Failure, Gated, GatedScoped, Outcome, Owned, Resources, Scoped,
};

const _: () = assert!(mem::size_of::<Outcome>() == mem::size_of::<Failure>());
const _: () = assert!(
    mem::size_of::<Borrowed>() == 0
        && mem::size_of::<Coupled>() == 0
        && mem::size_of::<Gated>() == 0
        && mem::size_of::<Scoped>() == 0
        && mem::size_of::<GatedScoped>() == 0
        && mem::size_of::<Owned>() == 0
        && mem::size_of::<<Borrowed as Resources>::Authority<'static>>() == 0
        && mem::size_of::<<Coupled as Resources>::Authority<'static>>() == 0
        && mem::size_of::<<Gated as Resources>::Authority<'static>>() == 0
        && mem::size_of::<<Owned as Resources>::Authority<'static>>() == 0
        && mem::size_of::<<Scoped as Resources>::Authority<'static>>()
            == mem::size_of::<resident::Budget<'static>>()
        && mem::size_of::<<GatedScoped as Resources>::Authority<'static>>()
            == mem::size_of::<resident::Budget<'static>>()
);
