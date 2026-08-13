use std::cell;

use crate::link::egress::metadata::pool::indices;

pub(super) struct State<'d> {
    pub(super) head: cell::Cell<indices::LinkedIndex<'d>>,
    pub(super) tail: cell::Cell<indices::LinkedIndex<'d>>,
    pub(super) len: cell::Cell<usize>,
    pub(super) bytes: cell::Cell<usize>,
    pub(super) resident: cell::Cell<usize>,
}

impl State<'_> {
    pub(super) fn new() -> Self {
        use std::cell::Cell;

        Self {
            head: Cell::new(indices::LinkedIndex::NONE),
            tail: Cell::new(indices::LinkedIndex::NONE),
            len: Cell::new(0),
            bytes: Cell::new(0),
            resident: Cell::new(0),
        }
    }
}
