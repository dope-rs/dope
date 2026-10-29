use crate::table::change::Filter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Index<I> {
    index: I,
    filter: Filter,
}

impl<I> Index<I> {
    pub fn new(index: I, filter: Filter) -> Self {
        Self { index, filter }
    }

    pub fn into_parts(self) -> (I, Filter) {
        (self.index, self.filter)
    }
}
