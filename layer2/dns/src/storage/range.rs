use o3::base::time::{Nanos, Unix};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Range {
    pub(crate) start: Unix<Nanos>,
    pub(crate) done: Unix<Nanos>,
}

impl Range {
    pub(crate) const fn new(start: Unix<Nanos>, done: Unix<Nanos>) -> Self {
        Self { start, done }
    }

    pub(crate) const fn at(stamp: Unix<Nanos>) -> Self {
        Self::new(stamp, stamp)
    }
}
