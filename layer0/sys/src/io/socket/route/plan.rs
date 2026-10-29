use std::mem;

use crate::io::socket::route::{count::Count, selector::Selector};

/// A selector bound to the dense routing domain it selects from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Plan {
    count: Count,
    selector: Selector,
}

impl Plan {
    pub const fn new(count: Count, selector: Selector) -> Self {
        Self { count, selector }
    }

    pub const fn count(self) -> Count {
        self.count
    }

    pub const fn selector(self) -> Selector {
        self.selector
    }
}

const _: () = {
    assert!(mem::size_of::<Plan>() == mem::size_of::<Count>() + mem::size_of::<Selector>());
};
