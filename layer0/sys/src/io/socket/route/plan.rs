use std::mem;

use crate::io::socket::route::{
    count::Count,
    selector::{PrefixPlan, Selector},
};

/// A selector bound to the dense routing domain it selects from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Plan {
    count: Count,
    selector: Selector,
    prefix: Option<PrefixPlan>,
}

impl Plan {
    pub const fn new(count: Count, selector: Selector) -> Self {
        Self {
            count,
            selector,
            prefix: None,
        }
    }

    pub const fn with_prefix(self, prefix: PrefixPlan) -> Self {
        Self {
            prefix: Some(prefix),
            ..self
        }
    }

    pub const fn count(self) -> Count {
        self.count
    }

    pub const fn selector(self) -> Selector {
        self.selector
    }

    pub const fn prefix(self) -> Option<PrefixPlan> {
        self.prefix
    }
}

const _: () = {
    assert!(
        mem::size_of::<Plan>()
            == mem::size_of::<Count>()
                + mem::size_of::<Selector>()
                + mem::size_of::<Option<PrefixPlan>>()
    );
};
