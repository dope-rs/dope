use std::mem;

use crate::io::socket::route::{
    count::Count,
    selector::{Bootstrap, PrefixPlan, Selector},
};

/// A selector bound to the dense routing domain it selects from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Plan {
    count: Count,
    selector: Selector,
    prefix: Option<PrefixPlan>,
    bootstrap: Option<Bootstrap>,
}

impl Plan {
    pub const fn new(count: Count, selector: Selector) -> Self {
        Self {
            count,
            selector,
            prefix: None,
            bootstrap: None,
        }
    }

    pub const fn with_prefix(self, prefix: PrefixPlan) -> Self {
        Self {
            prefix: Some(prefix),
            ..self
        }
    }

    pub const fn with_bootstrap(self, bootstrap: Bootstrap) -> Self {
        Self {
            bootstrap: Some(bootstrap),
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

    pub const fn bootstrap(self) -> Option<Bootstrap> {
        self.bootstrap
    }
}

const _: () = {
    assert!(
        mem::size_of::<Plan>()
            == mem::size_of::<(Count, Selector, Option<PrefixPlan>, Option<Bootstrap>)>()
    );
};
