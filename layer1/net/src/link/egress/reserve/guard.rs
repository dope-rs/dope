use o3::{
    base::cell::region,
    mem::{Lease, Return, lease::Open},
};

pub(in crate::link::egress) trait Rollback<'d> {
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>);
}

pub(in crate::link::egress) struct Guard<'token, 'd, T: Rollback<'d>> {
    parts: Lease<Parts<'token, 'd, T>, Open>,
}

struct Parts<'token, 'd, T> {
    token: &'token mut region::Token<region::Identity<'d>>,
    value: T,
}

impl<'token, 'd, T: Rollback<'d>> Guard<'token, 'd, T> {
    pub(in crate::link::egress) const fn new(
        token: &'token mut region::Token<region::Identity<'d>>,
        value: T,
    ) -> Self {
        Self {
            parts: Lease::open(Parts { token, value }, ()),
        }
    }

    pub(in crate::link::egress) fn parts_mut(
        &mut self,
    ) -> (&mut region::Token<region::Identity<'d>>, &mut T) {
        let (parts, ()) = self.parts.parts_mut();
        (parts.token, &mut parts.value)
    }

    pub(in crate::link::egress) fn into_parts(
        self,
    ) -> (&'token mut region::Token<region::Identity<'d>>, T) {
        let (Parts { token, value }, ()) = self.parts.into_parts();
        (token, value)
    }
}

impl<'d, T: Rollback<'d>> Return for Parts<'_, 'd, T> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.value.rollback(self.token);
    }
}
