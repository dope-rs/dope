use core::{fmt, hash, marker, mem};

use crate::driver::route;

/// One operation identity branded by its driver lifetime and route.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Operation<'d, Tag: route::Tag> {
    token: route::Token,
    driver: route::Brand<'d, Tag>,
}

impl<'d, Tag: route::Tag> Operation<'d, Tag> {
    pub(super) const fn new(token: route::Token) -> Self {
        Self {
            token,
            driver: marker::PhantomData,
        }
    }

    pub const fn slot(self) -> route::SlotIndex {
        self.token.slot()
    }

    pub const fn kind(self) -> u8 {
        self.token.kind()
    }

    pub fn matches(self, token: route::Token) -> bool {
        self.token == token
    }

    pub const fn with_kind(self, kind: u8) -> Self {
        Self::new(self.token.with_kind(kind))
    }

    #[doc(hidden)]
    pub const fn erase(self) -> route::Erased<'d> {
        route::Erased::new(self.token)
    }

    pub(crate) const fn into_token(self) -> route::Token {
        self.token
    }
}

impl<Tag: route::Tag> PartialEq for Operation<'_, Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl<Tag: route::Tag> Eq for Operation<'_, Tag> {}

impl<Tag: route::Tag> hash::Hash for Operation<'_, Tag> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.token.hash(state);
    }
}

impl<Tag: route::Tag> fmt::Debug for Operation<'_, Tag> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Operation")
            .field("route", &self.token.route())
            .field("kind", &self.token.kind())
            .field("slot", &self.token.slot())
            .field("epoch", &self.token.epoch())
            .finish()
    }
}

impl<Tag: route::Tag> route::Private for Operation<'_, Tag> {}

impl<'d, Tag: route::Tag> route::Credit<'d> for Operation<'d, Tag> {
    fn into_credit_token(self) -> route::Token {
        self.token
    }
}

const _: () = {
    assert!(
        mem::size_of::<Operation<'static, route::KeyTag<1>>>() == mem::size_of::<route::Token>()
    );
    assert!(
        mem::align_of::<Operation<'static, route::KeyTag<1>>>() == mem::align_of::<route::Token>()
    );
};
