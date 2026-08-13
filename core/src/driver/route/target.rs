use core::{fmt, hash, marker, mem};

use crate::driver::route::{self, table};

/// One generation-checked target branded by its driver lifetime and route.
///
/// ```compile_fail
/// use dope_core::driver::route::{KeyTag, Target};
///
/// fn rebrand<'a, 'b>(target: Target<'a, KeyTag<1>>) -> Target<'b, KeyTag<1>> {
///     target
/// }
/// ```
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Target<'d, Tag: route::Tag> {
    parts: table::Parts<Tag>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, Tag: route::Tag> Target<'d, Tag> {
    pub(super) const fn from_parts(parts: table::Parts<Tag>) -> Self {
        Self {
            parts,
            driver: marker::PhantomData,
        }
    }

    pub const fn parts(self) -> table::Parts<Tag> {
        self.parts
    }

    pub const fn slot(self) -> route::SlotIndex {
        self.parts.slot()
    }

    pub const fn epoch(self) -> route::Epoch {
        self.parts.epoch()
    }

    pub const fn bind<T>(self, value: T) -> route::Bound<'d, Tag, T> {
        route::Bound::new(self, value)
    }

    pub const fn operation(self, kind: u8) -> route::Operation<'d, Tag> {
        route::Operation::new(route::Token::from_target(self).with_kind(kind))
    }

    pub const fn dispatch(self) -> route::Operation<'d, Tag> {
        self.operation(Tag::KIND)
    }
}

impl<Tag: route::Tag> PartialEq for Target<'_, Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.parts == other.parts
    }
}

impl<Tag: route::Tag> Eq for Target<'_, Tag> {}

impl<Tag: route::Tag> hash::Hash for Target<'_, Tag> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.parts.hash(state);
    }
}

impl<Tag: route::Tag> fmt::Debug for Target<'_, Tag> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Target")
            .field("route", &Tag::ROUTE)
            .field("kind", &Tag::KIND)
            .field("slot", &self.slot())
            .field("epoch", &self.epoch())
            .finish()
    }
}

const _: () = {
    assert!(mem::size_of::<Target<'static, route::KeyTag<1>>>() == mem::size_of::<route::Token>());
    assert!(
        mem::align_of::<Target<'static, route::KeyTag<1>>>() == mem::align_of::<route::Token>()
    );
};
