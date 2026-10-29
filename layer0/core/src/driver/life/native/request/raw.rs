use std::mem;

use crate::{
    driver::{
        life::native::request::{Borrowed, bound},
        retained, route,
        route::kind,
    },
    io::fd::handles,
};

/// A submission whose borrowed resources are retained through terminal
/// completion or quiescence.
pub(crate) struct Submission<
    'borrow,
    'owner: 'borrow,
    'd: 'owner,
    Tag: route::Tag,
    C: kind::Class = kind::Any,
> {
    pub(crate) submission: bound::Captured<'owner, 'd, Borrowed<'borrow>>,
    pub(crate) target: route::Operation<'d, Tag, C>,
}

impl<'borrow, 'owner: 'borrow, 'd: 'owner, Tag: route::Tag, C: kind::Class>
    Submission<'borrow, 'owner, 'd, Tag, C>
{
    /// # Safety
    /// Captured storage belongs to `token` through completion or quiescence.
    pub(crate) unsafe fn bind(
        token: retained::raw::Token<'owner, 'd>,
        entry: retained::raw::Submission<'borrow, 'd, Tag, C>,
    ) -> Self {
        let (entry, target) = entry.into_parts();
        Self {
            submission: bound::Captured::new(entry, token),
            target,
        }
    }
}

/// A retained connect, distinct from unrelated raw submissions accepted by a
/// tuning transaction.
pub(crate) struct Connect<'borrow, 'owner: 'borrow, 'd: 'owner, Tag: route::Tag> {
    submission: bound::Captured<'owner, 'd, Borrowed<'borrow>>,
    target: route::Target<'d, Tag>,
}

impl<'borrow, 'owner: 'borrow, 'd: 'owner, Tag: route::Tag> Connect<'borrow, 'owner, 'd, Tag> {
    /// # Safety
    /// Captured address storage belongs to `context` through completion or
    /// quiescence.
    pub(crate) unsafe fn bind(
        ctx: &retained::Context<'_, 'owner, 'd>,
        req: retained::raw::Connect<'borrow, 'd, Tag>,
    ) -> (handles::Descriptor<'d>, Self) {
        let (fd, entry, target) = req.into_parts();
        let bound = Self {
            submission: bound::Captured::new(entry, ctx.token()),
            target,
        };
        (fd, bound)
    }

    pub(in crate::driver::life::native::request) fn into_parts(
        self,
    ) -> (
        bound::Captured<'owner, 'd, Borrowed<'borrow>>,
        route::Target<'d, Tag>,
    ) {
        (self.submission, self.target)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Submission<'static, 'static, 'static, route::KeyTag<1>>>()
            == mem::size_of::<retained::raw::Submission<'static, 'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::align_of::<Submission<'static, 'static, 'static, route::KeyTag<1>>>()
            == mem::align_of::<retained::raw::Submission<'static, 'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::size_of::<bound::Captured<'static, 'static, Borrowed<'static>>>()
            == mem::size_of::<Borrowed<'static>>()
    );
};
const _: () = {
    assert!(
        mem::size_of::<Connect<'static, 'static, 'static, route::KeyTag<1>>>()
            + mem::size_of::<handles::Descriptor<'static>>()
            == mem::size_of::<retained::raw::Connect<'static, 'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::align_of::<Connect<'static, 'static, 'static, route::KeyTag<1>>>()
            == mem::align_of::<retained::raw::Connect<'static, 'static, route::KeyTag<1>>>()
    );
};
