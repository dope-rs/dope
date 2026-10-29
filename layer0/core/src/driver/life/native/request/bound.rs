use std::mem;

use crate::driver::{life::native::request, retained, retained::flight, route, route::kind};

pub(crate) struct Bound<'owner, 'd: 'owner, R = request::Lowered, C: kind::Class = kind::Any> {
    raw: Captured<'owner, 'd, R>,
    reservation: flight::Reserved<'d, C>,
}

impl<'owner, 'd: 'owner, R, C: kind::Class> Bound<'owner, 'd, R, C> {
    pub(crate) fn new<Tag: route::Tag>(
        raw: Captured<'owner, 'd, R>,
        vacancy: flight::Vacant<'_, 'd, Tag, C>,
    ) -> Self {
        Self {
            raw,
            reservation: vacancy.reserve(),
        }
    }

    pub(in crate::driver::life::native::request) fn into_parts(
        self,
    ) -> (Captured<'owner, 'd, R>, flight::Reserved<'d, C>) {
        (self.raw, self.reservation)
    }
}

impl<'owner, 'd: 'owner, C: kind::Class> Bound<'owner, 'd, request::Datagram<'d>, C> {
    pub(crate) fn into_raw(self) -> Bound<'owner, 'd, request::Lowered, C> {
        Bound {
            raw: Captured::new(self.raw.raw.into_inner(), self.raw.token),
            reservation: self.reservation,
        }
    }
}

impl<'d, R, C: kind::Class> Bound<'d, 'd, R, C> {
    pub(crate) fn scoped<Tag: route::Tag>(raw: R, vacancy: flight::Vacant<'_, 'd, Tag, C>) -> Self {
        Self::new(Captured::scoped(raw), vacancy)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Bound<'static, 'static>>()
            == mem::size_of::<(request::Lowered, flight::Reserved<'static>,)>()
    );
    assert!(
        mem::align_of::<Bound<'static, 'static>>()
            == mem::align_of::<(request::Lowered, flight::Reserved<'static>,)>()
    );
};

/// Backend value whose erased resources stay tied to one retained owner.
#[must_use]
#[repr(transparent)]
pub(crate) struct Captured<'owner, 'd: 'owner, R> {
    raw: R,
    token: retained::raw::Token<'owner, 'd>,
}

impl<'owner, 'd: 'owner, R> Captured<'owner, 'd, R> {
    pub(crate) fn new(raw: R, token: retained::raw::Token<'owner, 'd>) -> Self {
        Self { raw, token }
    }

    pub(in crate::driver::life::native::request) fn into_inner(self) -> R {
        self.raw
    }
}

impl<'d, R> Captured<'d, 'd, R> {
    /// Binds an owned backend value whose Rust borrows already carry `'d`.
    pub(in crate::driver::life::native::request) fn scoped(raw: R) -> Self {
        Self::new(raw, retained::raw::Token::scoped())
    }
}

const _: () = {
    assert!(mem::size_of::<Captured<'static, 'static, usize>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Captured<'static, 'static, usize>>() == mem::align_of::<usize>());
};
