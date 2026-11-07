use std::mem;

use crate::driver::{life::native::request, retained::flight, route, route::kind};

pub(crate) struct Bound<'d, R = request::Lowered, C: kind::Class = kind::Any> {
    raw: R,
    reservation: flight::Reserved<'d, C>,
}

impl<'d, R, C: kind::Class> Bound<'d, R, C> {
    pub(crate) fn new<Tag: route::Tag>(raw: R, vacancy: flight::Vacant<'_, 'd, Tag, C>) -> Self {
        Self {
            raw,
            reservation: vacancy.reserve(),
        }
    }

    pub(in crate::driver::life::native::request) fn into_parts(
        self,
    ) -> (R, flight::Reserved<'d, C>) {
        (self.raw, self.reservation)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Bound<'static>>()
            == mem::size_of::<(request::Lowered, flight::Reserved<'static>,)>()
    );
    assert!(
        mem::align_of::<Bound<'static>>()
            == mem::align_of::<(request::Lowered, flight::Reserved<'static>,)>()
    );
};
