use std::mem;

use crate::{
    backend,
    driver::{flight, route},
};

pub(crate) struct Bound<'owner, 'd: 'owner, R = backend::RawSubmission> {
    raw: backend::Captured<'owner, R>,
    reservation: flight::Reservation<'d>,
}

const _: () = {
    type Parts = (backend::RawSubmission, flight::Reservation<'static>);
    assert!(mem::size_of::<Bound<'static, 'static>>() == mem::size_of::<Parts>());
    assert!(mem::align_of::<Bound<'static, 'static>>() == mem::align_of::<Parts>());
};

impl<'owner, 'd: 'owner, R> Bound<'owner, 'd, R> {
    fn new(raw: backend::Captured<'owner, R>, reservation: flight::Reservation<'d>) -> Self {
        Self { raw, reservation }
    }

    pub(in crate::backend) fn into_parts(self) -> (R, flight::Reservation<'d>) {
        (self.raw.into_inner(), self.reservation)
    }

    pub(crate) fn reserve_retained<Tag: route::Tag>(
        raw: backend::Captured<'owner, R>,
        target: route::Operation<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
    ) -> Option<Self> {
        Some(Self::new(raw, slots.reserve(target)?))
    }
}

impl<'d> Bound<'d, 'd> {
    pub(crate) fn reserve<Tag: route::Tag>(
        raw: backend::RawSubmission,
        target: route::Operation<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
    ) -> Option<Self> {
        Self::reserve_retained(backend::Captured::scoped(raw), target, slots)
    }
}
