use dope_sys::io::socket::option::tuning::device::Reserve;
use o3::depot::arena::slab;

use crate::driver::{
    retained::flight,
    route::{self, kind},
};

/// Retains the route proof until the native queue admits its complete chain.
pub(crate) struct Request<'a, 'd, Tag: route::Tag, C: kind::Class> {
    slots: &'a flight::Slots<'d, Tag>,
    target: route::Operation<'d, Tag, C>,
}

impl<'a, 'd, Tag: route::Tag, C: kind::Class> Request<'a, 'd, Tag, C> {
    pub(crate) fn new(
        slots: &'a flight::Slots<'d, Tag>,
        target: route::Operation<'d, Tag, C>,
    ) -> Self {
        Self { slots, target }
    }
}

impl<'a, 'd, Tag: route::Tag, C: kind::Flight> Reserve for Request<'a, 'd, Tag, C> {
    type Key = flight::raw::Echo;
    type Vacant = flight::Vacant<'a, 'd, Tag, C>;
    type Reservation = flight::Reserved<'d, C>;
    type Error = slab::Vacancy;

    fn try_vacancy(self) -> Result<Self::Vacant, Self::Error> {
        self.slots.try_vacancy(self.target)
    }

    fn reserve(vacant: Self::Vacant) -> (Self::Key, Self::Reservation) {
        let target = vacant.reserve();
        (target.key(), target)
    }
}
