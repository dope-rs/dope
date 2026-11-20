use dope_sys::engine::admit;

use crate::{
    Backend, Engine,
    driver::{
        life::native::{host::Host, request, request::bound},
        retained::flight,
        route,
        route::kind,
    },
};

pub(in crate::driver) type Guard<'a> = <Engine as admit::Admit<Host<Backend>>>::Admission<'a>;

/// Native reserved capacity bound to the exact driver that owns its operation.
#[repr(transparent)]
pub(crate) struct Admission<'a, 'd> {
    native: Guard<'a>,
    driver: route::Brand<'d, Engine>,
}
impl<'a, 'd> Admission<'a, 'd> {
    pub(crate) fn new(native: Guard<'a>, driver: route::Brand<'d, Engine>) -> Self {
        const {
            assert!(std::mem::size_of::<Self>() == std::mem::size_of::<Guard<'a>>());
            assert!(std::mem::align_of::<Self>() == std::mem::align_of::<Guard<'a>>());
        }
        Self { native, driver }
    }

    pub(crate) fn submit<C: kind::Class>(
        self,
        bound: bound::Bound<'d, request::Lowered, C>,
    ) -> flight::Flight<'d, C> {
        let (raw, reserve) = bound.into_parts();
        admit::Guard::<Host<Backend>>::submit(self.native, raw.into_inner(), reserve.key());
        reserve.commit()
    }

    pub(in crate::driver) fn retain<C: kind::Class, F: FnOnce(Guard<'a>, flight::raw::Echo)>(
        self,
        bound: bound::Bound<'d, F, C>,
    ) -> flight::Flight<'d, C> {
        let (submit, reserve) = bound.into_parts();
        submit(self.native, reserve.key());
        reserve.commit()
    }
}
