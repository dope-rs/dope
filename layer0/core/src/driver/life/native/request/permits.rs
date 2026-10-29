use dope_sys::{engine::admit, io::file::File};

use crate::{
    Backend, Engine,
    driver::{
        life::native::{host::Host, request, request::bound},
        retained::flight,
        route,
        route::kind,
    },
};

type Guard<'a> = <Engine as admit::Admit<Host<Backend>>>::Admission<'a>;

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

    pub(crate) fn submit<'owner, C: kind::Class>(
        self,
        bound: bound::Bound<'owner, 'd, request::Lowered, C>,
    ) -> flight::Flight<'d, C>
    where
        'd: 'owner,
    {
        let (raw, reserve) = bound.into_parts();
        admit::Guard::<Host<Backend>>::submit(
            self.native,
            raw.into_inner().into_inner(),
            reserve.key(),
        );
        reserve.commit()
    }

    pub(crate) fn retain<'borrow, 'owner, C: kind::Class>(
        self,
        bound: bound::Bound<'owner, 'd, request::Borrowed<'borrow>, C>,
    ) -> flight::Flight<'d, C>
    where
        'owner: 'borrow,
        'd: 'owner,
    {
        let (raw, reserve) = bound.into_parts();
        // SAFETY: Captured retains every encoded pointee through terminal completion
        // or quiescence. Native submission precedes the host flight's commitment.
        unsafe {
            admit::raw::Retain::<Host<Backend>>::retain(
                self.native,
                raw.into_inner().into_inner(),
                reserve.key(),
            )
        };
        reserve.commit()
    }

    pub(crate) fn file<'borrow, 'owner: 'borrow, C: kind::Class>(
        self,
        bound: bound::Bound<'owner, 'd, <Backend as File>::Request<'borrow>, C>,
    ) -> flight::Flight<'d, C>
    where
        'd: 'owner,
    {
        let (raw, reserve) = bound.into_parts();
        // SAFETY: Captured retains the exact file, path and buffer borrows through
        // terminal completion or quiescence, before this flight is committed.
        unsafe {
            admit::raw::Retain::<Host<Backend>>::file(self.native, raw.into_inner(), reserve.key())
        };
        reserve.commit()
    }
}
