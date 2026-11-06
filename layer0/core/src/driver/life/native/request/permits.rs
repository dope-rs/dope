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

    pub(crate) fn submit<C: kind::Class>(
        self,
        bound: bound::Bound<'d, request::Lowered, C>,
    ) -> flight::Flight<'d, C> {
        let (raw, reserve) = bound.into_parts();
        admit::Guard::<Host<Backend>>::submit(self.native, raw.into_inner(), reserve.key());
        reserve.commit()
    }

    /// # Safety
    /// Encoded handles and the reservation belong to this driver. Keep encoded
    /// addresses valid with their borrowed access through completion or quiescence.
    pub(crate) unsafe fn retain<'borrow, C: kind::Class>(
        self,
        bound: bound::Bound<'d, request::Borrowed<'borrow>, C>,
    ) -> flight::Flight<'d, C>
    where
        'd: 'borrow,
    {
        let (raw, reserve) = bound.into_parts();
        // SAFETY: the caller retains these exact native inputs. Submission
        // precedes commitment of the reserved flight.
        unsafe {
            admit::raw::Retain::<Host<Backend>>::retain(
                self.native,
                raw.into_inner(),
                reserve.key(),
            )
        };
        reserve.commit()
    }

    /// # Safety
    /// The reservation belongs to this driver. Retain every descriptor, path
    /// and buffer borrow through terminal completion or quiescence.
    pub(crate) unsafe fn file<'borrow, C: kind::Class>(
        self,
        bound: bound::Bound<'d, <Backend as File>::Request<'borrow>, C>,
    ) -> flight::Flight<'d, C>
    where
        'd: 'borrow,
    {
        let (raw, reserve) = bound.into_parts();
        // SAFETY: the caller retains these exact file borrows. Submission
        // precedes commitment of the reserved flight.
        unsafe { admit::raw::Retain::<Host<Backend>>::file(self.native, raw, reserve.key()) };
        reserve.commit()
    }
}
