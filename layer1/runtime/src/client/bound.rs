use std::{marker, mem, ops};

/// Application and driver authority issued for one provider type.
/// The exact provider object is carried by [`super::Context`]; this brand alone
/// does not establish the ownership of an arbitrary attached value.
#[repr(transparent)]
pub struct Bound<'app, 'd: 'app, P: ?Sized, T = ()> {
    value: T,
    owner: marker::PhantomData<(*mut &'app (), *mut &'d (), *mut P)>,
}

impl<'app, 'd: 'app, P: ?Sized> Bound<'app, 'd, P> {
    pub(in crate::client) const fn new() -> Self {
        Self {
            value: (),
            owner: marker::PhantomData,
        }
    }

    pub const fn borrow<'a>(&'a self) -> Bound<'a, 'd, P>
    where
        'app: 'a,
    {
        Bound {
            value: (),
            owner: marker::PhantomData,
        }
    }

    pub const fn bind<T>(self, value: T) -> Bound<'app, 'd, P, T> {
        Bound {
            value,
            owner: self.owner,
        }
    }
}

impl<P: ?Sized, T> ops::Deref for Bound<'_, '_, P, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<P: ?Sized, T> ops::DerefMut for Bound<'_, '_, P, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<P: ?Sized, T: Copy> Copy for Bound<'_, '_, P, T> {}
impl<P: ?Sized, T: Copy> Clone for Bound<'_, '_, P, T> {
    fn clone(&self) -> Self {
        *self
    }
}

const _: () = {
    assert!(mem::size_of::<Bound<'static, 'static, ()>>() == 0);
    assert!(mem::align_of::<Bound<'static, 'static, ()>>() == 1);
    assert!(!mem::needs_drop::<Bound<'static, 'static, ()>>());
    assert!(mem::size_of::<Bound<'static, 'static, (), usize>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Bound<'static, 'static, (), usize>>() == mem::align_of::<usize>());
    assert!(
        mem::size_of::<Option<Bound<'static, 'static, (), std::num::NonZero<usize>>>>()
            == mem::size_of::<usize>()
    );
    assert!(mem::needs_drop::<Bound<'static, 'static, (), String>>());
};
