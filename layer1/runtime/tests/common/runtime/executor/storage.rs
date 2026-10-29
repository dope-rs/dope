use std::{cell::Cell, marker, ptr::NonNull, rc::Rc};

use dope_core::driver::storage;
use dope_manifold::{app::creation, client::attempt::queue};
use dope_net::tcp::Tcp;
use o3::depot::capacity;

pub struct Collision<'a> {
    pub capacity: capacity::Count,
    pub borrowed: &'a Cell<usize>,
    pub local: Rc<()>,
}

pub struct Failure<'a> {
    pub cause: creation::Error,
    pub borrowed: &'a Cell<usize>,
    pub local: Rc<()>,
}

impl storage::Plan for Collision<'_> {
    type Output<'p, 'd>
        = (queue::Source<'d, Tcp>, queue::Source<'d, Tcp>)
    where
        Self: 'd,
        'd: 'p;
}

pub struct Pinned<'a> {
    address: Cell<Option<NonNull<Self>>>,
    dropped: &'a Cell<Option<bool>>,
    _pin: marker::PhantomPinned,
}

impl<'a> Pinned<'a> {
    pub fn new(dropped: &'a Cell<Option<bool>>) -> Self {
        Self {
            address: Cell::new(None),
            dropped,
            _pin: marker::PhantomPinned,
        }
    }

    pub fn record(&self) {
        self.address.set(Some(NonNull::from(self)));
    }
}

impl Drop for Pinned<'_> {
    fn drop(&mut self) {
        let actual = NonNull::from(&mut *self);
        self.dropped.set(Some(self.address.get() == Some(actual)));
    }
}

impl<'a> storage::Create for Collision<'a> {
    type Error = Failure<'a>;

    fn try_build<'p, 'd>(
        self,
        mut context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        let Self {
            capacity,
            borrowed,
            local,
        } = self;
        let result = (|| {
            let first = queue::Source::try_new(capacity, &mut context.reborrow())?;
            let second = queue::Source::try_new(capacity, &mut context)?;
            Ok((first, second))
        })();
        result.map_err(|cause| Failure {
            cause,
            borrowed,
            local,
        })
    }
}
