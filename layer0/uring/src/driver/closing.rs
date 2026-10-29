use dope_sys::{
    engine::ring::Context,
    io::{Close, Retry},
    ring::map::{Storage, Unmap},
};
use o3::mem::Rejected;

use crate::reactor::ring::Ready;

pub struct Closing<C: Context> {
    ring: <Ready<C::Token, C::Layout> as Close>::State,
    tuning: C::Tuning,
}

pub struct Pending<C: Context> {
    ring: <<Ready<C::Token, C::Layout> as Close>::State as Retry>::Owner,
    tuning: C::Tuning,
}

impl<C: Context> Closing<C> {
    pub(super) fn new(
        ring: <Ready<C::Token, C::Layout> as Close>::State,
        tuning: C::Tuning,
    ) -> Self {
        Self { ring, tuning }
    }
}

impl<C: Context> Retry for Closing<C> {
    type Failure = <<Ready<C::Token, C::Layout> as Close>::State as Retry>::Failure;
    type Owner = Pending<C>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        let Self { ring, tuning } = self;
        ring.recover().map(|ring| Pending { ring, tuning })
    }
}

impl<C: Context> Close for Pending<C> {
    type State = Closing<C>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { ring, tuning } = self;
        match ring.try_close() {
            Ok(()) => Ok(()),
            Err(ring) => Err(Closing { ring, tuning }),
        }
    }
}

impl<C: Context> Unmap for Closing<C> {
    type Error = <<Ready<C::Token, C::Layout> as Close>::State as Unmap>::Error;

    fn failure(&self) -> Storage<Self::Error> {
        self.ring.failure()
    }
}
