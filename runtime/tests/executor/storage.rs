use std::{cell::Cell, convert, io, marker::PhantomPinned, ptr::NonNull};

use dope_core::driver::{settings, storage};

struct Factory;

struct FailingFactory;

struct Pinned {
    address: Cell<Option<NonNull<Self>>>,
    _pin: PhantomPinned,
}

impl Pinned {
    fn record(&self) {
        self.address.set(Some(NonNull::from(self)));
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        assert_eq!(self.address.get(), Some(NonNull::from(&mut *self)));
    }
}

impl storage::Factory for Factory {
    type Output<'d> = Pinned;
    type Error = convert::Infallible;

    fn build<'d>(
        self,
        _context: &mut storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'d>, Self::Error> {
        Ok(Pinned {
            address: Cell::new(None),
            _pin: PhantomPinned,
        })
    }
}

impl storage::Factory for FailingFactory {
    type Output<'d> = ();
    type Error = io::Error;

    fn build<'d>(self, _context: &mut storage::Context<'_, 'd>) -> io::Result<()> {
        Err(io::Error::other("storage failed"))
    }
}

#[test]
fn storage_factory_failure_is_returned() -> io::Result<()> {
    let result = dope_runtime::executor::Executor::new(settings::Config::for_quic_udp(2, 8)?)?
        .with_factory(FailingFactory)
        .try_enter(|_| ());
    assert!(result.is_err());
    Ok(())
}

#[test]
fn composed_infallible_factories_use_infallible_entry() {
    dope_test::scenario::rt::Runtime::throughput()
        .executor()
        .with_factory(((), ()))
        .enter(|session| assert_eq!(*session.storage(), ((), ())));
}

#[test]
fn storage_is_dropped_in_place_after_pinning() {
    let exec = dope_test::scenario::rt::Runtime::throughput()
        .executor()
        .with_factory(Factory);
    exec.enter(|session| session.storage().record());
}

#[test]
fn owned_storage_keeps_its_exact_type_in_the_session() {
    dope_test::scenario::rt::Runtime::throughput()
        .executor()
        .with_storage(String::from("handler"))
        .enter(|session| assert_eq!(session.storage(), "handler"));
}
