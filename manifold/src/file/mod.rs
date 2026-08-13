use std::{io, marker, pin};

use dope_core::{
    driver::{
        self, lifecycle::routing, retained, route, schedule, schedule::ready::completion, storage,
    },
    io::fs,
};
use dope_runtime::client;

pub mod appender;
mod cancellation;
pub mod durable;
pub mod open;
pub mod read;

mod metadata;
mod regular;
mod sealed;
pub use metadata::Metadata;
pub use regular::Regular;
pub(in crate::file) use sealed::{Locked, Tables};

pub enum Outcome<R> {
    Done(R),
    Pending,
}

/// A file operation capability tied to one driver generation and route.
///
/// The driver brand is invariant: a key cannot be shortened and then carried
/// into another generative driver scope.
///
/// ```compile_fail
/// use dope_manifold::file::Key;
///
/// fn shorten<'short, 'long: 'short, T, const ID: u8>(
///     key: Key<'long, T, ID>,
/// ) -> Key<'short, T, ID> {
///     key
/// }
/// ```
#[repr(transparent)]
pub struct Key<'d, T, const ID: u8> {
    raw: route::Token,
    operation: marker::PhantomData<fn() -> T>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

const _: () =
    assert!(std::mem::size_of::<Key<'static, (), 0>>() == std::mem::size_of::<route::Token>());

impl<T, const ID: u8> Clone for Key<'_, T, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, const ID: u8> Copy for Key<'_, T, ID> {}

impl<'d, T, const ID: u8> Key<'d, T, ID> {
    fn new(raw: route::Token) -> Self {
        Self {
            raw,
            operation: marker::PhantomData,
            driver: marker::PhantomData,
        }
    }

    const fn raw(self) -> route::Token {
        self.raw
    }
}

/// Access issued by an application selecting its installed file manifold.
#[repr(transparent)]
pub struct Access<'app, 'd: 'app, const ID: u8, const N: usize, F>
where
    F: fs::Mode,
{
    files: &'d Files<'d, ID, N, F>,
    _lease: marker::PhantomData<client::Lease<'app, 'd>>,
}

impl<const ID: u8, const N: usize, F> Clone for Access<'_, '_, ID, N, F>
where
    F: fs::Mode,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<const ID: u8, const N: usize, F> Copy for Access<'_, '_, ID, N, F> where F: fs::Mode {}

pub struct Files<'d, const ID: u8, const N: usize, F>
where
    F: fs::Mode,
{
    route: routing::StorageRoute<'d, ID>,
    tables: Tables<'d, ID, F>,
    cancellations: cancellation::Cancellation,
    _id: marker::PhantomData<fn() -> [(); N]>,
}

pub struct FilesFactory<const ID: u8, const N: usize, F>(marker::PhantomData<fn() -> F>)
where
    F: fs::Mode;

impl<'d, const ID: u8, const N: usize, F> Files<'d, ID, N, F>
where
    F: fs::Mode,
{
    fn new(context: &mut storage::Context<'_, 'd>) -> io::Result<Self> {
        use dope_core::driver::route::table::Capacity;
        let capacity = Capacity::new(N).ok_or_else(|| {
            use std::io::{Error, ErrorKind};
            Error::new(
                ErrorKind::InvalidInput,
                "dope: file capacity exceeds token slots",
            )
        })?;
        let tables = Tables::<ID, F>::try_new(capacity, context)?;
        let route = context.reserve_route()?.bind_storage();
        Ok(Self {
            route,
            tables,
            cancellations: cancellation::Cancellation::new(),
            _id: marker::PhantomData,
        })
    }

    pub fn factory() -> FilesFactory<ID, N, F> {
        FilesFactory(marker::PhantomData)
    }

    pub fn manifold(&'d self) -> Manifold<'d, ID, N, F> {
        Manifold { files: self }
    }

    fn flush_cancellations(
        &self,
        work: schedule::Maintenance<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if !self.cancellations.is_pending() {
            return;
        }
        if !self
            .tables
            .flush_cancellations(&self.cancellations, work, driver)
        {
            return;
        }
        self.cancellations.clear();
    }
}

impl<'app, 'd: 'app, const ID: u8, const N: usize, F> Access<'app, 'd, ID, N, F>
where
    F: fs::Mode,
{
    #[doc(hidden)]
    pub fn begin_open(
        &self,
        path: fs::OpenPath,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> io::Result<Key<'d, open::Operation, ID>> {
        self.files.tables.begin_open(path, driver)
    }

    #[doc(hidden)]
    pub fn begin_read(
        &self,
        file: Regular,
        buffer: Vec<u8>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<Key<'d, read::Operation, ID>, (Regular, Vec<u8>, io::Error)> {
        self.files.tables.begin_read(file, buffer, driver)
    }

    #[doc(hidden)]
    pub fn poll_open(
        &self,
        token: Key<'d, open::Operation, ID>,
        wake: completion::Waker<'d>,
    ) -> Outcome<open::Done> {
        self.files.tables.poll_open(token, wake)
    }

    #[doc(hidden)]
    pub fn poll_read(
        &self,
        token: Key<'d, read::Operation, ID>,
        wake: completion::Waker<'d>,
    ) -> Outcome<(Vec<u8>, read::Done)> {
        self.files.tables.poll_read(token, wake)
    }

    #[doc(hidden)]
    pub fn cancel_open(&self, token: Key<'d, open::Operation, ID>) {
        self.files
            .tables
            .cancel_open(token, &self.files.cancellations);
    }

    #[doc(hidden)]
    pub fn cancel_read(&self, token: Key<'d, read::Operation, ID>) {
        self.files
            .tables
            .cancel_read(token, &self.files.cancellations);
    }
}

impl<const ID: u8, const N: usize, F> storage::Factory for FilesFactory<ID, N, F>
where
    F: fs::Mode + 'static,
{
    type Output<'d> = Files<'d, ID, N, F>;
    type Error = io::Error;

    fn build<'d>(self, context: &mut storage::Context<'_, 'd>) -> io::Result<Self::Output<'d>> {
        Files::new(context)
    }
}

pub struct Manifold<'d, const ID: u8, const N: usize, F>
where
    F: fs::Mode,
{
    files: &'d Files<'d, ID, N, F>,
}

impl<'d, const ID: u8, const N: usize, F> client::Provider<'d> for Manifold<'d, ID, N, F>
where
    F: fs::Mode,
{
    type Client<'app>
        = Access<'app, 'd, ID, N, F>
    where
        'd: 'app;

    fn provide<'app>(
        self: pin::Pin<&Self>,
        scope: client::Scope<'app, 'd, Self>,
    ) -> Self::Client<'app>
    where
        'd: 'app,
    {
        let _lease = scope.lease();
        Access {
            files: self.get_ref().files,
            _lease: marker::PhantomData,
        }
    }
}
