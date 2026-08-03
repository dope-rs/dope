use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;

pub mod metadata;
pub mod open;
mod raw;
pub mod read;
pub mod source;
pub mod stat;

use std::io::Error;
use std::process::abort;

use dope::DriverContext;
use dope::manifold::Manifold;
use dope::manifold::typed::TypedToken;
use dope::runtime::dispatcher::Idle;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::route::Route;
use dope_core::driver::token::{Token, TokenCapacity};
use dope_core::io::Event;
use dope_core::io::file::OpenPath;
use open::{OpenDone, OpenTable};
use raw::table::CancellationSignal;
use read::{ReadDone, ReadTable};
use source::Source;
use stat::{StatDone, StatTable};

use crate::runtime::executor::StorageFactory;

pub enum FileOutcome<R> {
    Done(R),
    Pending,
}

pub struct Files<'d, const ID: u8, const N: usize> {
    route: Route<'d, ID>,
    opens: OpenTable<'d, ID>,
    reads: ReadTable<'d, ID>,
    stats: StatTable<'d, ID>,
    cancellations: CancellationSignal,
    poison_route: Cell<bool>,
    _id: PhantomData<fn() -> [(); N]>,
}

pub struct FilesFactory<const ID: u8, const N: usize>;

impl<'d, const ID: u8, const N: usize> Files<'d, ID, N> {
    pub fn new(driver: &mut DriverContext<'_, 'd>) -> io::Result<Self> {
        let capacity = TokenCapacity::new(N).ok_or_else(|| {
            Error::new(
                io::ErrorKind::InvalidInput,
                "dope: file capacity exceeds token slots",
            )
        })?;
        let opens = OpenTable::new(capacity);
        let reads = ReadTable::new(capacity);
        let stats = StatTable::new(capacity);
        Ok(Self {
            route: Route::reserve(driver)?,
            opens,
            reads,
            stats,
            cancellations: CancellationSignal::new(),
            poison_route: Cell::new(false),
            _id: PhantomData,
        })
    }

    pub fn factory() -> FilesFactory<ID, N> {
        FilesFactory
    }

    pub fn manifold(&self) -> FileManifold<'_, 'd, ID, N> {
        FileManifold { files: self }
    }

    fn record_quiesce(&self, quiesced: bool) {
        if quiesced {
            self.poison_route.set(true);
        }
    }

    #[doc(hidden)]
    pub fn begin_open(
        &self,
        path: OpenPath,
        flags: i32,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.opens.begin(path, flags, driver)
    }

    #[doc(hidden)]
    pub fn begin_read(
        &self,
        source: Source<'d>,
        buf: Vec<u8>,
        len: u32,
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Source<'d>, Vec<u8>, Error)> {
        self.reads.begin(source, buf, len, offset, driver)
    }

    #[doc(hidden)]
    pub fn begin_stat_path(
        &self,
        path: OpenPath,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, OpenPath> {
        self.stats.begin_path(path, driver)
    }

    #[doc(hidden)]
    pub fn begin_stat_fd(
        &self,
        source: Source<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, Source<'d>> {
        self.stats.begin_fd(source, driver)
    }

    #[doc(hidden)]
    pub fn poll_open(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<OpenDone> {
        self.opens.poll(token, wake)
    }

    #[doc(hidden)]
    pub fn poll_read(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Source<'d>, Vec<u8>, ReadDone)> {
        self.reads.poll(token, wake)
    }

    #[doc(hidden)]
    pub fn poll_stat_path(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<StatDone> {
        self.stats.poll_path(token, wake)
    }

    #[doc(hidden)]
    pub fn poll_stat_fd(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Source<'d>, StatDone)> {
        self.stats.poll_fd(token, wake)
    }

    #[doc(hidden)]
    pub fn cancel_open(&self, token: Token) {
        self.opens.cancel(token, &self.cancellations);
    }

    #[doc(hidden)]
    pub fn cancel_read(&self, token: Token) {
        self.reads.cancel(token, &self.cancellations);
    }

    #[doc(hidden)]
    pub fn cancel_stat(&self, token: Token) {
        self.stats.cancel(token, &self.cancellations);
    }

    fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) {
        if !self.cancellations.is_pending() {
            return;
        }
        let mut quiesce = driver.quiesce_batch();
        self.opens.flush_cancellations(&mut quiesce);
        self.reads.flush_cancellations(&mut quiesce);
        self.stats.flush_cancellations(&mut quiesce);
        let outcome = quiesce.finish();
        self.cancellations.clear();
        self.record_quiesce(outcome.needs_poison());
    }
}

impl<'d, const ID: u8, const N: usize> Files<'d, ID, N> {
    fn shutdown(&self, driver: &mut DriverContext<'_, 'd>) {
        let mut quiesce = driver.quiesce_batch();
        self.opens.for_each_target(|target| quiesce.cancel(target));
        self.reads.for_each_target(|target| quiesce.cancel(target));
        self.stats.for_each_target(|target| quiesce.cancel(target));
        let outcome = quiesce.finish();
        self.route
            .finish(driver, self.poison_route.get() || outcome.has_targets());
    }
}

impl<const ID: u8, const N: usize> StorageFactory for FilesFactory<ID, N> {
    type Output<'d> = Files<'d, ID, N>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        match Files::new(driver) {
            Ok(files) => files,
            Err(_) => abort(),
        }
    }
}

pub struct FileManifold<'scope, 'd, const ID: u8, const N: usize> {
    files: &'scope Files<'d, ID, N>,
}

impl<'scope, 'd, const ID: u8, const N: usize> Manifold<'d> for FileManifold<'scope, 'd, ID, N> {
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, _driver: &mut DriverContext<'_, 'd>) {
        let this = self.as_ref().get_ref().files;
        match ev {
            Event::Open(token, e) => this.opens.complete(token, e),
            Event::Read(token, e) => this.reads.complete(token, e),
            Event::Stat(token, e) => this.stats.complete(token, e),
            _ => {}
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_ref().get_ref().files.flush_cancellations(driver);
    }

    fn idle(self: Pin<&Self>, _region: &o3::cell::RegionToken<'d>) -> Idle {
        let this = self.get_ref().files;
        if !this.opens.is_empty() || !this.reads.is_empty() || !this.stats.is_empty() {
            Idle::Busy
        } else {
            Idle::Park(None)
        }
    }

    fn activate(
        self: Pin<&mut Self>,
        _target: TypedToken<Self>,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = self;
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_ref().get_ref().files.shutdown(driver);
    }
}
