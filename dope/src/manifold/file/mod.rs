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
mod table;

use open::OpenDone;
use read::ReadDone;
use stat::StatDone;

use open::OpenTable;
use read::ReadTable;
use source::Source;
use stat::StatTable;

use crate::runtime::executor::StorageFactory;
use dope::DriverContext;
use dope::manifold::Manifold;
use dope::manifold::typed::TypedToken;
use dope::runtime::dispatcher::Idle;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::route::Route;
use dope_core::driver::token::Token;
use dope_core::io::Event;
use dope_core::io::EventKind;
use dope_core::io::file::OpenPath;
use std::io::Error;
use std::process::abort;

pub enum FileOutcome<R> {
    Done(R),
    Pending,
}

pub struct Files<'d, const ID: u8, const N: usize> {
    route: Route<'d, ID>,
    opens: OpenTable<'d, ID>,
    reads: ReadTable<'d, ID>,
    stats: StatTable<'d, ID>,
    poison_route: Cell<bool>,
    _id: PhantomData<fn() -> [(); N]>,
}

pub struct FilesFactory<const ID: u8, const N: usize>;

impl<'d, const ID: u8, const N: usize> Files<'d, ID, N> {
    pub fn new(driver: &mut DriverContext<'_, 'd>) -> io::Result<Self> {
        Ok(Self {
            route: Route::reserve(driver)?,
            opens: OpenTable::new(N),
            reads: ReadTable::new(N),
            stats: StatTable::new(N),
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
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Vec<u8>, Error)> {
        self.reads.begin(source, buf, offset, driver)
    }

    #[doc(hidden)]
    pub fn begin_stat_path(
        &self,
        path: OpenPath,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.stats.begin_path(path, driver)
    }

    #[doc(hidden)]
    pub fn begin_stat_fd(
        &self,
        source: Source<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
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
    ) -> FileOutcome<(Vec<u8>, ReadDone)> {
        self.reads.poll(token, wake)
    }

    #[doc(hidden)]
    pub fn poll_stat(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<StatDone> {
        self.stats.poll(token, wake)
    }

    #[doc(hidden)]
    pub fn cancel_open(&self, token: Token) {
        self.opens.cancel(token);
    }

    #[doc(hidden)]
    pub fn cancel_read(&self, token: Token) {
        self.reads.cancel(token);
    }

    #[doc(hidden)]
    pub fn cancel_stat(&self, token: Token) {
        self.stats.cancel(token);
    }

    fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) {
        let quiesced = self.opens.flush_cancellations(driver)
            | self.reads.flush_cancellations(driver)
            | self.stats.flush_cancellations(driver);
        self.record_quiesce(quiesced);
    }
}

impl<'d, const ID: u8, const N: usize> Files<'d, ID, N> {
    fn shutdown(&self, driver: &mut DriverContext<'_, 'd>) {
        let mut targets = Vec::new();
        self.opens.append_targets(&mut targets);
        self.reads.append_targets(&mut targets);
        self.stats.append_targets(&mut targets);
        if !targets.is_empty() {
            driver.quiesce(&targets);
        }
        self.route
            .finish(driver, self.poison_route.get() || !targets.is_empty());
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
        match ev.into_kind() {
            EventKind::Open(token, e) => this.opens.complete(token, e),
            EventKind::Read(token, e) => this.reads.complete(token, e),
            EventKind::Stat(token, e) => this.stats.complete(token, e),
            _ => {}
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_ref().get_ref().files.flush_cancellations(driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
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
