use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;

use dope_core::driver::control::ContextControl;
use o3::buffer::Block;

mod metadata;
mod open;
mod raw;
mod read;
mod source;
mod splice;
mod stat;
mod table;

pub use metadata::Metadata;
pub use open::OpenDone;
pub use read::ReadDone;
#[doc(hidden)]
pub use source::SourceRef;
pub use source::{Direct, Fixed, Source};
pub use splice::SpliceDone;
pub use stat::StatDone;

use open::OpenTable;
use read::ReadTable;
use splice::SpliceTable;
use stat::StatTable;

use dope::DriverContext;
use dope::manifold::Manifold;
use dope::manifold::TypedToken;
use dope::runtime::Idle;
use dope_core::driver::ready::CompletionWaker;
use dope_core::driver::route::Route;
use dope_core::driver::token::kind::{READ, READ_BLOCK};
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::EventKind;
use dope_core::io::fd::Fd;
use dope_core::io::file::OpenPath;

pub enum FileOutcome<R> {
    Done(R),
    Pending,
}

pub struct Files<'d, const ID: u8, const N: usize> {
    route: Route<'d, ID>,
    opens: OpenTable<'d, ID>,
    reads: ReadTable<'d, Vec<u8>, ID, READ>,
    block_reads: ReadTable<'d, Block, ID, READ_BLOCK>,
    splices: SpliceTable<'d, ID>,
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
            block_reads: ReadTable::new(N),
            splices: SpliceTable::new(N),
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

    pub fn alloc_fixed(&self, driver: &mut DriverContext<'_, 'd>) -> io::Result<Fd<'d>> {
        let base = driver.reserve_outbound(1)?;
        let Some(slot) = base.slot(SlotIndex::new(0)) else {
            return Err(io::Error::other(
                "dope: backend returned an empty single-slot reservation",
            ));
        };
        Ok(unsafe { Fd::from_raw_slot(slot.fd(), driver.driver_ref()) })
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
    pub fn begin_open_fixed(
        &self,
        path: OpenPath,
        flags: i32,
        fd: &Fd<'_>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.opens.begin_fixed(path, flags, fd, driver)
    }

    #[doc(hidden)]
    pub fn begin_read(
        &self,
        source: SourceRef<'d>,
        buf: Vec<u8>,
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Vec<u8>, io::Error)> {
        self.reads.begin(source, buf, offset, driver)
    }

    #[doc(hidden)]
    pub fn begin_block_read(
        &self,
        source: SourceRef<'d>,
        buf: Block,
        offset: u64,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Token, (Block, io::Error)> {
        self.block_reads.begin(source, buf, offset, driver)
    }

    #[doc(hidden)]
    pub fn begin_splice_to_pipe(
        &self,
        source: Rc<OwnedFd>,
        off_in: i64,
        sink: Rc<OwnedFd>,
        len: u32,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.splices.begin(source, off_in, sink, len, driver)
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
        fd: Rc<OwnedFd>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Token> {
        self.stats.begin_fd(fd, driver)
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
    pub fn poll_block_read(
        &self,
        token: Token,
        wake: CompletionWaker<'d>,
    ) -> FileOutcome<(Block, ReadDone)> {
        self.block_reads.poll(token, wake)
    }

    #[doc(hidden)]
    pub fn poll_splice(&self, token: Token, wake: CompletionWaker<'d>) -> FileOutcome<SpliceDone> {
        self.splices.poll(token, wake)
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
    pub fn cancel_block_read(&self, token: Token) {
        self.block_reads.cancel(token);
    }

    #[doc(hidden)]
    pub fn cancel_splice(&self, token: Token) {
        self.splices.cancel(token);
    }

    #[doc(hidden)]
    pub fn cancel_stat(&self, token: Token) {
        self.stats.cancel(token);
    }

    fn flush_cancellations(&self, driver: &mut DriverContext<'_, 'd>) {
        let quiesced = self.opens.flush_cancellations(driver)
            | self.reads.flush_cancellations(driver)
            | self.block_reads.flush_cancellations(driver)
            | self.splices.flush_cancellations(driver)
            | self.stats.flush_cancellations(driver);
        self.record_quiesce(quiesced);
    }
}

impl<'d, const ID: u8, const N: usize> Files<'d, ID, N> {
    fn shutdown(&self, driver: &mut DriverContext<'_, 'd>) {
        let mut targets = Vec::new();
        self.opens.append_targets(&mut targets);
        self.reads.append_targets(&mut targets);
        self.block_reads.append_targets(&mut targets);
        self.splices.append_targets(&mut targets);
        self.stats.append_targets(&mut targets);
        if !targets.is_empty() {
            driver.quiesce(&targets);
        }
        self.route
            .finish(driver, self.poison_route.get() || !targets.is_empty());
    }
}

impl<const ID: u8, const N: usize> dope::runtime::StorageFactory for FilesFactory<ID, N> {
    type Output<'d> = Files<'d, ID, N>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        match Files::new(driver) {
            Ok(files) => files,
            Err(_) => std::process::abort(),
        }
    }
}

pub struct FileManifold<'scope, 'd, const ID: u8, const N: usize> {
    files: &'scope Files<'d, ID, N>,
}

impl<'scope, 'd, const ID: u8, const N: usize> Manifold<'d> for FileManifold<'scope, 'd, ID, N> {
    const ID: u8 = ID;

    fn dispatch(
        self: Pin<&mut Self>,
        ev: dope_core::io::Event<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let this = self.as_ref().get_ref().files;
        match ev.into_kind() {
            EventKind::Open(token, e) => this.opens.complete(token, e, driver),
            EventKind::Read(token, e) => this.reads.complete(token, e, driver),
            EventKind::ReadBlock(token, e) => this.block_reads.complete(token, e, driver),
            EventKind::Splice(token, e) => this.splices.complete(token, e, driver),
            EventKind::Stat(token, e) => this.stats.complete(token, e, driver),
            _ => {}
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_ref().get_ref().files.flush_cancellations(driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        let this = self.get_ref().files;
        if !this.opens.is_empty()
            || !this.reads.is_empty()
            || !this.block_reads.is_empty()
            || !this.splices.is_empty()
            || !this.stats.is_empty()
        {
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
