use std::io;
use std::marker::PhantomData;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::task::Waker;

use pin_project::pin_project;

use crate::backend::file::OpenPath;
use crate::backend::socket::FdSlot;
use crate::backend::sqe::Sqe;
use crate::fiber::pending::{Pending, Resolve};
use crate::manifold::Manifold;
use crate::manifold::route::TypedToken;
use crate::slab::Slab;
use crate::{Drive, Driver, backend};

#[derive(Clone, Copy)]
pub enum OpenDone {
    Fd(RawFd),
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum ReadDone {
    Read(u32),
    Eof,
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum SpliceDone {
    Moved(u32),
    Eof,
    Failed(i32),
}

pub enum FileOutcome<R> {
    Done(R),
    Pending,
}

// Runtime-owned read buffer: the kernel SQE points at it, so it must outlive the
// future. `orphaned` keeps it alive past a mid-read drop until the CQE lands.
struct ReadHold {
    buf: Vec<u8>,
    orphaned: bool,
}

// `fixed`: a raw openat's CQE carries an fd to close if abandoned; a fixed-table
// open's result is 0 (fd lands in a caller-owned slot) and must never be closed.
#[derive(Clone, Copy)]
struct OpenHold {
    orphaned: bool,
    fixed: bool,
}

#[pin_project(!Unpin)]
pub struct Files<const ID: u8, const N: usize> {
    opens: Slab<OpenHold>,
    reads: Slab<ReadHold>,
    splices: Slab<()>,
    open_pending: Pending<OpenDone>,
    read_pending: Pending<ReadDone>,
    splice_pending: Pending<SpliceDone>,
    _id: PhantomData<fn() -> [(); N]>,
}

impl<const ID: u8, const N: usize> Default for Files<ID, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ID: u8, const N: usize> Files<ID, N> {
    pub fn new() -> Self {
        Self {
            opens: Slab::new(N),
            reads: Slab::new(N),
            splices: Slab::new(N),
            open_pending: Pending::default(),
            read_pending: Pending::default(),
            splice_pending: Pending::default(),
            _id: PhantomData,
        }
    }

    pub fn open_held<'d, 'p>(
        this: crate::fiber::Holding<'d, Self>,
        driver: &mut Driver,
        path: &'p OpenPath,
        flags: i32,
    ) -> crate::fiber::file::Open<'d, 'p, ID, N> {
        crate::fiber::file::Open::new(this, driver, path, flags, None)
    }

    pub fn open_fixed_held<'d, 'p>(
        this: crate::fiber::Holding<'d, Self>,
        driver: &mut Driver,
        path: &'p OpenPath,
        flags: i32,
        slot: FdSlot,
    ) -> crate::fiber::file::Open<'d, 'p, ID, N> {
        crate::fiber::file::Open::new(this, driver, path, flags, Some(slot))
    }

    pub fn read_held<'d>(
        this: crate::fiber::Holding<'d, Self>,
        driver: &mut Driver,
        src: crate::fiber::file::Source,
        buf: Vec<u8>,
        offset: u64,
    ) -> crate::fiber::file::Read<'d, ID, N> {
        crate::fiber::file::Read::new(this, driver, src, buf, offset)
    }

    pub fn splice_to_pipe_held<'d>(
        this: crate::fiber::Holding<'d, Self>,
        driver: &mut Driver,
        src: crate::fiber::file::Source,
        off_in: i64,
        pipe_write_fd: RawFd,
        len: u32,
    ) -> crate::fiber::file::SpliceToPipe<'d, ID, N> {
        crate::fiber::file::SpliceToPipe::new(this, driver, src, off_in, pipe_write_fd, len)
    }

    pub fn alloc_fixed_slot(driver: &mut Driver, count: u32) -> io::Result<FdSlot> {
        let base = driver.reserve_outbound(count)?;
        Ok(base.absolute(backend::token::LocalIdx::new(0)))
    }

    fn token(key: backend::token::Key) -> backend::token::Token {
        backend::token::Token::from_key(ID, key)
    }

    pub(crate) fn begin_open(
        self: Pin<&mut Self>,
        path: &OpenPath,
        flags: i32,
        driver: &mut Driver,
    ) -> Option<backend::token::Token> {
        let this = self.project();
        let key = this.opens.alloc(OpenHold {
            orphaned: false,
            fixed: false,
        })?;
        let token = Self::token(key);
        if driver
            .push(backend::file::open_at(path, flags, token))
            .is_err()
        {
            this.opens.remove(key);
            return None;
        }
        Some(token)
    }

    pub(crate) fn begin_open_fixed(
        self: Pin<&mut Self>,
        path: &OpenPath,
        flags: i32,
        slot: FdSlot,
        driver: &mut Driver,
    ) -> Option<backend::token::Token> {
        let this = self.project();
        let key = this.opens.alloc(OpenHold {
            orphaned: false,
            fixed: true,
        })?;
        let token = Self::token(key);
        let flags = flags & !libc::O_CLOEXEC;
        let sqe = match Sqe::openat_fixed(libc::AT_FDCWD, path.as_ptr(), flags, 0, slot, token) {
            Ok(sqe) => sqe,
            Err(_) => {
                this.opens.remove(key);
                return None;
            }
        };
        if driver.push(sqe).is_err() {
            this.opens.remove(key);
            return None;
        }
        Some(token)
    }

    pub(crate) fn begin_read(
        self: Pin<&mut Self>,
        fd: RawFd,
        buf: Vec<u8>,
        offset: u64,
        driver: &mut Driver,
    ) -> Result<backend::token::Token, Vec<u8>> {
        let this = self.project();
        Self::submit_read(this.reads, driver, buf, |held, token| {
            backend::file::read_fd(fd, held, offset, token)
        })
    }

    pub(crate) fn begin_read_fixed(
        self: Pin<&mut Self>,
        slot: FdSlot,
        buf: Vec<u8>,
        offset: u64,
        driver: &mut Driver,
    ) -> Result<backend::token::Token, Vec<u8>> {
        let this = self.project();
        Self::submit_read(this.reads, driver, buf, |held, token| {
            Sqe::read_fixed_file(slot, held, offset, token)
        })
    }

    // Buffer moves into the slab so the SQE points at stable, owned storage;
    // recovered via `Err` on any submit failure.
    fn submit_read(
        reads: &mut Slab<ReadHold>,
        driver: &mut Driver,
        buf: Vec<u8>,
        make_sqe: impl FnOnce(&mut [u8], backend::token::Token) -> Sqe,
    ) -> Result<backend::token::Token, Vec<u8>> {
        let Some(reservation) = reads.reserve() else {
            return Err(buf);
        };
        let key = reservation.fill(ReadHold {
            buf,
            orphaned: false,
        });
        let token = Self::token(key);
        let held = reads.at_index_mut(key.index()).expect("just filled");
        let sqe = make_sqe(&mut held.buf, token);
        if driver.push(sqe).is_err() {
            let buf = std::mem::take(&mut held.buf);
            reads.remove(key);
            return Err(buf);
        }
        Ok(token)
    }

    pub(crate) fn begin_splice_to_pipe(
        self: Pin<&mut Self>,
        fd_in: RawFd,
        off_in: i64,
        pipe_write_fd: RawFd,
        len: u32,
        driver: &mut Driver,
    ) -> Option<backend::token::Token> {
        let this = self.project();
        let key = this.splices.alloc(())?;
        let token = backend::token::Token::from_key(ID, key);
        if driver
            .push(backend::file::splice_fd_to_pipe(
                fd_in,
                off_in,
                pipe_write_fd,
                len,
                token,
            ))
            .is_err()
        {
            this.splices.remove(key);
            return None;
        }
        Some(token)
    }

    pub(crate) fn poll_splice(
        self: Pin<&mut Self>,
        token: backend::token::Token,
        waker: &Waker,
    ) -> FileOutcome<SpliceDone> {
        let tag = token.slot().raw();
        let this = self.project();
        match this.splice_pending.poll(tag, waker) {
            Resolve::Ready(done) => {
                this.splices.remove(token.key());
                FileOutcome::Done(done)
            }
            Resolve::Pending => FileOutcome::Pending,
        }
    }

    pub(crate) fn cancel_splice(
        self: Pin<&mut Self>,
        token: backend::token::Token,
        driver: &mut Driver,
    ) {
        let this = self.project();
        if this.splices.get(token.key()).is_none() {
            this.splice_pending.cancel(token.slot().raw());
            return;
        }
        let _ = driver.push(Sqe::cancel(token, backend::token::kind::SPLICE));
        this.splice_pending.cancel(token.slot().raw());
        this.splices.remove(token.key());
    }

    pub(crate) fn poll_open(
        self: Pin<&mut Self>,
        token: backend::token::Token,
        waker: &Waker,
    ) -> FileOutcome<OpenDone> {
        let tag = token.slot().raw();
        let this = self.project();
        match this.open_pending.poll(tag, waker) {
            Resolve::Ready(done) => {
                this.opens.remove(token.key());
                FileOutcome::Done(done)
            }
            Resolve::Pending => FileOutcome::Pending,
        }
    }

    pub(crate) fn poll_read(
        self: Pin<&mut Self>,
        token: backend::token::Token,
        waker: &Waker,
    ) -> FileOutcome<(Vec<u8>, ReadDone)> {
        let tag = token.slot().raw();
        let this = self.project();
        match this.read_pending.poll(tag, waker) {
            Resolve::Ready(done) => {
                let buf = this
                    .reads
                    .at_index_mut(token.slot())
                    .map(|held| std::mem::take(&mut held.buf))
                    .unwrap_or_default();
                this.reads.remove(token.key());
                FileOutcome::Done((buf, done))
            }
            Resolve::Pending => FileOutcome::Pending,
        }
    }

    pub(crate) fn cancel_open(
        self: Pin<&mut Self>,
        token: backend::token::Token,
        driver: &mut Driver,
    ) {
        let this = self.project();
        let Some(hold) = this.opens.get(token.key()).copied() else {
            this.open_pending.cancel(token.slot().raw());
            return;
        };
        // Already completed -> close the abandoned fd now; else orphan so
        // `on_open` closes it when the cancelled open's CQE lands.
        if let Some(done) = this.open_pending.take(token.slot().raw()) {
            if let OpenDone::Fd(fd) = done {
                close_abandoned_open(fd, hold.fixed);
            }
            this.opens.remove(token.key());
            return;
        }
        if let Some(held) = this.opens.get_mut(token.key()) {
            held.orphaned = true;
        }
        let _ = driver.push(Sqe::cancel(token, backend::token::kind::OPEN));
    }

    pub(crate) fn cancel_read(
        self: Pin<&mut Self>,
        token: backend::token::Token,
        driver: &mut Driver,
    ) {
        let this = self.project();
        if this.reads.get(token.key()).is_none() {
            this.read_pending.cancel(token.slot().raw());
            return;
        }
        // Already completed -> free now; still in flight -> orphan so the buffer
        // outlives the cancelled op until `on_read` sees the CQE.
        if this.read_pending.cancel(token.slot().raw()) {
            this.reads.remove(token.key());
            return;
        }
        if let Some(held) = this.reads.at_index_mut(token.slot()) {
            held.orphaned = true;
        }
        let _ = driver.push(Sqe::cancel(token, backend::token::kind::READ));
    }

    fn on_open(self: Pin<&mut Self>, token: backend::token::Token, e: backend::OpenEvent) {
        let this = self.project();
        // Epoch-guard: a CQE for a cancelled/reused slot is stale.
        let Some(hold) = this.opens.get(token.key()).copied() else {
            return;
        };
        if hold.orphaned {
            // Future dropped mid-open: close a leaked fd, then free the slot.
            if let backend::OpenEvent::Opened(fd) = e {
                close_abandoned_open(fd, hold.fixed);
            }
            this.opens.remove(token.key());
            return;
        }
        let done = match e {
            backend::OpenEvent::Opened(fd) => OpenDone::Fd(fd),
            backend::OpenEvent::Failed(errno) => OpenDone::Failed(errno),
        };
        this.open_pending.settle(token.slot().raw(), done);
    }

    fn on_read(self: Pin<&mut Self>, token: backend::token::Token, e: backend::ReadEvent) {
        let this = self.project();
        // Epoch-guard: a CQE for a cancelled/reused slot is stale.
        let orphaned = match this.reads.get(token.key()) {
            Some(held) => held.orphaned,
            None => return,
        };
        if orphaned {
            // Future dropped mid-read: kernel done, free the held buffer.
            this.reads.remove(token.key());
            return;
        }
        let done = match e {
            backend::ReadEvent::Read(n) => ReadDone::Read(n),
            backend::ReadEvent::Eof => ReadDone::Eof,
            backend::ReadEvent::Failed(errno) => ReadDone::Failed(errno),
        };
        this.read_pending.settle(token.slot().raw(), done);
    }

    fn on_splice(self: Pin<&mut Self>, token: backend::token::Token, e: backend::SpliceEvent) {
        let this = self.project();
        // Epoch-guard: a CQE for a cancelled/reused slot is stale.
        if this.splices.get(token.key()).is_none() {
            return;
        }
        let done = match e {
            backend::SpliceEvent::Moved(n) => SpliceDone::Moved(n),
            backend::SpliceEvent::Eof => SpliceDone::Eof,
            backend::SpliceEvent::Failed(errno) => SpliceDone::Failed(errno),
        };
        this.splice_pending.settle(token.slot().raw(), done);
    }
}

impl<const ID: u8, const N: usize> Manifold for Files<ID, N> {
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: backend::Event, _driver: &mut Driver) {
        match ev {
            backend::Event::Open(token, e) => self.on_open(token, e),
            backend::Event::Read(token, e) => self.on_read(token, e),
            backend::Event::Splice(token, e) => self.on_splice(token, e),
            _ => {}
        }
    }

    fn pre_park(self: Pin<&mut Self>, _driver: &mut Driver) {}

    fn on_wake(self: Pin<&mut Self>, _target: TypedToken<Self>, _driver: &mut Driver) {}
}

// Fixed-table opens report result 0 (caller-owned slot), so only a raw open's
// abandoned fd is closed here.
fn close_abandoned_open(fd: RawFd, fixed: bool) {
    if fixed || fd < 0 {
        return;
    }
    // SAFETY: a fresh kernel fd whose future was dropped before taking it; unowned.
    unsafe {
        libc::close(fd);
    }
}
