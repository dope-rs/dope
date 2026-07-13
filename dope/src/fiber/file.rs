use std::future::Future;
use std::io;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::backend::file::{OpenPath, OsFile};
use crate::backend::socket::FdSlot;
use crate::fiber::Holding;
use crate::manifold::file::{FileOutcome, Files, OpenDone, ReadDone, SpliceDone};
use crate::{Driver, backend};

#[derive(Debug)]
pub enum Source {
    Fd(RawFd),
    Fixed(FdSlot),
}

impl Source {
    pub fn fd(fd: RawFd) -> Self {
        Self::Fd(fd)
    }

    pub fn fixed(slot: FdSlot) -> Self {
        Self::Fixed(slot)
    }

    pub fn of(file: &OsFile) -> Self {
        Self::Fd(file.fd())
    }
}

enum Stage {
    Init,
    Pending(backend::token::Token),
    Done,
}

pub struct Open<'h, 'd, 'p, const ID: u8, const N: usize> {
    host: Holding<'h, Files<ID, N>>,
    driver: &'d Driver,
    path: &'p OpenPath,
    flags: i32,
    fixed: Option<FdSlot>,
    stage: Stage,
}

impl<'h, 'd, 'p, const ID: u8, const N: usize> Open<'h, 'd, 'p, ID, N> {
    pub(crate) fn new(
        host: Holding<'h, Files<ID, N>>,
        driver: &'d Driver,
        path: &'p OpenPath,
        flags: i32,
        fixed: Option<FdSlot>,
    ) -> Self {
        Self {
            host,
            driver,
            path,
            flags,
            fixed,
            stage: Stage::Init,
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for Open<'_, '_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for Open<'_, '_, '_, ID, N> {
    fn drop(&mut self) {
        if let Stage::Pending(token) = self.stage {
            // SAFETY: thread-per-core; driver outlives the future, no aliasing &mut.
            let driver = self.driver;
            self.host.hold().cancel_open(token, driver);
        }
    }
}

impl<'h, 'd, 'p, const ID: u8, const N: usize> Future for Open<'h, 'd, 'p, ID, N> {
    type Output = io::Result<Source>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match this.stage {
            Stage::Done => return Poll::Ready(Err(already_done())),
            Stage::Pending(t) => t,
            Stage::Init => {
                // SAFETY: thread-per-core; driver outlives the future, no aliasing &mut.
                let driver = this.driver;
                let begun = match this.fixed {
                    Some(slot) => this
                        .host
                        .hold()
                        .begin_open_fixed(this.path, this.flags, slot, driver),
                    None => this.host.hold().begin_open(this.path, this.flags, driver),
                };
                let Some(t) = begun else {
                    this.stage = Stage::Done;
                    return Poll::Ready(Err(io::Error::other("dope::file: open submit failed")));
                };
                this.stage = Stage::Pending(t);
                t
            }
        };
        match this.host.hold().poll_open(token, cx.waker()) {
            FileOutcome::Done(done) => {
                this.stage = Stage::Done;
                Poll::Ready(match done {
                    OpenDone::Failed(errno) => Err(io::Error::from_raw_os_error(errno)),
                    OpenDone::Fd(fd) => match this.fixed {
                        Some(slot) => Ok(Source::Fixed(slot)),
                        None => Ok(Source::Fd(fd)),
                    },
                })
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}

/// Positional read; the buffer is owned by the manifold for the op's lifetime and
/// returned as `(buf, result)`, so a mid-read drop is sound (no kernel UAF).
pub struct Read<'h, 'd, const ID: u8, const N: usize> {
    host: Holding<'h, Files<ID, N>>,
    driver: &'d Driver,
    src: Source,
    buf: Option<Vec<u8>>,
    offset: u64,
    stage: Stage,
}

impl<'h, 'd, const ID: u8, const N: usize> Read<'h, 'd, ID, N> {
    pub(crate) fn new(
        host: Holding<'h, Files<ID, N>>,
        driver: &'d Driver,
        src: Source,
        buf: Vec<u8>,
        offset: u64,
    ) -> Self {
        Self {
            host,
            driver,
            src,
            buf: Some(buf),
            offset,
            stage: Stage::Init,
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for Read<'_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for Read<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let Stage::Pending(token) = self.stage {
            // SAFETY: thread-per-core; driver outlives the future, no aliasing &mut.
            let driver = self.driver;
            self.host.hold().cancel_read(token, driver);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Future for Read<'h, 'd, ID, N> {
    type Output = (Vec<u8>, io::Result<usize>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match this.stage {
            Stage::Done => return Poll::Ready((Vec::new(), Err(already_done()))),
            Stage::Pending(t) => t,
            Stage::Init => {
                let buf = this
                    .buf
                    .take()
                    .expect("read future polled without a buffer");
                if buf.is_empty() {
                    this.stage = Stage::Done;
                    return Poll::Ready((buf, Ok(0)));
                }
                let host = this.host;
                // SAFETY: thread-per-core; driver outlives the future, no aliasing &mut.
                let driver = this.driver;
                let begun = match this.src {
                    Source::Fd(fd) => host.hold().begin_read(fd, buf, this.offset, driver),
                    Source::Fixed(slot) => {
                        host.hold().begin_read_fixed(slot, buf, this.offset, driver)
                    }
                };
                match begun {
                    Ok(t) => {
                        this.stage = Stage::Pending(t);
                        t
                    }
                    Err(buf) => {
                        this.stage = Stage::Done;
                        return Poll::Ready((
                            buf,
                            Err(io::Error::other("dope::file: read submit failed")),
                        ));
                    }
                }
            }
        };
        match this.host.hold().poll_read(token, cx.waker()) {
            FileOutcome::Done((buf, done)) => {
                this.stage = Stage::Done;
                Poll::Ready(match done {
                    ReadDone::Read(n) => (buf, Ok(n as usize)),
                    ReadDone::Eof => (buf, Ok(0)),
                    ReadDone::Failed(errno) => (buf, Err(io::Error::from_raw_os_error(errno))),
                })
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}

pub struct SpliceToPipe<'h, 'd, const ID: u8, const N: usize> {
    host: Holding<'h, Files<ID, N>>,
    driver: &'d Driver,
    src: Source,
    off_in: i64,
    pipe_write_fd: RawFd,
    len: u32,
    stage: Stage,
}

impl<'h, 'd, const ID: u8, const N: usize> SpliceToPipe<'h, 'd, ID, N> {
    pub(crate) fn new(
        host: Holding<'h, Files<ID, N>>,
        driver: &'d Driver,
        src: Source,
        off_in: i64,
        pipe_write_fd: RawFd,
        len: u32,
    ) -> Self {
        Self {
            host,
            driver,
            src,
            off_in,
            pipe_write_fd,
            len,
            stage: Stage::Init,
        }
    }
}

impl<const ID: u8, const N: usize> Unpin for SpliceToPipe<'_, '_, ID, N> {}

impl<const ID: u8, const N: usize> Drop for SpliceToPipe<'_, '_, ID, N> {
    fn drop(&mut self) {
        if let Stage::Pending(token) = self.stage {
            // SAFETY: thread-per-core; driver outlives the future, no aliasing &mut.
            let driver = self.driver;
            self.host.hold().cancel_splice(token, driver);
        }
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Future for SpliceToPipe<'h, 'd, ID, N> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let token = match this.stage {
            Stage::Done => return Poll::Ready(Err(already_done())),
            Stage::Pending(t) => t,
            Stage::Init => {
                let fd_in = match this.src {
                    Source::Fd(fd) => fd,
                    Source::Fixed(_) => {
                        this.stage = Stage::Done;
                        return Poll::Ready(Err(io::Error::other(
                            "dope::file: splice-to-pipe requires a raw fd source",
                        )));
                    }
                };
                // SAFETY: thread-per-core; driver outlives the future, no aliasing &mut.
                let driver = this.driver;
                let begun = this.host.hold().begin_splice_to_pipe(
                    fd_in,
                    this.off_in,
                    this.pipe_write_fd,
                    this.len,
                    driver,
                );
                let Some(t) = begun else {
                    this.stage = Stage::Done;
                    return Poll::Ready(Err(io::Error::other("dope::file: splice submit failed")));
                };
                this.stage = Stage::Pending(t);
                t
            }
        };
        match this.host.hold().poll_splice(token, cx.waker()) {
            FileOutcome::Done(done) => {
                this.stage = Stage::Done;
                Poll::Ready(match done {
                    SpliceDone::Moved(n) => Ok(n as usize),
                    SpliceDone::Eof => Ok(0),
                    SpliceDone::Failed(errno) => Err(io::Error::from_raw_os_error(errno)),
                })
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }
}

fn already_done() -> io::Error {
    io::Error::other("dope::file: future polled after completion")
}
