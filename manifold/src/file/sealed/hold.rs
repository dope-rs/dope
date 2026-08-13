use std::{io, marker, os::fd::AsFd as _};

use dope_core::{
    driver::route::{self, kind},
    io::{fs, transfer},
};

use crate::file::{self, read, sealed::operation};

pub(super) struct Hold<F> {
    file: file::Regular,
    buffer: Vec<u8>,
    mode: marker::PhantomData<fn() -> F>,
}

pub(super) struct Prepared<F> {
    file: file::Regular,
    buffer: Vec<u8>,
    remaining: u64,
    inflight: u32,
    mode: marker::PhantomData<fn() -> F>,
}

impl<F> Prepared<F>
where
    F: fs::Mode,
{
    fn submission<'a, 'd, Tag: route::Tag>(
        &'a mut self,
        target: route::Target<'d, Tag>,
    ) -> io::Result<fs::Submission<'a, 'd, F, Tag>> {
        let len = self.remaining.min(transfer::MAX_BYTES as u64) as usize;
        if len == 0 || self.buffer.capacity() - self.buffer.len() < len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope::file: read buffer has no bounded writable region",
            ));
        }
        self.inflight = len as u32;
        let offset = self.buffer.len() as u64;
        let spare = &mut self.buffer.spare_capacity_mut()[..len];
        fs::Submission::read_uninit(self.file.as_fd(), spare, offset, target)
    }

    fn commit(&mut self, amount: u32) -> io::Result<()> {
        if amount > self.inflight || u64::from(amount) > self.remaining {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope::file: completion exceeded its prepared read region",
            ));
        }
        let initialized = self.buffer.len() + amount as usize;
        // SAFETY: the retained submission exclusively borrowed the exact
        // spare region and the checked completion amount reports initialized bytes.
        unsafe { self.buffer.set_len(initialized) };
        self.remaining -= u64::from(amount);
        self.inflight = 0;
        Ok(())
    }
}

// SAFETY: Prepared owns the file and Vec whose spare region is submitted. The
// operation table keeps Prepared fixed and inaccessible until each read
// completes, and commit touches the region only after that completion.
unsafe impl<F> operation::Contract for Hold<F>
where
    F: fs::Mode,
{
    type Mode = F;
    type Event = crate::ReadEvent;
    type Output = read::Done;
    type Prepared = Prepared<F>;

    fn prepare(self) -> Result<Self::Prepared, (Self, io::Error)> {
        let remaining = self.file.metadata().len();
        let Ok(remaining_usize) = usize::try_from(remaining) else {
            return Err((
                self,
                io::Error::new(io::ErrorKind::FileTooLarge, "dope::file: file is too large"),
            ));
        };
        if remaining == 0 || self.buffer.capacity() - self.buffer.len() < remaining_usize {
            return Err((
                self,
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dope::file: read buffer does not match file metadata",
                ),
            ));
        }
        Ok(Prepared {
            file: self.file,
            buffer: self.buffer,
            remaining,
            inflight: 0,
            mode: marker::PhantomData,
        })
    }

    fn submission<'a, 'd, Tag: route::Tag>(
        prepared: &'a mut Self::Prepared,
        target: route::Target<'d, Tag>,
    ) -> io::Result<fs::Submission<'a, 'd, F, Tag>> {
        prepared.submission(target)
    }

    fn into_hold(prepared: Self::Prepared) -> Self {
        Self {
            file: prepared.file,
            buffer: prepared.buffer,
            mode: marker::PhantomData,
        }
    }

    fn target<'d, Tag: route::Tag>(
        _prepared: &Self::Prepared,
        target: route::Target<'d, Tag>,
    ) -> route::Operation<'d, Tag> {
        target.operation(kind::READ)
    }

    fn complete(
        prepared: &mut Self::Prepared,
        event: Self::Event,
    ) -> operation::Step<Self::Output> {
        use dope_core::io::ReadEvent;
        match event {
            ReadEvent::Read(amount) => match prepared.commit(amount) {
                Err(error) => operation::Step::Done(read::Done::Failed(error)),
                Ok(()) if prepared.remaining == 0 => operation::Step::Done(read::Done::Complete),
                Ok(()) => operation::Step::Submit,
            },
            ReadEvent::Eof => operation::Step::Done(read::Done::Failed(io::Error::from(
                io::ErrorKind::UnexpectedEof,
            ))),
            ReadEvent::Failed(errno) => {
                operation::Step::Done(read::Done::Failed(io::Error::from_raw_os_error(errno)))
            }
        }
    }

    fn rejected(_prepared: &mut Self::Prepared, error: io::Error) -> Self::Output {
        read::Done::Failed(error)
    }
}

impl<F> Hold<F> {
    pub(super) fn new(file: file::Regular, buffer: Vec<u8>) -> Self {
        Self {
            file,
            buffer,
            mode: marker::PhantomData,
        }
    }

    pub(super) fn into_parts(self) -> (file::Regular, Vec<u8>) {
        (self.file, self.buffer)
    }
}
