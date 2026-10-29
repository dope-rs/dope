use std::{os::fd, time};

use dope_sys::{
    io::{
        Close,
        reactor::{
            complete,
            complete::{Entry as _, Flags as _, flags::Meta as _},
            submit::Queue as _,
        },
    },
    kernel, memory,
    memory::{
        buffer,
        buffer::{provided, provided::Producer as _},
    },
    ring::{
        Access as _, Source, enter,
        enter::{Enter as _, Poll},
        ready,
        ready::Error,
        register::{Registered as _, Registry as _},
    },
    table::token,
};
use o3::base::num::bounded;

use crate::{
    Ring, Uring, error,
    reactor::{
        buffers::{self, Producer, Selected},
        cq, register, ring,
        ring::enter::Timeout,
        sq,
    },
};

const GROUP: u16 = <Uring as provided::Source>::PROVIDED;

#[repr(transparent)]
pub struct Ready<T, L: memory::raw::Receive> {
    owner: register::raw::Buffered<Producer<L>, GROUP, register::raw::Registered<T>>,
}

type Failure<T, L> = Error<
    Ring<T>,
    Producer<L>,
    <Producer<L> as provided::Producer>::Error,
    register::Attempt<T>,
    register::raw::Registered<T>,
    kernel::Errno<error::Register<22>>,
>;

impl<T: complete::raw::Token, L: memory::raw::Receive> ready::Complete for Ready<T, L> {
    type Token = T;
    type Layout = L;
    type Pending = Selected<L::Extent, <L::Extent as memory::raw::Extent>::Capacity>;
    type Index = cq::Buffer;
    type Entry = cq::Entry<T>;

    fn acquire(&mut self) {
        self.owner.acquire();
    }

    fn publish(&mut self) {
        self.owner.publish();
    }

    fn pop(&mut self) -> Option<(Self::Entry, Option<Result<Self::Pending, Self::Index>>)> {
        let entry = self.owner.pop()?;
        let buffer = entry.into_parts().flags.meta().buffer().map(|index| {
            // SAFETY: this popped CQE uniquely transfers the slot from this ring.
            unsafe { buffer::raw::Select::select(&self.owner.storage, index) }
        });
        Some((entry, buffer))
    }

    fn refresh(&mut self) -> bounded::U32<0, { u32::MAX }> {
        self.owner.refresh()
    }

    fn layout(&self) -> L {
        self.owner.storage.layout()
    }

    fn flush(&mut self) {
        self.owner.storage_mut().flush();
    }
}

impl<T: complete::raw::Token, L: memory::raw::Receive> Ready<T, L> {
    /// Retains the idle queue's exclusive admitted submission vacancy.
    pub(crate) fn try_idle(
        &mut self,
    ) -> Result<Option<sq::raw::Vacancy<'_, T>>, <Uring as Source>::Poll> {
        let register::raw::Buffered { owner, storage } = &mut self.owner;
        owner.publish();
        storage.flush();
        owner
            .ring
            .sq()
            .try_idle(ring::ffi::Binding::registered(&owner.auth))
            .map_err(Poll::Submit)
    }
}

impl<T: complete::raw::Token, L: memory::raw::Receive> ready::Ready for Ready<T, L> {
    type Ring = Ring<T>;
    type Context = buffers::raw::Context<L>;
    type Producer = Producer<L>;
    type Registration = register::Attempt<T>;
    type Registered = register::raw::Registered<T>;
    type Error = kernel::Errno<error::Enter>;
    type BufferError = kernel::Errno<error::Register<22>>;
    type InstallError = kernel::Errno<error::Register<6>>;
    type CancelError = kernel::Errno<error::Register<24>>;
    type Target = sq::Fixed;
    type Queue<'q>
        = sq::raw::Queue<'q, T>
    where
        Self: 'q;
    type Enter<'q>
        = ring::Enter<'q, T>
    where
        Self: 'q;

    fn enter(&mut self) -> ring::Enter<'_, T> {
        self.owner.publish();
        self.owner.storage_mut().flush();
        self.owner.enter()
    }

    fn submission(&mut self) -> sq::raw::Queue<'_, T> {
        self.owner.sq()
    }

    fn submit(&mut self) -> Result<(), Poll<Self::Error, Self::CancelError>> {
        loop {
            self.enter().try_submit().map_err(Poll::Submit)?;
            let mut queue = self.submission();
            queue.refresh();
            if queue.is_empty() {
                return Ok(());
            }
        }
    }

    fn commit(&mut self) -> Result<(), Poll<Self::Error, Self::CancelError>> {
        if matches!(self.advance()?, enter::Progress::Entered) {
            self.submission().refresh();
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<enter::Progress, Poll<Self::Error, Self::CancelError>> {
        self.enter().try_commit().map_err(Poll::Submit)
    }

    fn try_wait(
        &mut self,
        timeout: Option<time::Duration>,
    ) -> Result<(), Poll<Self::Error, Self::CancelError>> {
        let wait = match timeout {
            Some(value) => Some(Timeout::try_from(value).map_err(Poll::from)?),
            None => None,
        };
        match self.enter().try_wait(wait) {
            Ok(()) => Ok(()),
            Err(errno)
                if errno.code().get() == libc::ETIME
                    || errno.is_code(dope_sys::Errno::INTERRUPTED) =>
            {
                Ok(())
            }
            Err(code) => Err(Poll::Wait(code)),
        }
    }

    fn install(
        &mut self,
        target: sq::Fixed,
        file: fd::BorrowedFd<'_>,
    ) -> Result<(), Self::InstallError> {
        self.owner.registry().install(target, file)
    }

    fn cancel_all(&mut self) -> Result<(), Poll<Self::Error, Self::CancelError>> {
        self.owner.storage_mut().flush();
        self.owner.registry().cancel_all().map_err(Poll::Cancel)
    }
}

impl<T, L: memory::raw::Receive> Close for Ready<T, L> {
    type State = register::raw::Closing<Producer<L>, GROUP, register::raw::Retained<T>>;

    fn try_close(self) -> Result<(), Self::State> {
        self.owner.try_close()
    }
}

mod raw;
