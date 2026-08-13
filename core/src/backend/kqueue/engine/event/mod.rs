mod changes;
mod sealed;

use std::{marker, slice};

pub(in crate::backend::kqueue) use changes::Changes;
pub(in crate::backend::kqueue) use sealed::{Completion, CreateOutcome, Dispatch, Poll, Queue};

use crate::driver::{flight, schedule};

pub(in crate::backend::kqueue) enum CompletionLane {}
pub(in crate::backend::kqueue) enum ChangeLane {}
pub(in crate::backend::kqueue) enum ResumeLane {}

#[repr(transparent)]
pub(in crate::backend::kqueue) struct Events<'a>(slice::Iter<'a, libc::kevent>);

#[repr(transparent)]
pub(in crate::backend::kqueue) struct Kernel<'a>(&'a libc::kevent);

impl<'a> Events<'a> {
    pub(in crate::backend::kqueue) fn new(events: &'a [libc::kevent]) -> Self {
        Self(events.iter())
    }

    pub(in crate::backend::kqueue) fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    pub(in crate::backend::kqueue) fn next(&mut self) -> Option<Kernel<'a>> {
        self.0.next().map(Kernel)
    }
}

impl Kernel<'_> {
    pub(in crate::backend::kqueue) fn filter(&self) -> i16 {
        self.0.filter
    }

    pub(in crate::backend::kqueue) fn error(&self) -> Option<i32> {
        (self.0.flags & libc::EV_ERROR != 0 && self.0.data != 0).then_some(self.0.data as i32)
    }

    pub(in crate::backend::kqueue) fn into_raw(self) -> u64 {
        self.0.udata as usize as u64
    }
}

/// Linear authority for one admitted kqueue dispatch step.
pub(in crate::backend::kqueue) struct Credit<'turn>(marker::PhantomData<&'turn mut ()>);

pub(in crate::backend::kqueue) struct Budget<'turn, 'd, Lane> {
    quota: schedule::Budget<'turn, 'd, Lane>,
}

impl<'turn, 'd, Lane> Budget<'turn, 'd, Lane> {
    pub(in crate::backend::kqueue) fn new(quota: schedule::Budget<'turn, 'd, Lane>) -> Self {
        Self { quota }
    }

    pub(in crate::backend::kqueue) fn remaining(&self) -> usize {
        self.quota.remaining()
    }
}

impl Budget<'_, '_, CompletionLane> {
    pub(in crate::backend::kqueue) fn take(&mut self) -> Option<Credit<'_>> {
        self.quota.take().then_some(Credit(marker::PhantomData))
    }
}

impl Budget<'_, '_, ResumeLane> {
    pub(in crate::backend::kqueue) fn take(&mut self) -> Option<Credit<'_>> {
        self.quota.take().then_some(Credit(marker::PhantomData))
    }
}

impl Budget<'_, '_, ChangeLane> {
    pub(in crate::backend::kqueue) fn spend(&mut self, count: usize) {
        self.quota.spend(count);
    }
}

const _: () = {
    assert!(std::mem::size_of::<Credit<'static>>() == 0);
    assert!(
        std::mem::size_of::<Events<'static>>()
            == std::mem::size_of::<slice::Iter<'static, libc::kevent>>()
    );
    assert!(std::mem::size_of::<Kernel<'static>>() == std::mem::size_of::<&'static libc::kevent>());
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::backend::kqueue::engine) struct Udata(u64);

pub(in crate::backend::kqueue) enum Dequeued {
    Emit(Completion),
    Reclaim(Completion),
}

impl Dequeued {
    pub(super) fn into_completion(self) -> Completion {
        match self {
            Self::Emit(completion) | Self::Reclaim(completion) => completion,
        }
    }
}

impl Udata {
    pub(in crate::backend::kqueue::engine) fn read_key(key: flight::raw::Echo) -> usize {
        key.raw() as usize
    }

    pub(in crate::backend::kqueue::engine) fn accept(key: flight::raw::Echo) -> Self {
        Self(key.raw())
    }

    pub(in crate::backend::kqueue::engine) fn recv(key: flight::raw::Echo) -> Self {
        Self(key.raw())
    }

    pub(in crate::backend::kqueue::engine) fn recv_msg(key: flight::raw::Echo) -> Self {
        Self(key.raw())
    }

    pub(in crate::backend::kqueue::engine) const fn shutdown() -> Self {
        Self(u64::MAX)
    }

    pub(in crate::backend::kqueue::engine) fn into_kevent(self) -> *mut libc::c_void {
        self.0 as usize as *mut libc::c_void
    }
}
