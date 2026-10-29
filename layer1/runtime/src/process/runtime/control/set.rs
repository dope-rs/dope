use std::{iter, marker, mem, os::fd, time};

use dope_core::platform::{poll, signal};
use o3::alloc;

use crate::process::{failure::execute, runtime::worker::bound};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::process::runtime) struct Index<'a> {
    raw: usize,
    owner: marker::PhantomData<fn(&'a ()) -> &'a ()>,
}

pub(in crate::process::runtime) enum Ready<'signal, 'process, 'worker> {
    Signal(&'signal signal::Source<'process>),
    Worker(Index<'worker>),
}

pub(in crate::process::runtime) struct Both<'signal, 'process>(&'signal signal::Source<'process>);

pub(in crate::process::runtime) struct Workers;

pub(in crate::process::runtime) struct Seal(());

pub(in crate::process::runtime) trait Mode {
    type Ready<'a>;

    fn decode<'a>(&self, seal: Seal, raw: usize) -> Self::Ready<'a>;
}

impl<'signal, 'process> Mode for Both<'signal, 'process> {
    type Ready<'a> = Ready<'signal, 'process, 'a>;

    fn decode<'a>(&self, Seal(()): Seal, raw: usize) -> Self::Ready<'a> {
        match raw {
            0 => Ready::Signal(self.0),
            raw => Ready::Worker(Index::new(raw - 1)),
        }
    }
}

impl Mode for Workers {
    type Ready<'a> = Index<'a>;

    fn decode<'a>(&self, Seal(()): Seal, raw: usize) -> Self::Ready<'a> {
        Index::new(raw)
    }
}

impl<'a> Index<'a> {
    const fn new(raw: usize) -> Self {
        Self {
            raw,
            owner: marker::PhantomData,
        }
    }

    pub(in crate::process::runtime) const fn raw(self) -> usize {
        self.raw
    }
}

const _: () = {
    assert!(mem::size_of::<Index<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Index<'static>>() == mem::align_of::<usize>());
};

pub(in crate::process) struct Set<'worker, 'signal, M> {
    entries: poll::Set<'signal>,
    owners: marker::PhantomData<(Index<'worker>, fd::BorrowedFd<'signal>)>,
    mode: M,
}

impl<'signal, 'process, 'worker: 'signal> Set<'worker, 'signal, Both<'signal, 'process>> {
    pub(in crate::process) fn both<I, E, W>(
        signal: &'signal signal::Source<'process>,
        buses: &bound::Bound<'worker, I, E, W>,
    ) -> Result<Self, alloc::Error> {
        let entries = poll::Set::try_new(iter::once(fd::AsFd::as_fd(signal)), buses.fds())?;
        Ok(Self {
            entries,
            owners: marker::PhantomData,
            mode: Both(signal),
        })
    }
}

impl<'worker> Set<'worker, 'worker, Workers> {
    pub(in crate::process) fn bus<I, E, W>(
        buses: &bound::Bound<'worker, I, E, W>,
    ) -> Result<Self, alloc::Error> {
        let entries = poll::Set::try_new(iter::empty(), buses.fds())?;
        Ok(Self {
            entries,
            owners: marker::PhantomData,
            mode: Workers,
        })
    }
}

impl<'worker, 'signal, M> Set<'worker, 'signal, M>
where
    M: Mode,
{
    pub(in crate::process) fn wait<S>(self) -> Result<M::Ready<'worker>, execute::Cause<S>> {
        self.next().map(|(_, ready)| ready)
    }

    pub(in crate::process) fn next<S>(
        mut self,
    ) -> Result<(Self, M::Ready<'worker>), execute::Cause<S>> {
        loop {
            if let Some(ready) = self
                .poll(-1, |ready| (ready, false))
                .map_err(execute::Cause::Poll)?
            {
                return Ok((self, ready));
            }
        }
    }

    pub(in crate::process) fn wait_until<S, R>(
        mut self,
        deadline: time::Instant,
        mut receive: impl FnMut(M::Ready<'worker>) -> (R, bool),
    ) -> Result<(Self, Option<R>), execute::Cause<S>> {
        loop {
            let left = deadline.saturating_duration_since(time::Instant::now());
            let whole = left.as_millis();
            let partial = u128::from(!left.subsec_nanos().is_multiple_of(1_000_000));
            let millis = (whole + partial).min(i32::MAX as u128) as i32;
            match self
                .poll(millis, &mut receive)
                .map_err(execute::Cause::Poll)?
            {
                None => {
                    if time::Instant::now() >= deadline {
                        return Ok((self, None));
                    }
                }
                Some(ready) => return Ok((self, Some(ready))),
            }
        }
    }
}

impl<'worker, 'signal, M: Mode> Set<'worker, 'signal, M> {
    fn poll<R>(
        &mut self,
        timeout: i32,
        receive: impl FnOnce(M::Ready<'worker>) -> (R, bool),
    ) -> Result<Option<R>, dope_core::Errno> {
        match self.entries.poll(timeout) {
            Err(errno) if errno.is_intr() => Ok(None),
            Err(errno) => Err(errno),
            Ok(None) => Ok(None),
            Ok(Some(ready)) => {
                let (output, retire) = receive(self.mode.decode(Seal(()), ready.index()));
                if retire {
                    ready.retire();
                }
                Ok(Some(output))
            }
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Set<'static, 'static, Both<'static, 'static>>>()
            == mem::size_of::<(poll::Set<'static>, &signal::Source<'static>)>()
    );
    assert!(
        mem::align_of::<Set<'static, 'static, Both<'static, 'static>>>()
            == mem::align_of::<(poll::Set<'static>, &signal::Source<'static>)>()
    );
    assert!(
        mem::size_of::<Set<'static, 'static, Workers>>() == mem::size_of::<poll::Set<'static>>()
    );
    assert!(
        mem::align_of::<Set<'static, 'static, Workers>>() == mem::align_of::<poll::Set<'static>>()
    );
};
