use std::mem;

use dope_core::{
    driver,
    driver::{
        retained::flight::cancel,
        route::{Identity, KeyTag},
        schedule::ready,
    },
    io::{
        event::net::{connect, tuning},
        fd::handles,
        socket::{option, pending},
    },
};

use crate::link::{
    duplex::state,
    pool::{self, change::close},
};

pub(crate) enum Setup<'d, ID: Identity> {
    CreatingTargeted(Targeted<'d, ID, handles::Creation<'d>>),
    Idle(handles::Descriptor<'d>),
    IdleTargeted(Targeted<'d, ID, handles::Descriptor<'d>>),
    Tuning(pending::Tuning<'d>),
    TuningCancelled(pending::Tuning<'d, pending::Cancelled>),
    Connecting(Targeted<'d, ID, pending::Connection<'d>>, Intent),
    ConnectingCancelled(Targeted<'d, ID, pending::Connection<'d, pending::Cancelled>>),
    Done(handles::Descriptor<'d>),
    Connected(Targeted<'d, ID, handles::Descriptor<'d>>),
    Detached(ready::Handle<'d>),
}

#[derive(Clone, Copy)]
pub(crate) enum Intent {
    Live,
    Rejected,
}

pub(crate) struct Targeted<'d, ID: Identity, T> {
    inner: T,
    stored: pool::Stored<'d, ID>,
}

pub(crate) enum Close<'d, ID: Identity> {
    Creating(Targeted<'d, ID, handles::Creation<'d>>),
    Idle(handles::Descriptor<'d>),
    Done(handles::Descriptor<'d>),
    IdleTargeted(Targeted<'d, ID, handles::Descriptor<'d>>),
    Connected(Targeted<'d, ID, handles::Descriptor<'d>>),
}

pub(crate) enum Completion<C> {
    Ready,
    Failed(dope_core::Errno),
    Cancelled,
    Stale(C),
}

pub(crate) enum Submission {
    Pending,
    Failed(pending::Cause),
}

pub(crate) enum Cancellation {
    Ready,
    Pending,
    Retry,
    Rejected(cancel::Failure),
}

pub(crate) enum Retire<'d, ID: Identity> {
    Ready {
        close: Close<'d, ID>,
        handle: ready::Handle<'d>,
    },
    Pending(ready::Target<'d>),
    Unavailable,
}

impl<'d, ID: Identity> Setup<'d, ID> {
    pub(in crate::link) fn take(&mut self) -> Self {
        let detached = Self::Detached(self.ready_handle());
        mem::replace(self, detached)
    }

    pub(crate) fn targeted(socket: handles::Creation<'d>, stored: pool::Stored<'d, ID>) -> Self {
        Self::CreatingTargeted(Targeted::new(socket, stored))
    }

    pub(in crate::link) fn tuned(
        self,
        done: tuning::Completion<'d>,
        state: &state::State,
    ) -> (Self, Completion<tuning::Completion<'d>>) {
        match self {
            Self::Tuning(item) => {
                let (fd, result) = match item.complete(done) {
                    Ok(pair) => pair,
                    Err(mismatch) => {
                        let (item, done) = mismatch.into_parts();
                        return (Self::Tuning(item), Completion::Stale(done));
                    }
                };
                match result {
                    Ok(()) if state.is_closing() => (Self::Idle(fd), Completion::Cancelled),
                    Ok(()) => (Self::Done(fd), Completion::Ready),
                    Err(error) => (Self::Idle(fd), Completion::Failed(error)),
                }
            }
            Self::TuningCancelled(item) => {
                let (fd, result) = match item.complete(done) {
                    Ok(pair) => pair,
                    Err(mismatch) => {
                        let (item, done) = mismatch.into_parts();
                        return (Self::TuningCancelled(item), Completion::Stale(done));
                    }
                };
                match result {
                    Ok(()) => (Self::Idle(fd), Completion::Cancelled),
                    Err(error) => (Self::Idle(fd), Completion::Failed(error)),
                }
            }
            state => (state, Completion::Stale(done)),
        }
    }

    pub(in crate::link) fn created(
        self,
        made: handles::CreatedSlot<'d>,
        opts: option::Set,
        submit: impl FnOnce(
            handles::Descriptor<'d>,
            option::Set,
        ) -> Result<pending::Connection<'d>, pending::Rejection<'d>>,
    ) -> (Self, Submission) {
        let targeted = match self {
            Self::CreatingTargeted(targeted) => targeted,
            state => return (state, Submission::Pending),
        };
        let (socket, stored) = targeted.into_parts();
        let fd = match made.try_join(socket) {
            Ok(join) => join.activate(),
            Err((socket, made)) => {
                drop(made);
                return (
                    Self::CreatingTargeted(Targeted::new(socket, stored)),
                    Submission::Pending,
                );
            }
        };
        match submit(fd, opts) {
            Ok(conn) => (
                Self::Connecting(Targeted::new(conn, stored), Intent::Live),
                Submission::Pending,
            ),
            Err(reject) => {
                let (fd, cause) = reject.into_parts();
                (
                    Self::IdleTargeted(Targeted::new(fd, stored)),
                    Submission::Failed(cause),
                )
            }
        }
    }

    pub(crate) fn complete(
        self,
        done: connect::Completion<'d>,
    ) -> (Self, Completion<connect::Completion<'d>>) {
        if let Self::Connecting(targeted, intent) = self {
            let (item, stored) = targeted.into_parts();
            let (fd, result) = match item.complete(done) {
                Ok(pair) => pair,
                Err(mismatch) => {
                    let (item, done) = mismatch.into_parts();
                    return (
                        Self::Connecting(Targeted::new(item, stored), intent),
                        Completion::Stale(done),
                    );
                }
            };
            return match result {
                Ok(()) => (
                    Self::Connected(Targeted::new(fd, stored)),
                    match intent {
                        Intent::Live => Completion::Ready,
                        Intent::Rejected => Completion::Cancelled,
                    },
                ),
                Err(error) => (
                    Self::IdleTargeted(Targeted::new(fd, stored)),
                    Completion::Failed(error),
                ),
            };
        }
        if let Self::ConnectingCancelled(targeted) = self {
            let (item, stored) = targeted.into_parts();
            let (fd, result) = match item.complete(done) {
                Ok(pair) => pair,
                Err(mismatch) => {
                    let (item, done) = mismatch.into_parts();
                    return (
                        Self::ConnectingCancelled(Targeted::new(item, stored)),
                        Completion::Stale(done),
                    );
                }
            };
            return match result {
                Ok(()) => (
                    Self::Connected(Targeted::new(fd, stored)),
                    Completion::Cancelled,
                ),
                Err(error) => (
                    Self::IdleTargeted(Targeted::new(fd, stored)),
                    Completion::Failed(error),
                ),
            };
        }
        (self, Completion::Stale(done))
    }

    pub(crate) fn fd(&self) -> Option<&handles::Descriptor<'d>> {
        if let Self::Idle(fd) | Self::Done(fd) = self {
            return Some(fd);
        }
        if let Self::IdleTargeted(targeted) | Self::Connected(targeted) = self {
            return Some(targeted.inner());
        }
        self.pending_fd()
    }

    pub(crate) fn ready_handle(&self) -> ready::Handle<'d> {
        match self {
            Self::CreatingTargeted(targeted) => targeted.inner().ready_handle(),
            Self::Idle(fd) | Self::Done(fd) => fd.ready_handle(),
            Self::IdleTargeted(targeted) | Self::Connected(targeted) => {
                targeted.inner().ready_handle()
            }
            Self::Tuning(item) => item.fd().ready_handle(),
            Self::TuningCancelled(item) => item.fd().ready_handle(),
            Self::Connecting(targeted, _) => targeted.inner().fd().ready_handle(),
            Self::ConnectingCancelled(targeted) => targeted.inner().fd().ready_handle(),
            Self::Detached(handle) => *handle,
        }
    }

    pub(crate) fn retire(self) -> (Self, Retire<'d, ID>) {
        let (handle, close) = match self {
            Self::CreatingTargeted(targeted) => {
                (targeted.inner().ready_handle(), Close::Creating(targeted))
            }
            Self::Idle(fd) => (fd.ready_handle(), Close::Idle(fd)),
            Self::Done(fd) => (fd.ready_handle(), Close::Done(fd)),
            Self::IdleTargeted(targeted) => (
                targeted.inner().ready_handle(),
                Close::IdleTargeted(targeted),
            ),
            Self::Connected(targeted) => {
                (targeted.inner().ready_handle(), Close::Connected(targeted))
            }
            Self::Tuning(item) => {
                let target = item.fd().ready_handle().target();
                return (Self::Tuning(item), Retire::Pending(target));
            }
            Self::TuningCancelled(item) => {
                let target = item.fd().ready_handle().target();
                return (Self::TuningCancelled(item), Retire::Pending(target));
            }
            Self::Connecting(targeted, intent) => {
                let target = targeted.inner().fd().ready_handle().target();
                return (Self::Connecting(targeted, intent), Retire::Pending(target));
            }
            Self::ConnectingCancelled(targeted) => {
                let target = targeted.inner().fd().ready_handle().target();
                return (Self::ConnectingCancelled(targeted), Retire::Pending(target));
            }
            Self::Detached(handle) => {
                return (Self::Detached(handle), Retire::Unavailable);
            }
        };
        (Self::Detached(handle), Retire::Ready { close, handle })
    }

    pub(crate) fn pending_fd(&self) -> Option<&handles::Descriptor<'d>> {
        if let Self::Tuning(item) = self {
            return Some(item.fd());
        }
        if let Self::TuningCancelled(item) = self {
            return Some(item.fd());
        }
        if let Self::Connecting(targeted, _) = self {
            return Some(targeted.inner().fd());
        }
        if let Self::ConnectingCancelled(targeted) = self {
            return Some(targeted.inner().fd());
        }
        None
    }

    pub(crate) fn cancel(self, driver: &mut driver::Context<'_, 'd>) -> (Self, Cancellation) {
        match self {
            Self::Tuning(item) => match item.cancel(driver) {
                pending::Cancellation::Submitted(item) => {
                    (Self::TuningCancelled(item), Cancellation::Pending)
                }
                pending::Cancellation::Retry { item, error: _ } => {
                    (Self::Tuning(item), Cancellation::Retry)
                }
            },
            Self::Connecting(targeted, Intent::Live) => {
                let (item, stored) = targeted.into_parts();
                match item.try_cancel(driver) {
                    Ok(item) => (
                        Self::ConnectingCancelled(Targeted::new(item, stored)),
                        Cancellation::Pending,
                    ),
                    Err((item, cause)) => {
                        let targeted = Targeted::new(item, stored);
                        match cause {
                            cancel::Error::Full(_) => (
                                Self::Connecting(targeted, Intent::Live),
                                Cancellation::Retry,
                            ),
                            cancel::Error::Rejected(cause) => (
                                Self::Connecting(targeted, Intent::Rejected),
                                Cancellation::Rejected(cause),
                            ),
                        }
                    }
                }
            }
            Self::Connecting(targeted, Intent::Rejected) => (
                Self::Connecting(targeted, Intent::Rejected),
                Cancellation::Pending,
            ),
            Self::TuningCancelled(item) => (Self::TuningCancelled(item), Cancellation::Pending),
            Self::ConnectingCancelled(targeted) => {
                (Self::ConnectingCancelled(targeted), Cancellation::Pending)
            }
            state => (state, Cancellation::Ready),
        }
    }

    pub(in crate::link) fn established(&self) -> Option<&handles::Descriptor<'d>> {
        if let Self::Done(fd) = self {
            return Some(fd);
        }
        self.connected()
    }

    pub(in crate::link) fn connected(&self) -> Option<&handles::Descriptor<'d>> {
        if let Self::Connected(targeted) = self {
            return Some(targeted.inner());
        }
        None
    }
}

impl<'d, ID: Identity, T> Targeted<'d, ID, T> {
    const fn new(inner: T, stored: pool::Stored<'d, ID>) -> Self {
        Self { inner, stored }
    }

    const fn inner(&self) -> &T {
        &self.inner
    }

    fn into_parts(self) -> (T, pool::Stored<'d, ID>) {
        (self.inner, self.stored)
    }
}

impl<'d, ID: Identity> Close<'d, ID> {
    pub(crate) const fn kind(&self) -> close::Kind {
        match self {
            Self::Done(_) => close::Kind::Connected,
            Self::Creating(_) | Self::Idle(_) | Self::IdleTargeted(_) | Self::Connected(_) => {
                close::Kind::Opening
            }
        }
    }

    pub(crate) fn restore(self) -> Setup<'d, ID> {
        match self {
            Self::Creating(value) => Setup::CreatingTargeted(value),
            Self::Idle(value) => Setup::Idle(value),
            Self::Done(value) => Setup::Done(value),
            Self::IdleTargeted(value) => Setup::IdleTargeted(value),
            Self::Connected(value) => Setup::Connected(value),
        }
    }

    pub(crate) fn close(self, driver: &mut driver::Context<'_, 'd>) {
        match self {
            Self::Creating(targeted) => targeted.inner.detach(),
            Self::Idle(fd) | Self::Done(fd) => fd.close(driver),
            Self::IdleTargeted(targeted) | Self::Connected(targeted) => {
                targeted.inner.close(driver)
            }
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Targeted<'_, KeyTag<0>, handles::Descriptor<'_>>>()
            == mem::size_of::<handles::Descriptor<'_>>()
    );
    assert!(mem::size_of::<Setup<'_, KeyTag<0>>>() == 4 * mem::size_of::<usize>());
    assert!(
        mem::align_of::<Setup<'_, KeyTag<0>>>()
            == mem::align_of::<Option<pending::Connection<'_>>>()
    );
};
