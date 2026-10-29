use std::{
    marker::PhantomData,
    ops::ControlFlow,
    os::fd::{self, AsFd as _},
};

use dope_sys::{
    Errno,
    io::{
        reactor::{
            complete::Vacancy,
            ready::{self, Failure as _},
            retry::{self, Occupied as _, State as _},
        },
        socket::{self, Socket as _, addr},
    },
    table::{
        Files as _,
        change::{self, Interest as _},
    },
};
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

use crate::{
    Kqueue,
    output::{Data, Kind, Pending, Retry, State, retry::Prepared},
    store::table::Files,
};

impl<'a, C, E, P> retry::raw::Write<'a, C, E, P> for Retry<'a, C, E, P>
where
    C: retry::Context + 'a,
    C::Key: 'a,
    E: retry::Complete<C>,
    P: ready::Poll<
            C::Slots,
            Files = Files<C::Slots>,
            Failure: ready::Failure<Operation = C::Operation>,
        >,
{
    type Files = Files<C::Slots>;
    type Entry = Pending<C>;
    type State = State<Pending<C>>;
    type Send = Data<C>;
    type Failure = P::Failure;
    type Prepared = Prepared<'a, C, E>;

    fn new(files: &'a mut Self::Files, state: &'a mut Self::State, poll: &'a mut P) -> Self {
        Self {
            files,
            state,
            poll,
            event: PhantomData,
        }
    }

    fn prepare(self, key: C::Key) -> Result<Option<Self::Prepared>, Self::Failure> {
        self.poll.check()?;
        let Some(owner) = self.state.occupied(key) else {
            return Ok(None);
        };
        let interest = self
            .files
            .write(owner.record().file)
            .map_err(P::Failure::backing)?;
        Ok(Some(Prepared {
            owner,
            interest,
            event: PhantomData,
        }))
    }

    fn take_fd(&mut self, fd: fd::RawFd) -> ControlFlow<(), Option<C::Key>> {
        let Some(owner) = self.state.occupied_fd(fd) else {
            return ControlFlow::Continue(None);
        };
        match self.poll.execute(
            || self.files.write(owner.record().file),
            P::Failure::backing,
        ) {
            ControlFlow::Continue(Some(file)) => file.discard(),
            ControlFlow::Continue(None) => {}
            ControlFlow::Break(()) => return ControlFlow::Break(()),
        }
        ControlFlow::Continue(Some(owner.remove().target))
    }

    fn connect(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        addr: addr::Addr<'_>,
        vacant: impl Vacancy<Event = E>,
    ) {
        let (socket, file) = match Self::file(self.files, self.poll, slot, C::CONNECT) {
            ControlFlow::Continue(file) => file,
            ControlFlow::Break(()) => return,
        };
        let errno = match addr.try_connect::<Kqueue, Errno>(socket) {
            Ok(()) => {
                vacant.push(E::connected(key, C::connected(Ok(()))));
                return;
            }
            Err(errno) => errno,
        };
        if errno == Errno::IN_PROGRESS || errno.is_block() {
            let pending = Pending {
                target: key,
                file: file.id(),
                kind: Kind::Connect,
            };
            Self::insert(self.state, file, pending, errno, vacant);
            return;
        }
        vacant.push(E::connected(key, C::connected(Err(errno))));
    }

    fn shutdown(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        vacant: impl Vacancy<Event = E>,
    ) {
        let result = match self.files.borrow(slot) {
            Some(file) => Kqueue::shutdown(file, socket::How::WRITE),
            None => Err(Errno::BAD_DESCRIPTOR),
        };
        let result = result.map(|()| bounded::U32::<0, { MAX as u32 }>::exact::<0>().into());
        vacant.push(E::sent(key, C::sent(result)));
    }

    unsafe fn dispatch(
        &mut self,
        key: C::Key,
        error: Option<Errno>,
        vacant: impl Vacancy<Event = E>,
    ) {
        let Some(pending) = self.state.occupied(key).map(retry::Occupied::remove) else {
            return;
        };
        if let Some(errno) = error {
            vacant.push(pending.rejected(errno));
            return;
        }
        let op = match &pending.kind {
            Kind::Data(_) => C::SEND,
            Kind::Connect => C::CONNECT,
        };
        let file = match self.poll.execute(
            || self.files.try_socket(pending.file),
            |cause| P::Failure::missing(op, cause),
        ) {
            ControlFlow::Continue(file) => file.as_fd(),
            ControlFlow::Break(()) => return,
        };
        match pending.kind {
            Kind::Data(write) => {
                match unsafe { <Data<C> as retry::raw::Send<C>>::attempt(&write, file) } {
                    retry::Attempt::Ready(result) => vacant.push(E::sent(key, result)),
                    retry::Attempt::Blocked(errno) => {
                        self.arm(pending.file, key, Kind::Data(write), errno, vacant)
                    }
                }
            }
            Kind::Connect => {
                let result = match self.poll.execute(
                    || <Data<C> as retry::raw::Send<C>>::connected(file),
                    |errno| P::Failure::system(C::SOCKET_ERROR, errno),
                ) {
                    ControlFlow::Continue(retry::Attempt::Ready(result)) => result,
                    ControlFlow::Continue(retry::Attempt::Blocked(errno)) => {
                        self.arm(pending.file, key, Kind::Connect, errno, vacant);
                        return;
                    }
                    ControlFlow::Break(()) => return,
                };
                vacant.push(E::connected(key, result));
            }
        }
    }

    unsafe fn send(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        make: impl FnOnce() -> Data<C>,
        vacant: impl Vacancy<Event = E>,
    ) {
        let (socket, file) = match Self::file(self.files, self.poll, slot, C::SEND) {
            ControlFlow::Continue(file) => file,
            ControlFlow::Break(()) => return,
        };
        let write = make();
        match unsafe { <Data<C> as retry::raw::Send<C>>::attempt(&write, socket) } {
            retry::Attempt::Ready(result) => vacant.push(E::sent(key, result)),
            retry::Attempt::Blocked(errno) => {
                let pending = Pending {
                    target: key,
                    file: file.id(),
                    kind: Kind::Data(write),
                };
                Self::insert(self.state, file, pending, errno, vacant);
            }
        }
    }
}
