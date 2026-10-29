use std::ops;

use dope_sys::{
    Errno,
    io::{
        reactor::{
            complete, read,
            ready::{
                self,
                delivery::{self, Decoded},
                store::{self, Parts, State as _},
            },
            retry,
        },
        socket::option::{self, Source as _, Step as _},
    },
    table::{Files as _, change, registry},
};

use crate::Kqueue;

pub struct State<C: store::Context> {
    retries: C::Writes,
    files: C::Files,
    poll: C::Poll,
    reads: C::Reads,
    recv: C::Pool,
}

impl<C: store::Context> store::State<C> for State<C> {
    fn new(
        retries: C::Writes,
        files: C::Files,
        poll: C::Poll,
        reads: C::Reads,
        recv: C::Pool,
    ) -> Self {
        Self {
            retries,
            files,
            poll,
            reads,
            recv,
        }
    }

    fn view(&self) -> Parts<&C::Writes, &C::Files, &C::Poll, &C::Reads, &C::Pool> {
        Parts {
            retries: &self.retries,
            files: &self.files,
            poll: &self.poll,
            reads: &self.reads,
            recv: &self.recv,
        }
    }

    fn edit(
        &mut self,
    ) -> Parts<&mut C::Writes, &mut C::Files, &mut C::Poll, &mut C::Reads, &C::Pool> {
        Parts {
            retries: &mut self.retries,
            files: &mut self.files,
            poll: &mut self.poll,
            reads: &mut self.reads,
            recv: &self.recv,
        }
    }

    fn set<O: option::Cursor>(
        &self,
        slot: <C::Slots as change::raw::Slots>::Physical,
        options: Option<O>,
    ) -> Result<(), Errno> {
        let Some(fd) = self.files.borrow(slot) else {
            return Err(Errno::BAD_DESCRIPTOR);
        };
        let Some(mut options) = options else {
            return Ok(());
        };
        loop {
            let option = options.pop();
            match option.kind() {
                ops::ControlFlow::Continue(kind) => Kqueue::set(fd, kind, option.value())?,
                ops::ControlFlow::Break(kind) => return Kqueue::set(fd, kind, option.value()),
            }
        }
    }

    fn read<'a>(&'a mut self) -> C::Arm<'a>
    where
        C: store::Exec + 'a,
    {
        read::Arm::new(&mut self.files, &mut self.reads, &mut self.poll)
    }

    fn write<'a>(&'a mut self) -> C::Retry<'a>
    where
        C: store::Exec + 'a,
    {
        retry::raw::Write::new(&mut self.files, &mut self.retries, &mut self.poll)
    }

    fn poll<'a>(&'a mut self, query: registry::Query<C::Read>) -> Option<C::ReadPoll<'a>>
    where
        C: store::Exec + 'a,
    {
        let arm = read::Arm::new(&mut self.files, &mut self.reads, &mut self.poll);
        read::raw::Poll::new(arm, &self.recv, query)
    }
}

pub(super) unsafe fn dispatch<C>(
    state: &mut C::State,
    vacant: impl complete::Vacancy<Event = <C::Context as store::Exec>::Event>,
    event: impl delivery::Event,
) where
    C: ready::Driver<Context: ready::raw::Decode>,
{
    let filter = event.filter();
    let error = event.error();
    let data = unsafe { <C::Context as ready::raw::Decode>::decode(event.data()) };
    if filter == libc::EVFILT_WRITE {
        if let Decoded::Public(target) = data {
            unsafe { retry::raw::Write::dispatch(&mut state.write(), target, error, vacant) };
        }
        return;
    }
    match data {
        Decoded::Public(key) => {
            if let Some(poll) = state.poll(registry::Query::Echo(key)) {
                unsafe { read::raw::Poll::dispatch(poll, filter, error, vacant) };
            }
        }
        Decoded::Shutdown => vacant.push(<C::Context as ready::raw::Decode>::SHUTDOWN),
        Decoded::Empty => {}
    }
}
