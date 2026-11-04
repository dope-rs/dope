//! Connection-scoped stream I/O.

use std::task;

use dope::net::wire::{self, receive::Chunk, terminal};

use crate::{
    abi::{self, machine},
    context,
    manifold::app::dispatch::identity,
    net::{
        error::{self, read::Read},
        port::{result, table::Table},
        read,
    },
};

type Received<'a, 'w, 'd, W> =
    Result<Option<read::Lease<'a, 'w, 'd, W>>, Read<terminal::Error<'w, W>>>;

pub struct Io<'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> {
    port: &'scope Table<'d, Chunk<'w, 'd, W>, terminal::Error<'w, W>, I>,
    id: I,
}

impl<'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> Io<'scope, 'w, 'd, W, I> {
    pub(crate) fn new(
        port: &'scope Table<'d, Chunk<'w, 'd, W>, terminal::Error<'w, W>, I>,
        id: I,
    ) -> Self {
        Self { port, id }
    }

    pub(crate) fn handle(
        &self,
    ) -> (
        &'scope Table<'d, Chunk<'w, 'd, W>, terminal::Error<'w, W>, I>,
        I,
    ) {
        (self.port, self.id)
    }

    /// Borrowed bytes must remain valid for this driver's generative domain.
    pub fn write_all(
        &mut self,
        data: &'d [u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), error::Write<'d>>> + '_ {
        WriteAll::new(self, data)
    }

    /// Returns one retained wire cursor, or `None` after queued data drains at EOF.
    pub fn read<'io>(
        &'io mut self,
    ) -> impl abi::Fiber<'d, Output = Received<'io, 'w, 'd, W>> + 'io {
        Registration::new(self)
    }
}

impl<W: wire::Spec, I: identity::Identity> Drop for Io<'_, '_, '_, W, I> {
    fn drop(&mut self) {
        self.port.channel().close(self.id);
    }
}

struct Registration<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> {
    io: &'a mut Io<'scope, 'w, 'd, W, I>,
    registered: bool,
}

impl<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity>
    Registration<'a, 'scope, 'w, 'd, W, I>
{
    fn new(io: &'a mut Io<'scope, 'w, 'd, W, I>) -> Self {
        Self {
            io,
            registered: false,
        }
    }
}

impl<W: wire::Spec, I: identity::Identity> Drop for Registration<'_, '_, '_, '_, W, I> {
    fn drop(&mut self) {
        if self.registered {
            self.io.port.channel().clear_recv(self.io.id);
        }
    }
}

enum WriteState<'d> {
    Init(&'d [u8]),
    Waiting(&'d [u8]),
    Committed,
}

struct WriteAll<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> {
    io: &'a mut Io<'scope, 'w, 'd, W, I>,
    state: machine::Machine<WriteState<'d>>,
}

impl<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity>
    WriteAll<'a, 'scope, 'w, 'd, W, I>
{
    fn new(io: &'a mut Io<'scope, 'w, 'd, W, I>, payload: &'d [u8]) -> Self {
        Self {
            io,
            state: machine::Machine::new(WriteState::Init(payload)),
        }
    }
}

impl<W: wire::Spec, I: identity::Identity> Drop for WriteAll<'_, '_, '_, '_, W, I> {
    fn drop(&mut self) {
        let (port, id) = self.io.handle();
        match self.state.take() {
            Some(WriteState::Waiting(_)) => port.channel().clear_send(id),
            Some(WriteState::Committed) => port.channel().cancel_send(id),
            Some(WriteState::Init(_)) | None => {}
        }
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> abi::Fiber<'d>
    for WriteAll<'_, '_, 'w, 'd, W, I>
{
    type Output = Result<(), error::Write<'d>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        let (port, id) = this.io.handle();
        let wake = context.as_ref().completion();
        this.state.step(|state| match state {
            WriteState::Init(payload) | WriteState::Waiting(payload) => {
                if payload.is_empty() {
                    return machine::Step::Complete(task::Poll::Ready(Ok(())));
                }
                use crate::net::port::result::StageSend;
                match port.channel().try_send(id, payload, wake) {
                    StageSend::Staged => {
                        machine::Step::Suspend(WriteState::Committed, task::Poll::Pending)
                    }
                    StageSend::Busy => {
                        machine::Step::Suspend(WriteState::Waiting(payload), task::Poll::Pending)
                    }
                    StageSend::Failed(error) => {
                        machine::Step::Complete(task::Poll::Ready(Err(error)))
                    }
                }
            }
            WriteState::Committed => {
                use crate::net::port::result::SendStatus;
                match port.channel().send_status(id, wake) {
                    SendStatus::Complete => machine::Step::Complete(task::Poll::Ready(Ok(()))),
                    SendStatus::Failed(error) => {
                        machine::Step::Complete(task::Poll::Ready(Err(error)))
                    }
                    SendStatus::Pending => {
                        machine::Step::Suspend(WriteState::Committed, task::Poll::Pending)
                    }
                }
            }
        })
    }
}

impl<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> abi::Fiber<'d>
    for Registration<'a, 'scope, 'w, 'd, W, I>
{
    type Output = Received<'a, 'w, 'd, W>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let (port, id) = this.io.handle();
        let wake = context.as_ref().completion();
        use result::Recv;
        let result = match port
            .channel()
            .recv(id, context.as_mut().region_token(), wake)
        {
            Recv::Ready(cursor) => Ok(Some(read::Lease::new(cursor))),
            Recv::Closed => Ok(None),
            Recv::Failed(error) => Err(error),
            Recv::Pending => {
                this.registered = true;
                return task::Poll::Pending;
            }
        };
        this.registered = false;
        task::Poll::Ready(result)
    }
}
