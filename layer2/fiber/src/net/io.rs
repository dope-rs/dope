//! Connection-scoped stream I/O.

use std::{mem, task};

use dope::net::wire::{self, receive::Chunk, terminal};
use o3::buffer::resident;

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
        WriteAll::new(self, resident::Shared::from(data))
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

#[repr(u8)]
enum Wake {
    Idle,
    Recv,
    Send,
}

struct Registration<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> {
    io: &'a mut Io<'scope, 'w, 'd, W, I>,
    wake: Wake,
}

impl<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity>
    Registration<'a, 'scope, 'w, 'd, W, I>
{
    fn new(io: &'a mut Io<'scope, 'w, 'd, W, I>) -> Self {
        Self {
            io,
            wake: Wake::Idle,
        }
    }

    fn complete(&mut self) {
        self.wake = Wake::Idle;
    }

    fn poll_send(
        &mut self,
        status: result::SendStatus<'d>,
    ) -> task::Poll<Result<(), error::Write<'d>>> {
        use crate::net::port::result::SendStatus;
        let result = match status {
            SendStatus::Complete => Ok(()),
            SendStatus::Failed(error) => Err(error),
            SendStatus::Pending => {
                self.wake = Wake::Send;
                return task::Poll::Pending;
            }
        };
        self.complete();
        task::Poll::Ready(result)
    }
}

impl<W: wire::Spec, I: identity::Identity> Drop for Registration<'_, '_, '_, '_, W, I> {
    fn drop(&mut self) {
        match self.wake {
            Wake::Idle => {}
            Wake::Recv => self.io.port.channel().clear_recv(self.io.id),
            Wake::Send => self.io.port.channel().clear_send(self.io.id),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Wake>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Wake>() == mem::align_of::<bool>());
};

enum WriteState<'d> {
    Owned(resident::Shared<'d>),
    Committed,
}

struct WriteAll<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> {
    registration: Registration<'a, 'scope, 'w, 'd, W, I>,
    state: machine::Machine<WriteState<'d>>,
}

impl<'a, 'scope, 'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity>
    WriteAll<'a, 'scope, 'w, 'd, W, I>
{
    pub(crate) fn new(io: &'a mut Io<'scope, 'w, 'd, W, I>, payload: resident::Shared<'d>) -> Self {
        Self {
            registration: Registration::new(io),
            state: machine::Machine::new(WriteState::Owned(payload)),
        }
    }
}

impl<W: wire::Spec, I: identity::Identity> Drop for WriteAll<'_, '_, '_, '_, W, I> {
    fn drop(&mut self) {
        let Some(WriteState::Committed) = self.state.take() else {
            return;
        };
        let (port, id) = self.registration.io.handle();
        port.channel().cancel_send(id);
        self.registration.complete();
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, I: identity::Identity> abi::Fiber<'d>
    for WriteAll<'_, '_, 'w, 'd, W, I>
{
    type Output = Result<(), error::Write<'d>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        let reg = &mut this.registration;
        let wake = context.as_ref().completion();
        this.state.step(|state| match state {
            WriteState::Owned(payload) => {
                if payload.as_ref().is_empty() {
                    reg.complete();
                    return machine::Step::Complete(task::Poll::Ready(Ok(())));
                }
                let (port, id) = reg.io.handle();
                use crate::net::port::result::{SendStatus, StageSend};
                match port.channel().try_send(id, payload, wake) {
                    StageSend::Staged => {
                        let result = reg.poll_send(SendStatus::Pending);
                        machine::Step::Suspend(WriteState::Committed, result)
                    }
                    StageSend::Busy(payload) => {
                        let result = reg.poll_send(SendStatus::Pending);
                        machine::Step::Suspend(WriteState::Owned(payload), result)
                    }
                    StageSend::Failed(error) => {
                        machine::Step::Complete(reg.poll_send(SendStatus::Failed(error)))
                    }
                }
            }
            WriteState::Committed => {
                let (port, id) = reg.io.handle();
                let result = reg.poll_send(port.channel().send_status(id, wake));
                if result.is_ready() {
                    machine::Step::Complete(result)
                } else {
                    machine::Step::Suspend(WriteState::Committed, result)
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
                this.wake = Wake::Recv;
                return task::Poll::Pending;
            }
        };
        this.complete();
        task::Poll::Ready(result)
    }
}
