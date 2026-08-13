//! Connection-scoped stream I/O.

use std::{io, mem, pin, process, task};

use dope::{
    core::driver::schedule::ready::completion,
    manifold::dispatch::typed::identity,
    net::{link::egress::data, wire},
};

use crate::{
    abi, context,
    net::{
        port::{self, result},
        read,
    },
};

pub struct Io<'scope, 'd, W: wire::Wire, I: identity::Identity> {
    port: &'scope port::Table<'d, W::RetainedRecv<'d>, I>,
    id: I,
}

impl<'scope, 'd, W: wire::Wire, I: identity::Identity> Io<'scope, 'd, W, I> {
    pub(crate) fn new(port: &'scope port::Table<'d, W::RetainedRecv<'d>, I>, id: I) -> Self {
        Self { port, id }
    }

    pub(crate) fn handle(&self) -> (&'scope port::Table<'d, W::RetainedRecv<'d>, I>, I) {
        (self.port, self.id)
    }

    /// Borrowed bytes must remain valid for this driver's generative domain.
    pub fn write_all(
        &mut self,
        data: impl Into<data::Buffer<'d>>,
    ) -> impl abi::Fiber<'d, Output = io::Result<()>> + '_ {
        WriteAll::new(self, data.into())
    }

    /// Returns one retained wire cursor, or `None` after queued data drains at EOF.
    pub fn read<'io>(
        &'io mut self,
    ) -> impl abi::Fiber<'d, Output = io::Result<Option<read::Lease<'io, 'd, W>>>> + 'io {
        Read::new(self)
    }
}

impl<W: wire::Wire, I: identity::Identity> Drop for Io<'_, '_, W, I> {
    fn drop(&mut self) {
        self.port.channel().close(self.id);
    }
}

#[derive(Clone, Copy)]
enum Interest {
    Recv,
    Send,
}

struct Registration<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> {
    io: &'a mut Io<'scope, 'd, W, I>,
    interest: Interest,
    armed: bool,
}

impl<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> Registration<'a, 'scope, 'd, W, I> {
    fn new(io: &'a mut Io<'scope, 'd, W, I>, interest: Interest) -> Self {
        Self {
            io,
            interest,
            armed: false,
        }
    }

    fn retain(&mut self, wake: completion::Waker<'d>) {
        self.armed = true;
        let (port, id) = self.io.handle();
        match self.interest {
            Interest::Recv => port.channel().recv_waker(id, wake),
            Interest::Send => port.channel().send_waker(id, wake),
        }
    }

    fn arm(&mut self, context: pin::Pin<&mut context::Context<'_, 'd>>) {
        self.retain(context.as_ref().completion_waker());
    }

    fn clear(&mut self) {
        if !self.armed {
            return;
        }
        let (port, id) = self.io.handle();
        match self.interest {
            Interest::Recv => port.channel().clear_recv_waker(id),
            Interest::Send => port.channel().clear_send_waker(id),
        }
        self.armed = false;
    }

    fn complete(&mut self) {
        self.armed = false;
    }

    fn poll_recv(
        &mut self,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
        done: &mut bool,
    ) -> task::Poll<io::Result<Option<read::Lease<'a, 'd, W>>>> {
        let (port, id) = self.io.handle();
        use result::Recv;
        let result = match port.channel().recv(id, context.as_mut().region_token()) {
            Recv::Ready(cursor) => task::Poll::Ready(Ok(Some(read::Lease::new(cursor)))),
            Recv::Closed => task::Poll::Ready(Ok(None)),
            Recv::Failed(error) => task::Poll::Ready(Err(error.into())),
            Recv::Pending => {
                self.arm(context);
                task::Poll::Pending
            }
        };
        if result.is_ready() {
            self.complete();
            *done = true;
        }
        result
    }

    fn poll_send(
        &mut self,
        status: result::SendStatus,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<io::Result<()>> {
        use crate::net::port::result::SendStatus;
        let result = match status {
            SendStatus::Complete => task::Poll::Ready(Ok(())),
            SendStatus::Failed(error) => task::Poll::Ready(Err(error.into())),
            SendStatus::Pending => {
                self.arm(context);
                task::Poll::Pending
            }
        };
        if result.is_ready() {
            self.complete();
        }
        result
    }
}

impl<W: wire::Wire, I: identity::Identity> Drop for Registration<'_, '_, '_, W, I> {
    fn drop(&mut self) {
        self.clear();
    }
}

enum WriteState<'d> {
    Owned(data::Buffer<'d>),
    Committed,
    Done,
}

struct WriteAll<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> {
    registration: Registration<'a, 'scope, 'd, W, I>,
    state: WriteState<'d>,
}

impl<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> WriteAll<'a, 'scope, 'd, W, I> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd, W, I>, payload: data::Buffer<'d>) -> Self {
        Self {
            registration: Registration::new(io, Interest::Send),
            state: WriteState::Owned(payload),
        }
    }
}

impl<W: wire::Wire, I: identity::Identity> Drop for WriteAll<'_, '_, '_, W, I> {
    fn drop(&mut self) {
        if matches!(&self.state, WriteState::Committed) {
            let (port, id) = self.registration.io.handle();
            port.channel().cancel_staged_send(id);
        }
    }
}

impl<'d, W: wire::Wire, I: identity::Identity> abi::Fiber<'d> for WriteAll<'_, '_, 'd, W, I> {
    type Output = io::Result<()>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        loop {
            match mem::replace(&mut this.state, WriteState::Done) {
                WriteState::Owned(payload) => {
                    if payload.as_ref().is_empty() {
                        this.registration.complete();
                        return task::Poll::Ready(Ok(()));
                    }
                    let (port, id) = this.registration.io.handle();
                    use crate::net::port::result::{SendStatus, StageSend};
                    match port.channel().try_stage_send(id, payload) {
                        StageSend::Staged => {
                            this.state = WriteState::Committed;
                            return this
                                .registration
                                .poll_send(SendStatus::Pending, context.as_mut());
                        }
                        StageSend::Busy(payload) => {
                            let status = port.channel().send_status(id);
                            match this.registration.poll_send(status, context.as_mut()) {
                                task::Poll::Ready(Ok(())) => {
                                    this.state = WriteState::Owned(payload);
                                }
                                task::Poll::Ready(Err(error)) => {
                                    return task::Poll::Ready(Err(error));
                                }
                                task::Poll::Pending => {
                                    this.state = WriteState::Owned(payload);
                                    return task::Poll::Pending;
                                }
                            }
                        }
                        StageSend::Failed(error) => {
                            return this
                                .registration
                                .poll_send(SendStatus::Failed(error), context.as_mut());
                        }
                    }
                }
                WriteState::Committed => {
                    let status = {
                        let (port, id) = this.registration.io.handle();
                        port.channel().send_status(id)
                    };
                    this.state = WriteState::Committed;
                    let result = this.registration.poll_send(status, context.as_mut());
                    if result.is_ready() {
                        let (port, id) = this.registration.io.handle();
                        port.channel().cancel_staged_send(id);
                        this.state = WriteState::Done;
                    }
                    return result;
                }
                WriteState::Done => process::abort(),
            }
        }
    }
}

struct Read<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> {
    registration: Registration<'a, 'scope, 'd, W, I>,
    done: bool,
}

impl<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> Read<'a, 'scope, 'd, W, I> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd, W, I>) -> Self {
        Self {
            registration: Registration::new(io, Interest::Recv),
            done: false,
        }
    }
}

impl<'a, 'scope, 'd, W: wire::Wire, I: identity::Identity> abi::Fiber<'d>
    for Read<'a, 'scope, 'd, W, I>
{
    type Output = io::Result<Option<read::Lease<'a, 'd, W>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        if this.done {
            process::abort();
        }
        this.registration.poll_recv(context, &mut this.done)
    }
}
