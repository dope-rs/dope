use std::io;
use std::mem::take;
use std::pin::Pin;
use std::task::Poll;

use o3::buffer::Shared;

use crate::io::Io;
use crate::net::port::result::{RecvInto, SendIdle};
use crate::raw::task::CompletionRegistrar;
use crate::{Context, Fiber};
use dope::driver::ready::CompletionWaker;
use dope_net::wire::{RecvTarget, Wire};

#[derive(Clone, Copy)]
enum Interest {
    Recv,
    Send,
}

struct Registration<'a, 'scope, 'd, W: Wire> {
    io: &'a mut Io<'scope, 'd, W>,
    interest: Interest,
    armed: bool,
}

impl<'a, 'scope, 'd, W: Wire> Registration<'a, 'scope, 'd, W> {
    fn new(io: &'a mut Io<'scope, 'd, W>, interest: Interest) -> Self {
        Self {
            io,
            interest,
            armed: false,
        }
    }

    fn retain(&mut self, wake: CompletionWaker<'d>) {
        self.armed = true;
        let (port, id) = self.io.handle();
        match self.interest {
            Interest::Recv => port.recv_waker(id, wake),
            Interest::Send => port.send_waker(id, wake),
        }
    }

    fn arm(&mut self, context: Pin<&mut Context<'_, 'd>>) {
        context.as_ref().register_completion(&mut *self);
    }

    fn clear(&mut self) {
        if !self.armed {
            return;
        }
        let (port, id) = self.io.handle();
        match self.interest {
            Interest::Recv => port.clear_recv_waker(id),
            Interest::Send => port.clear_send_waker(id),
        }
        self.armed = false;
    }

    fn complete(&mut self) {
        self.armed = false;
    }

    fn poll_recv(
        &mut self,
        mut context: Pin<&mut Context<'_, 'd>>,
        target: &mut RecvTarget<'_>,
        done: &mut bool,
    ) -> Poll<io::Result<()>> {
        let (port, id) = self.io.handle();
        let result = if target.remaining() == 0 {
            Poll::Ready(Ok(()))
        } else {
            match port.recv_into(id, target, context.as_mut().region_token()) {
                RecvInto::Ready => Poll::Ready(Ok(())),
                RecvInto::Failed(error) => Poll::Ready(Err(error)),
                RecvInto::Pending => {
                    self.arm(context);
                    Poll::Pending
                }
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
        context: Pin<&mut Context<'_, 'd>>,
        done: &mut bool,
    ) -> Poll<io::Result<()>> {
        let (port, id) = self.io.handle();
        let result = match port.send_idle(id) {
            SendIdle::Idle => Poll::Ready(Ok(())),
            SendIdle::Failed(error) => Poll::Ready(Err(error)),
            SendIdle::Pending => {
                self.arm(context);
                Poll::Pending
            }
        };
        if result.is_ready() {
            self.complete();
            *done = true;
        }
        result
    }
}

// SAFETY: Registration exclusively borrows the stream and clears its exact
// port waiter on cancellation or Drop before the task context can disappear.
unsafe impl<'d, W: Wire> CompletionRegistrar<'d> for &mut Registration<'_, '_, 'd, W> {
    type Output = ();

    fn register(self, wake: CompletionWaker<'d>) {
        self.retain(wake);
    }
}

impl<W: Wire> Drop for Registration<'_, '_, '_, W> {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(crate) struct WriteAll<'a, 'scope, 'd, W: Wire> {
    registration: Registration<'a, 'scope, 'd, W>,
    data: &'a [u8],
    submitted: bool,
    done: bool,
}

impl<'a, 'scope, 'd, W: Wire> WriteAll<'a, 'scope, 'd, W> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd, W>, data: &'a [u8]) -> Self {
        Self {
            registration: Registration::new(io, Interest::Send),
            data,
            submitted: false,
            done: false,
        }
    }
}

impl<'d, W: Wire> Fiber<'d> for WriteAll<'_, '_, 'd, W> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::write_all polled after completion");
        if !this.submitted {
            if this.data.is_empty() {
                this.done = true;
                return Poll::Ready(Ok(()));
            }
            let (port, id) = this.registration.io.handle();
            port.send(id, Shared::copy_from_slice(this.data));
            this.submitted = true;
        }
        this.registration.poll_send(context, &mut this.done)
    }
}

pub(crate) struct WriteAllShared<'a, 'scope, 'd, W: Wire> {
    registration: Registration<'a, 'scope, 'd, W>,
    bytes: Option<Shared>,
    done: bool,
}

impl<'a, 'scope, 'd, W: Wire> WriteAllShared<'a, 'scope, 'd, W> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd, W>, bytes: Shared) -> Self {
        Self {
            registration: Registration::new(io, Interest::Send),
            bytes: Some(bytes),
            done: false,
        }
    }
}

impl<'d, W: Wire> Fiber<'d> for WriteAllShared<'_, '_, 'd, W> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !this.done,
            "fiber::Io::write_all_shared polled after completion"
        );
        if let Some(bytes) = this.bytes.take() {
            if bytes.is_empty() {
                this.done = true;
                return Poll::Ready(Ok(()));
            }
            let (port, id) = this.registration.io.handle();
            port.send(id, bytes);
        }
        this.registration.poll_send(context, &mut this.done)
    }
}

pub(crate) struct Read<'a, 'scope, 'd, W: Wire> {
    registration: Registration<'a, 'scope, 'd, W>,
    buf: Vec<u8>,
    done: bool,
}

impl<'a, 'scope, 'd, W: Wire> Read<'a, 'scope, 'd, W> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd, W>, buf: Vec<u8>) -> Self {
        Self {
            registration: Registration::new(io, Interest::Recv),
            buf,
            done: false,
        }
    }
}

impl<'d, W: Wire> Fiber<'d> for Read<'_, '_, 'd, W> {
    type Output = (io::Result<()>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::read polled after completion");
        let mut target = RecvTarget::new(&mut this.buf);
        let result = this
            .registration
            .poll_recv(context, &mut target, &mut this.done);
        match result {
            Poll::Ready(result) => Poll::Ready((result, take(&mut this.buf))),
            Poll::Pending => Poll::Pending,
        }
    }
}
