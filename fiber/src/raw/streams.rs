use std::io;
use std::mem::take;
use std::pin::Pin;
use std::task::Poll;

use o3::buffer::Shared;

use crate::io::Io;
use crate::net::port::result::{RecvInto, SendIdle};
use crate::{Context, Fiber};

#[derive(Clone, Copy)]
enum Interest {
    Recv,
    Send,
}

struct Registration<'a, 'scope, 'd> {
    io: &'a mut Io<'scope, 'd>,
    interest: Interest,
    armed: bool,
}

impl<'a, 'scope, 'd> Registration<'a, 'scope, 'd> {
    fn new(io: &'a mut Io<'scope, 'd>, interest: Interest) -> Self {
        Self {
            io,
            interest,
            armed: false,
        }
    }

    fn arm(&mut self, context: Pin<&mut Context<'_, 'd>>) {
        // SAFETY: the registration exclusively borrows the stream and clears
        // its port waiter on cancellation before the task context can drop.
        let waker = unsafe { context.waker_unchecked() };
        let (port, id) = self.io.handle();
        match self.interest {
            Interest::Recv => port.recv_waker(id, waker),
            Interest::Send => port.send_waker(id, waker),
        }
        self.armed = true;
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
        context: Pin<&mut Context<'_, 'd>>,
        dst: &mut [u8],
        done: &mut bool,
    ) -> Poll<io::Result<usize>> {
        let (port, id) = self.io.handle();
        let result = if dst.is_empty() {
            Poll::Ready(Ok(0))
        } else {
            match port.recv_into(id, dst) {
                RecvInto::Bytes(count) => Poll::Ready(Ok(count)),
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

impl Drop for Registration<'_, '_, '_> {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(crate) struct WriteAll<'a, 'scope, 'd> {
    registration: Registration<'a, 'scope, 'd>,
    data: &'a [u8],
    submitted: bool,
    done: bool,
}

impl<'a, 'scope, 'd> WriteAll<'a, 'scope, 'd> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd>, data: &'a [u8]) -> Self {
        Self {
            registration: Registration::new(io, Interest::Send),
            data,
            submitted: false,
            done: false,
        }
    }
}

impl<'d> Fiber<'d> for WriteAll<'_, '_, 'd> {
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

pub(crate) struct WriteAllShared<'a, 'scope, 'd> {
    registration: Registration<'a, 'scope, 'd>,
    bytes: Option<Shared>,
    done: bool,
}

impl<'a, 'scope, 'd> WriteAllShared<'a, 'scope, 'd> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd>, bytes: Shared) -> Self {
        Self {
            registration: Registration::new(io, Interest::Send),
            bytes: Some(bytes),
            done: false,
        }
    }
}

impl<'d> Fiber<'d> for WriteAllShared<'_, '_, 'd> {
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

pub(crate) struct Read<'a, 'scope, 'd> {
    registration: Registration<'a, 'scope, 'd>,
    buf: Vec<u8>,
    done: bool,
}

impl<'a, 'scope, 'd> Read<'a, 'scope, 'd> {
    pub(crate) fn new(io: &'a mut Io<'scope, 'd>, buf: Vec<u8>) -> Self {
        Self {
            registration: Registration::new(io, Interest::Recv),
            buf,
            done: false,
        }
    }
}

impl<'d> Fiber<'d> for Read<'_, '_, 'd> {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::read polled after completion");
        let result = this
            .registration
            .poll_recv(context, &mut this.buf, &mut this.done);
        match result {
            Poll::Ready(result) => Poll::Ready((result, take(&mut this.buf))),
            Poll::Pending => Poll::Pending,
        }
    }
}
