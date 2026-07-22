use std::io;
use std::marker::PhantomData;
use std::mem;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::Poll;

use dope::ProvidedView;
use o3::buffer::{Bytes, Retained, Shared};

use crate::net::port::Port;
use crate::net::port::result::{RecvChunkResult, RecvInto, SendIdle};
use crate::{Context, Fiber, Waker};
use dope::driver::token::Token;

pub trait Host<'d> {
    fn port(&self) -> &Port<'d>;

    fn recv_into(&self, id: Token, dst: &mut [u8]) -> RecvInto {
        self.port().recv_into(id, dst)
    }

    fn recv_chunk(&self, id: Token) -> RecvChunkResult<'d> {
        self.port().recv_chunk(id)
    }

    fn recv_waker(&self, id: Token, waker: Waker<'d>) {
        self.port().recv_waker(id, waker);
    }

    fn clear_recv_waker(&self, id: Token) {
        self.port().clear_recv_waker(id);
    }

    fn send_waker(&self, id: Token, waker: Waker<'d>) {
        self.port().send_waker(id, waker);
    }

    fn clear_send_waker(&self, id: Token) {
        self.port().clear_send_waker(id);
    }

    fn send(&self, id: Token, bytes: Shared) {
        self.port().send(id, bytes);
    }

    fn send_idle(&self, id: Token) -> SendIdle {
        self.port().send_idle(id)
    }

    fn shutdown(&self, id: Token, how: i32) {
        self.port().shutdown(id, how);
    }

    fn close(&self, id: Token) {
        self.port().close(id);
    }
}

pub enum RecvBuffer<'d> {
    Owned(Bytes<Retained>),
    Provided(ProvidedView<'d>),
}

impl RecvBuffer<'_> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(value) => value.as_slice(),
            Self::Provided(value) => value.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn advance(&mut self, count: usize) {
        match self {
            Self::Owned(value) => value.advance(count),
            Self::Provided(value) => value.advance(count),
        }
    }
}

impl AsRef<[u8]> for RecvBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

pub struct Io<'d, H: Host<'d>> {
    host: H,
    id: Token,
    brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy)]
enum Interest {
    Recv,
    Send,
}

struct Registration<'a, 'd, H: Host<'d>> {
    io: &'a mut Io<'d, H>,
    interest: Interest,
    armed: bool,
}

impl<'a, 'd, H: Host<'d>> Registration<'a, 'd, H> {
    fn new(io: &'a mut Io<'d, H>, interest: Interest) -> Self {
        Self {
            io,
            interest,
            armed: false,
        }
    }

    fn arm(&mut self, waker: Waker<'d>) {
        match self.interest {
            Interest::Recv => self.io.set_recv_waker(waker),
            Interest::Send => self.io.set_send_waker(waker),
        }
        self.armed = true;
    }

    fn clear(&mut self) {
        if !self.armed {
            return;
        }
        match self.interest {
            Interest::Recv => self.io.clear_recv_waker(),
            Interest::Send => self.io.clear_send_waker(),
        }
        self.armed = false;
    }

    fn complete(&mut self) {
        // Every transition that can make a registered operation ready takes
        // its waiter before waking the task. The port therefore no longer
        // owns this registration once a re-poll observes readiness. Keep the
        // explicit clear in Drop for the cancellation path, where no wake has
        // transferred ownership back to the task.
        self.armed = false;
    }

    fn poll_recv(
        &mut self,
        cx: Pin<&mut Context<'_, 'd>>,
        dst: &mut [u8],
        done: &mut bool,
    ) -> Poll<io::Result<usize>> {
        let result = if dst.is_empty() {
            Poll::Ready(Ok(0))
        } else {
            match self.io.try_recv_into(dst) {
                RecvInto::Bytes(count) => Poll::Ready(Ok(count)),
                RecvInto::Failed(error) => Poll::Ready(Err(error)),
                RecvInto::Pending => {
                    self.arm(unsafe { cx.waker_unchecked() });
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
        cx: Pin<&mut Context<'_, 'd>>,
        done: &mut bool,
    ) -> Poll<io::Result<()>> {
        let result = match self.io.try_send_idle() {
            SendIdle::Idle => Poll::Ready(Ok(())),
            SendIdle::Failed(error) => Poll::Ready(Err(error)),
            SendIdle::Pending => {
                self.arm(unsafe { cx.waker_unchecked() });
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

impl<'d, H: Host<'d>> Drop for Registration<'_, 'd, H> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<'d, H: Host<'d>> Io<'d, H> {
    pub(super) fn new(host: H, id: Token) -> Self {
        Self {
            host,
            id,
            brand: PhantomData,
        }
    }

    fn host(&self) -> &H {
        &self.host
    }

    fn try_recv_into(&mut self, dst: &mut [u8]) -> RecvInto {
        self.host().recv_into(self.id, dst)
    }

    fn try_recv_chunk(&mut self) -> RecvChunkResult<'d> {
        self.host().recv_chunk(self.id)
    }

    fn set_recv_waker(&mut self, waker: Waker<'d>) {
        self.host().recv_waker(self.id, waker);
    }

    fn set_send_waker(&mut self, waker: Waker<'d>) {
        self.host().send_waker(self.id, waker);
    }

    fn clear_recv_waker(&mut self) {
        self.host().clear_recv_waker(self.id);
    }

    fn clear_send_waker(&mut self) {
        self.host().clear_send_waker(self.id);
    }

    fn submit_send(&mut self, bytes: Shared) {
        self.host().send(self.id, bytes);
    }

    fn try_send_idle(&mut self) -> SendIdle {
        self.host().send_idle(self.id)
    }

    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        let how = match how {
            Shutdown::Read => libc::SHUT_RD,
            Shutdown::Write => libc::SHUT_WR,
            Shutdown::Both => libc::SHUT_RDWR,
        };
        self.host().shutdown(self.id, how);
        Ok(())
    }

    pub fn read_into<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Fiber<'d, Output = io::Result<usize>> + 'a {
        ReadInto {
            registration: Registration::new(self, Interest::Recv),
            buf,
            done: false,
        }
    }

    pub fn read_chunk(
        &mut self,
    ) -> impl Fiber<'d, Output = io::Result<Option<RecvBuffer<'d>>>> + '_ {
        ReadChunk {
            registration: Registration::new(self, Interest::Recv),
            done: false,
        }
    }

    pub fn write_all<'a>(
        &'a mut self,
        data: &'a [u8],
    ) -> impl Fiber<'d, Output = io::Result<()>> + 'a {
        WriteAll {
            registration: Registration::new(self, Interest::Send),
            data,
            submitted: false,
            done: false,
        }
    }

    pub fn write_all_shared<'a>(
        &'a mut self,
        bytes: Shared,
    ) -> impl Fiber<'d, Output = io::Result<()>> + 'a {
        WriteAllShared {
            registration: Registration::new(self, Interest::Send),
            bytes: Some(bytes),
            done: false,
        }
    }

    pub fn write_all_static<'a>(
        &'a mut self,
        bytes: &'static [u8],
    ) -> impl Fiber<'d, Output = io::Result<()>> + 'a {
        self.write_all_shared(Shared::from_static(bytes))
    }

    pub fn read<'a>(
        &'a mut self,
        buf: Vec<u8>,
    ) -> impl Fiber<'d, Output = (io::Result<usize>, Vec<u8>)> + 'a {
        Read {
            registration: Registration::new(self, Interest::Recv),
            buf,
            done: false,
        }
    }
}

struct ReadInto<'a, 'd, H: Host<'d>> {
    registration: Registration<'a, 'd, H>,
    buf: &'a mut [u8],
    done: bool,
}

impl<'d, H: Host<'d>> Fiber<'d> for ReadInto<'_, 'd, H> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::read_into polled after completion");
        this.registration.poll_recv(cx, this.buf, &mut this.done)
    }
}

struct ReadChunk<'a, 'd, H: Host<'d>> {
    registration: Registration<'a, 'd, H>,
    done: bool,
}

impl<'d, H: Host<'d>> Fiber<'d> for ReadChunk<'_, 'd, H> {
    type Output = io::Result<Option<RecvBuffer<'d>>>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::read_chunk polled after completion");
        let result = match this.registration.io.try_recv_chunk() {
            RecvChunkResult::Chunk(chunk) => Poll::Ready(Ok(Some(chunk))),
            RecvChunkResult::Closed => Poll::Ready(Ok(None)),
            RecvChunkResult::Failed(error) => Poll::Ready(Err(error)),
            RecvChunkResult::Pending => {
                this.registration.arm(unsafe { cx.waker_unchecked() });
                Poll::Pending
            }
        };
        if result.is_ready() {
            this.registration.complete();
            this.done = true;
        }
        result
    }
}

struct WriteAll<'a, 'd, H: Host<'d>> {
    registration: Registration<'a, 'd, H>,
    data: &'a [u8],
    submitted: bool,
    done: bool,
}

impl<'d, H: Host<'d>> Fiber<'d> for WriteAll<'_, 'd, H> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::write_all polled after completion");
        if !this.submitted {
            if this.data.is_empty() {
                this.done = true;
                return Poll::Ready(Ok(()));
            }
            this.registration
                .io
                .submit_send(Shared::copy_from_slice(this.data));
            this.submitted = true;
        }
        this.registration.poll_send(cx, &mut this.done)
    }
}

struct WriteAllShared<'a, 'd, H: Host<'d>> {
    registration: Registration<'a, 'd, H>,
    bytes: Option<Shared>,
    done: bool,
}

impl<'d, H: Host<'d>> Fiber<'d> for WriteAllShared<'_, 'd, H> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
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
            this.registration.io.submit_send(bytes);
        }
        this.registration.poll_send(cx, &mut this.done)
    }
}

struct Read<'a, 'd, H: Host<'d>> {
    registration: Registration<'a, 'd, H>,
    buf: Vec<u8>,
    done: bool,
}

impl<'d, H: Host<'d>> Fiber<'d> for Read<'_, 'd, H> {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.done, "fiber::Io::read polled after completion");
        let result = this
            .registration
            .poll_recv(cx, &mut this.buf, &mut this.done);
        match result {
            Poll::Ready(result) => Poll::Ready((result, mem::take(&mut this.buf))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'d, H: Host<'d>> Drop for Io<'d, H> {
    fn drop(&mut self) {
        self.host().close(self.id);
    }
}
