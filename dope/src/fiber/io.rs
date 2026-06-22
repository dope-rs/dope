use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Poll, Waker};

use o3::buffer::Shared;

use super::state::{RecvInto, SendIdle};
use super::{Fiber, Holding};
use crate::backend;

pub trait Host {
    fn recv_into(self: Pin<&mut Self>, id: backend::token::Token, dst: &mut [u8]) -> RecvInto;
    fn recv_waker(self: Pin<&mut Self>, id: backend::token::Token, w: &Waker);
    fn send_waker(self: Pin<&mut Self>, id: backend::token::Token, w: &Waker);
    fn send(self: Pin<&mut Self>, id: backend::token::Token, bytes: Shared);
    fn send_idle(self: Pin<&mut Self>, id: backend::token::Token) -> SendIdle;
    fn shutdown(self: Pin<&mut Self>, id: backend::token::Token, how: i32);
    fn close(self: Pin<&mut Self>, id: backend::token::Token);
}

pub struct Io<'d, H: Host> {
    host: Holding<'d, H>,
    id: backend::token::Token,
}

impl<'d, H: Host> Io<'d, H> {
    pub(super) fn new(host: Holding<'d, H>, id: backend::token::Token) -> Self {
        Self { host, id }
    }

    fn try_recv_into(&mut self, dst: &mut [u8]) -> RecvInto {
        self.host.hold().recv_into(self.id, dst)
    }

    fn set_recv_waker(&mut self, w: &Waker) {
        self.host.hold().recv_waker(self.id, w);
    }

    fn set_send_waker(&mut self, w: &Waker) {
        self.host.hold().send_waker(self.id, w);
    }

    fn submit_send(&mut self, bytes: Shared) {
        self.host.hold().send(self.id, bytes);
    }

    fn try_send_idle(&mut self) -> SendIdle {
        self.host.hold().send_idle(self.id)
    }

    pub fn shutdown(&mut self, how: std::net::Shutdown) -> io::Result<()> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        self.host.hold().shutdown(self.id, how);
        Ok(())
    }

    pub fn read_into<'f, 'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Fiber<'f, impl Future<Output = io::Result<usize>> + 'a> {
        Fiber::new(async move {
            if buf.is_empty() {
                return Ok(0);
            }
            std::future::poll_fn(|cx| match self.try_recv_into(buf) {
                RecvInto::Bytes(n) => Poll::Ready(Ok(n)),
                RecvInto::Failed(e) => Poll::Ready(Err(e)),
                RecvInto::Pending => {
                    self.set_recv_waker(cx.waker());
                    Poll::Pending
                }
            })
            .await
        })
    }

    pub fn write_all<'f, 'a>(
        &'a mut self,
        data: &'a [u8],
    ) -> Fiber<'f, impl Future<Output = io::Result<()>> + 'a> {
        Fiber::new(async move {
            if data.is_empty() {
                return Ok(());
            }
            self.flush(Shared::copy_from_slice(data)).await
        })
    }

    pub fn write_all_owned<'f, 'a, Buf>(
        &'a mut self,
        buf: Buf,
    ) -> Fiber<'f, impl Future<Output = (io::Result<()>, Buf)> + 'a>
    where
        Buf: AsRef<[u8]> + Unpin + 'static,
    {
        Fiber::new(async move {
            if buf.as_ref().is_empty() {
                return (Ok(()), buf);
            }
            let bytes = Shared::copy_from_slice(buf.as_ref());
            (self.flush(bytes).await, buf)
        })
    }

    async fn flush(&mut self, bytes: Shared) -> io::Result<()> {
        self.submit_send(bytes);
        std::future::poll_fn(|cx| match self.try_send_idle() {
            SendIdle::Idle => Poll::Ready(Ok(())),
            SendIdle::Failed(e) => Poll::Ready(Err(e)),
            SendIdle::Pending => {
                self.set_send_waker(cx.waker());
                Poll::Pending
            }
        })
        .await
    }

    pub fn read<'f>(
        &mut self,
        mut buf: Vec<u8>,
    ) -> Fiber<'f, impl Future<Output = (io::Result<usize>, Vec<u8>)>> {
        Fiber::new(async move {
            let res = self.read_into(buf.as_mut_slice()).await;
            (res, buf)
        })
    }
}

impl<'d, H: Host> Drop for Io<'d, H> {
    fn drop(&mut self) {
        self.host.hold().close(self.id);
    }
}
