pub mod buffered;
pub mod identity;
pub mod send;

use std::io;

use crate::RetainBytes;

use self::send::{Plain, Prepared, SendStorage, Storage, Vectored};

pub enum Reclaim {
    OnSubmit,
    OnComplete,
}

#[derive(Clone, Copy)]
pub struct RuntimeLimits {
    max_connections: usize,
    max_retained_recv_chunks: usize,
    max_recv_len: usize,
}

impl RuntimeLimits {
    pub fn new(
        max_connections: usize,
        max_retained_recv_chunks: usize,
        max_recv_len: usize,
    ) -> Self {
        Self {
            max_connections,
            max_retained_recv_chunks,
            max_recv_len,
        }
    }

    pub fn max_connections(self) -> usize {
        self.max_connections
    }

    pub fn max_retained_recv_chunks(self) -> usize {
        self.max_retained_recv_chunks
    }

    pub fn max_recv_len(self) -> usize {
        self.max_recv_len
    }
}

pub trait Wire: 'static + Sized {
    type InitConfig: 'static + Clone + Default;
    type RuntimeContext: 'static;
    type Recv<'a>: RetainBytes + 'a;
    type SendStorage: SendStorage;

    const RECLAIM: Reclaim;

    const RAW_RECV: bool = false;

    fn runtime_context(limits: RuntimeLimits) -> io::Result<Self::RuntimeContext>;

    fn open(
        cfg: &Self::InitConfig,
        runtime: &Self::RuntimeContext,
    ) -> Option<(Self, Self::SendStorage)>;

    fn holds_plain(&self, _send: &Self::SendStorage) -> bool {
        false
    }

    fn process_recv<'a>(
        &mut self,
        runtime: &Self::RuntimeContext,
        bytes: &'a [u8],
    ) -> Option<Self::Recv<'a>>;

    fn recv_eof(&mut self) {}

    fn prepare_send<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a>;

    fn prepare_send_vectored<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a>;

    fn submit_failed(&mut self) {}

    fn after_send<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>, n: usize)
    -> Prepared<'a>;

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a>;

    fn graceful_close<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a> {
        send.empty(0)
    }
}
