use crate::transport::link::Core;
use crate::{Driver, backend};

pub struct Vectored<'a> {
    pub iovs: &'a [backend::socket::IoVec],
    pub iov_storage: &'a mut [backend::socket::IoVec],
    pub msghdr_storage: &'a mut backend::socket::MsgHdr,
}

impl<'a> Vectored<'a> {
    pub(super) fn install_into_msghdr(&mut self) {
        let n = self.iovs.len();
        self.iov_storage[..n].copy_from_slice(self.iovs);
        self.msghdr_storage.set_iov(&self.iov_storage[..n]);
    }

    pub(super) fn msghdr_storage(&self) -> &backend::socket::MsgHdr {
        self.msghdr_storage
    }
}

pub enum RecvChunk<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> RecvChunk<'a> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b,
            Self::Owned(v) => v.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum Reclaim {
    OnSubmit,
    OnComplete,
}

pub trait Wire: 'static + Sized {
    type InitConfig: 'static + Clone + Default;

    const RECLAIM: Reclaim;

    /// Recv bytes reach the app untransformed, making in-kernel discard exact.
    const RAW_RECV: bool = false;

    fn new(cfg: &Self::InitConfig) -> Self;

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>>;

    fn on_recv_eof(&mut self) {}

    fn submit_send(
        &mut self,
        core: &mut Core,
        plain: &[u8],
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> usize;

    fn submit_send_vectored(
        &mut self,
        core: &mut Core,
        vectored: Vectored<'_>,
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> usize;

    fn submit_send_tracked(
        &mut self,
        core: &mut Core,
        plain: &[u8],
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> Option<usize> {
        let was_inflight = core.is_send_inflight();
        let consumed = self.submit_send(core, plain, ud, driver);
        (core.is_send_inflight() && !was_inflight).then_some(consumed)
    }

    fn submit_send_vectored_tracked(
        &mut self,
        core: &mut Core,
        vectored: Vectored<'_>,
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> Option<usize> {
        let was_inflight = core.is_send_inflight();
        let consumed = self.submit_send_vectored(core, vectored, ud, driver);
        (core.is_send_inflight() && !was_inflight).then_some(consumed)
    }

    fn after_send_cqe(
        &mut self,
        core: &mut Core,
        n: usize,
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> bool;

    fn flush_pending(&mut self, core: &mut Core, ud: backend::token::Token, driver: &mut Driver);

    fn on_graceful_close(
        &mut self,
        _core: &mut Core,
        _ud: backend::token::Token,
        _driver: &mut Driver,
    ) {
    }
}

pub struct Identity;

impl Wire for Identity {
    type InitConfig = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    const RAW_RECV: bool = true;

    fn new(_: &()) -> Self {
        Identity
    }

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>> {
        Some(RecvChunk::Borrowed(bytes))
    }

    fn submit_send(
        &mut self,
        core: &mut Core,
        plain: &[u8],
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> usize {
        if plain.is_empty() {
            return 0;
        }
        core.submit_single(ud, plain, driver);
        plain.len()
    }

    fn submit_send_vectored(
        &mut self,
        core: &mut Core,
        vectored: Vectored<'_>,
        ud: backend::token::Token,
        driver: &mut Driver,
    ) -> usize {
        if vectored.iovs.is_empty() {
            return 0;
        }
        let consumed: usize = vectored.iovs.iter().map(|v| v.len()).sum();
        core.submit_vectored(ud, vectored, driver);
        consumed
    }

    fn after_send_cqe(
        &mut self,
        _core: &mut Core,
        _n: usize,
        _ud: backend::token::Token,
        _driver: &mut Driver,
    ) -> bool {
        false
    }

    fn flush_pending(
        &mut self,
        _core: &mut Core,
        _ud: backend::token::Token,
        _driver: &mut Driver,
    ) {
    }
}
